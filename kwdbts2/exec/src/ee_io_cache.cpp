// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "ee_io_cache.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cmath>
#include "ee_global.h"
#include "lg_api.h"

namespace kwdbts {

#define LOCK_APPEND_BUFFER \
  if (need_append_buffer_lock) {  \
    info->append_buffer_lock_.lock();   \
  }

#define UNLOCK_APPEND_BUFFER \
  if (need_append_buffer_lock) {  \
    info->append_buffer_lock_.unlock(); \
  }

#define lock_append_buffer(info) std::lock_guard<std::mutex> lcm(info->append_buffer_lock_);

static k_char *ee_strdup(const char *from) {
  size_t length = strlen(from) + 1;
  char *ptr = static_cast<k_char *>(malloc(length));
  if (nullptr != ptr) {
    memset(ptr, 0, length);
    memcpy(ptr, from, length);
  }

  return ptr;
}

static void init_functions(IO_CACHE *info) {
  enum cache_type type = info->type_;
  switch (type) {
    case cache_type::CACHE_SEQ_READ_APPEND:
      info->read_func_ = _my_b_seq_read;
      info->write_func_ = nullptr;        /* use my_b_append */
      break;
    default:
      info->read_func_ = _my_b_read;
      info->write_func_ = _my_b_write;
  }

  // setup_io_cache(info);
}

static int read_append_buffer(IO_CACHE *info, k_char *buffer, k_uint64 count,
                                                k_uint64 pos_in_file, k_uint64 save_count) {
  assert(info->append_read_pos_ <= info->write_pos_);
  assert(pos_in_file == info->end_of_file_);
  k_char *buff = buffer;
  /* read size */
  // Remaining readable data in write cache
  k_uint64 len_in_buff = info->write_pos_ - info->append_read_pos_;
  k_uint32 copy_len = std::min(count, len_in_buff);
  // read data
  memcpy(buff, info->append_read_pos_, copy_len);
  info->append_read_pos_ += copy_len;
  count -= copy_len;
  if (count > 0) {
    info->error_ = save_count - count;
  }

  /* Copy the remaining data from writebuffer to readbuffer_ */
  // unread data in write cache
  k_uint32 transfer_len = len_in_buff - copy_len;
  // Copy the remaining data to buffer
  memcpy(info->buffer_, info->append_read_pos_, transfer_len);
  info->read_pos_ = info->buffer_;
  info->read_end_ = info->buffer_ + transfer_len;
  info->append_read_pos_ = info->write_pos_;
  info->pos_in_file_ = pos_in_file + copy_len;
  info->end_of_file_ += len_in_buff;

  return count ? 1 : 0;
}

IO_CACHE::~IO_CACHE() {
  SafeFreePointer(buffer_);
  SafeFreePointer(dir_);
  SafeFreePointer(prefix_);
  SafeFreePointer(file_name_);

  if (file_ >= 0) {
    close(file_);
    file_ = -1;
  }
}

bool open_cached_file(IO_CACHE *cache, const char *dir,
          const char *prefix, const char *filename, size_t cache_size, k_uint64 offset, enum cache_type type) {
  cache->dir_ = ee_strdup(dir);
  cache->prefix_ = ee_strdup(prefix);
  cache->file_name_ = ee_strdup(filename);
  cache->buffer_ = nullptr; /* Mark that not open */

  char dirname_buf[FN_REFLEN];
  memset(dirname_buf, 0, FN_REFLEN);
  convert_dirname(dirname_buf, dir, prefix, filename);
  int file = kwdbts_file_open(dirname_buf, type);
  if (file > 0 && 0 == init_io_cache(cache, file, cache_size, offset, type)) {
    return false;
  }

  SafeFreePointer(cache->dir_);
  SafeFreePointer(cache->prefix_);
  SafeFreePointer(cache->file_name_);

  return true;
}

int init_io_cache(IO_CACHE *info, k_int32 file, k_uint64 cachesize, k_uint64 seek_offset, enum cache_type type) {
  k_uint64 min_cache;
  k_uint64 pos;
  k_uint64 end_of_file = ~(k_uint64)0;
  // init IO_CACHE
  info->file_ = file;
  info->type_ = cache_type::CACHE_UNKNOW;
  info->pos_in_file_ = seek_offset;
  info->alloced_buffer_ = false;
  info->buffer_ = nullptr;
  info->disk_writes_ = 0;
  info->seek_not_done_ = false;
  if (file >= 0) {
    k_uint64 pos = kwdbts_file_seek(file, 0, SEEK_CUR);     // seek the current pos
    if (MY_FILEPOS_ERROR == pos && (errno == ESPIPE)) {
      info->seek_not_done_ = false;
      assert(seek_offset == 0);
    } else {
      info->seek_not_done_ = (seek_offset != pos);
    }
  }

  min_cache = IO_SIZE * 2;
  if (cache_type::CACHE_READ == type || cache_type::CACHE_SEQ_READ_APPEND == type) {
    end_of_file = kwdbts_file_seek(file, 0L, SEEK_END);
    info->seek_not_done_ = !(end_of_file == seek_offset);  // set seek_not_done
    if (end_of_file < seek_offset) {
      end_of_file = seek_offset;
    }
  }
  cachesize = ((cachesize + min_cache - 1) & ~(min_cache - 1));   // min_cache right alignment
  // min_cache
  if (cachesize < min_cache) {
    cachesize = min_cache;
  }
  k_uint64 buffer_block = cachesize;
  // SEQ_READ_APPEND allocate twice the space
  if (cache_type::CACHE_SEQ_READ_APPEND == type) {
    buffer_block *= 2;
  }
  info->buffer_ = static_cast<k_char *>(malloc(buffer_block));
  if (nullptr == info->buffer_) {
    LOG_ERROR("malloc IO_CACHE buffer failed, request size : %li64u", buffer_block);
    return -1;
  }
  info->alloced_buffer_ = true;
  info->write_buffer_ = info->buffer_;
  if (cache_type::CACHE_SEQ_READ_APPEND == type) {
    info->write_buffer_ = info->buffer_ + cachesize;
  }

  info->read_length_ = info->buffer_length_ = cachesize;
  info->read_pos_ = info->write_pos_ = info->buffer_;
  if (cache_type::CACHE_SEQ_READ_APPEND == type) {
    info->append_read_pos_ = info->write_pos_ = info->write_buffer_;
    info->write_end_ = info->write_buffer_ + info->buffer_length_;
  }

  if (cache_type::CACHE_WRITE == type) {
    info->write_end_ = info->buffer_ + info->buffer_length_;
  } else {
    info->read_end_ = info->buffer_;    /* Nothing in cache */
  }

  info->end_of_file_ = end_of_file;      /* End_of_file may be changed by user later */
  info->type_ = type;
  init_functions(info);
  return 0;
}

bool reinit_io_cache(IO_CACHE *info, enum cache_type type, k_uint64 seek_offset) {
  info->read_pos_ = info->write_pos_ = info->buffer_;
  if (cache_type::CACHE_WRITE == type) {
    info->write_end_ = info->buffer_ + info->buffer_length_;
    info->end_of_file_ = MY_FILEPOS_ERROR;
  } else {
    info->read_end_ = info->read_pos_;
    info->end_of_file_ = kwdbts_file_seek(info->file_, 0, SEEK_END);
  }
  info->seek_not_done_ = true;
  info->pos_in_file_ = seek_offset;
  info->type_ = type;
  info->error_ = 0;
  init_functions(info);

  return false;
}

int end_io_cache(IO_CACHE *info) {
  k_int32 error = 0;
  if (info->file_ != -1) {
    error = kwdbts_flush_io_cache(info, 1);
    close(info->file_);
  }

  info->pos_in_file_ = 0;
  info->end_of_file_ = 0;
  info->buffer_length_ = 0;
  info->read_length_ = 0;
  info->read_pos_ = nullptr;
  info->read_end_ = nullptr;
  SafeFreePointer(info->buffer_);
  info->write_buffer_ = nullptr;
  info->append_read_pos_ = nullptr;
  info->write_pos_ = nullptr;
  info->write_end_ = nullptr;
  info->read_func_ = nullptr;
  info->write_func_ = nullptr;
  info->type_ = cache_type::CACHE_UNKNOW;
  info->disk_writes_ = 0;

  SafeFreePointer(info->file_name_);
  SafeFreePointer(info->dir_);
  SafeFreePointer(info->prefix_);
  info->file_ = -1;
  info->error_ = 0;
  info->alloced_buffer_ = false;
  info->seek_not_done_ = false;

  return error;
}

k_uint64 kwdbts_file_seek(k_int32 fd, k_int64 offset, k_int32 whence) {
  const k_int64 newpos = lseek(fd, offset, whence);
  if (-1 == newpos) {
    return MY_FILEPOS_ERROR;
  }

  assert(newpos >= 0);
  return newpos;
}

k_uint64 kwdbts_file_tell(k_int32 fd) {
  assert(fd >= 0);

  k_uint64 pos = kwdbts_file_seek(fd, 0L, SEEK_CUR);

  return pos;
}

k_int32 kwdbts_file_sync(k_int32 fd) {
  int res;

  do {
    res = fdatasync(fd);
    // res = fsync(fd);
  } while (res == -1 && errno == EINTR);

  return res;
}

k_int32 kwdbts_file_open(const char *filename, enum cache_type type) {
  int Flags = 0;
  switch (type) {
    case cache_type::CACHE_READ: {
      Flags = O_RDWR;
      break;
    }
    default: {
      Flags = O_RDWR | O_CREAT | O_TRUNC;
      break;
    }
  }

  return open(filename, Flags, 0600);
}

bool convert_dirname(char *buff, const char *dir, const char *prefix, const char *filename) {
  size_t sz = strlen(dir);
  memcpy(buff, dir, sz);
  buff += sz;
  if (dir[sz - 1] != '/') {
    memset(buff, '/', 1);
    ++buff;
  }
  sz = strlen(prefix);
  memcpy(buff, prefix, sz);
  buff += sz;
  sz = strlen(filename);
  memcpy(buff, filename, sz);

  return true;
}

int my_b_read(IO_CACHE *info, k_char *buffer, k_uint64 count) {
  // read
  if (info->read_pos_ + count <= info->read_end_) {
    memcpy(buffer, info->read_pos_, count);
    info->read_pos_ += count;
    return 0;
  }
  return (info->read_func_)(info, buffer, count);
}

int _my_b_read(IO_CACHE *info, k_char *buffer, k_uint64 count) {
  k_char *buff = buffer;
  k_uint32 length = 0;

  /* Copy the remaining buffer data to user space */
  k_uint32 left_length = info->read_end_ - info->read_pos_;
  if (left_length > 0) {
    assert(count >= left_length);
    memcpy(buff, info->read_pos_, left_length);
    buff += left_length;
    count -= left_length;
  }

  k_uint64 pos_in_file = info->pos_in_file_ + (info->read_end_ - info->buffer_);

  // seek the current pos
  if (info->seek_not_done_) {
    if (MY_FILEPOS_ERROR != kwdbts_file_seek(info->file_, pos_in_file, SEEK_SET)) {
      info->seek_not_done_ = false;
    } else {
      assert(errno != ESPIPE);
      info->error_ = -1;
      return 1;
    }
  }

  // Align files to the left with IO_SIZE
  k_uint32 diff_length = pos_in_file & (IO_SIZE - 1);

  if (count >= IO_SIZE * 2 - diff_length) {
    if (info->end_of_file_ <= pos_in_file) {  // read finish
      // The error code is the remaining data in the buffer
      info->error_ = static_cast<k_int32>(left_length);
      return 1;
    }
    // ensure overall IO_SIZE left alignment
    length = (count & ~(IO_SIZE - 1)) - diff_length;
    k_uint64 read_length = kwdbts_file_read(info->file_, buff, length);
    if (read_length != length) {
      info->error_ = (read_length == MY_FILE_ERROR) ? -1 : read_length + left_length;
      return 1;
    }
    // update
    count -= length;
    buff += length;
    pos_in_file += length;
    left_length += length;
    diff_length = 0;            // diff_length is 0
  }

  k_uint32 max_length = info->read_length_ - diff_length;
  /* We will not read past end of file. */
  if (max_length > (info->end_of_file_ - pos_in_file)) {
    max_length = info->end_of_file_ - pos_in_file;
  }

  // read finish
  if (0 == max_length) {
    if (count > 0) {  // it has't finished reading yet
      info->error_ = left_length;
      return 1;
    }
    length = 0; /* Didn't read any chars */
  } else {
    length = kwdbts_file_read(info->file_, info->buffer_, max_length);
    if (length < count || length == MY_READ_ERROR) {
      if (length != MY_READ_ERROR) {  // read end
        memcpy(buff, info->buffer_, length);
      }

      info->pos_in_file_ = pos_in_file;
      info->error_ = length == MY_READ_ERROR ? -1 : length + left_length;
      info->read_pos_ = info->read_end_ = info->buffer_;
      return 1;
    }
  }

  /* update */
  info->read_pos_ = info->buffer_ + count;
  info->read_end_ = info->buffer_ + length;
  info->pos_in_file_ = pos_in_file;
  memcpy(buff, info->buffer_, count);

  return 0;
}

int _my_b_seq_read(IO_CACHE *info, k_char *buffer, k_uint64 count) {
  k_uint32 diff_length = 0;
  k_uint64 max_length = 0;
  k_uint32 length = 0;
  k_uint64 save_count = count;
  k_char *buff = buffer;
  /* copy data to buffer */
  k_uint32 left_length = info->read_end_ - info->read_pos_;
  if (left_length > 0) {
    assert(count > left_length);    /* user is not using my_b_read() */
    memcpy(buff, info->read_pos_, left_length);
    buff += left_length;
    count -= left_length;
  }

  lock_append_buffer(info);

  k_uint64 pos_in_file = info->pos_in_file_ + (info->read_end_ - info->buffer_);  // read current pos
  if (pos_in_file >= info->end_of_file_) {    // read write_buffer
    // goto read_append_buffer;
    return read_append_buffer(info, buff, count, pos_in_file, save_count);
  }

  if (MY_FILEPOS_ERROR ==
      kwdbts_file_seek(info->file_, pos_in_file, SEEK_SET)) {
    info->error_ = -1;
    return 1;
  }
  info->seek_not_done_ = false;
  diff_length = pos_in_file & (IO_SIZE - 1);

  // read
  if (count >= IO_SIZE * 2 - diff_length) {
    /* Fill first intern buffer */
    length = count & ~(IO_SIZE - 1) - diff_length;    // read size
    k_uint64 read_length = kwdbts_file_read(info->file_, buff, length);
    if (MY_FILEPOS_ERROR == read_length) {
      info->error_ = -1;
      return 1;
    }
    // update
    count -= read_length;
    buff += read_length;
    pos_in_file += read_length;

    if (read_length != length) {
      // goto read_append_buffer;
      return read_append_buffer(info, buff, count, pos_in_file, save_count);
    }

    left_length += length;
    diff_length = 0;
  }

  max_length = info->read_length_ - diff_length;          // read the size of buffer
  if (max_length > (info->end_of_file_ - pos_in_file))    // over flow
    max_length = info->end_of_file_ - pos_in_file;
  if (0 == max_length) {
    if (count > 0) {
      // goto read_append_buffer;    // read write_buffer
      return read_append_buffer(info, buff, count, pos_in_file, save_count);
    }
    length = 0; /* Didn't read any more chars */
  } else {
    length = kwdbts_file_read(info->file_, info->buffer_, max_length);
    if (MY_READ_ERROR == length) {
      info->error_ = -1;
      return 1;
    }

    if (length < count) {
      memcpy(buff, info->buffer_, length);
      count -= length;
      buff += length;
      pos_in_file += length;
      // goto read_append_buffer;
      return read_append_buffer(info, buff, count, pos_in_file, save_count);
    }
  }

  info->read_pos_ = info->buffer_ + count;
  info->read_end_ = info->buffer_ + length;
  info->pos_in_file_ = pos_in_file;
  memcpy(buff, info->buffer_, count);

  return 0;
}

int my_b_write(IO_CACHE *info, const k_char *buffer, k_uint64 count) {
  // enough
  if (info->write_pos_ + count <= info->write_end_) {
    memcpy(info->write_pos_, buffer, count);
    info->write_pos_ += count;
    return 0;
  }
  return (info->write_func_)(info, buffer, count);
}

int _my_b_write(IO_CACHE *info, const k_char *buffer, k_uint64 count) {
  const k_char *buff = buffer;
  k_uint64 pos_in_file = info->pos_in_file_;
  if (pos_in_file + info->buffer_length_ > info->end_of_file_) {
    return -1;
  }
  // full
  k_uint32 rest_length = info->write_end_ - info->write_pos_;
  memcpy(info->write_pos_, buff, rest_length);
  buff += rest_length;
  count -= rest_length;
  info->write_pos_ += rest_length;
  // flush to disk
  if (0 != kwdbts_flush_io_cache(info, 1)) {
    return 1;
  }

  if (count >= IO_SIZE) {
    k_uint32 length = count & ~(IO_SIZE - 1);
    if (info->seek_not_done_) {
      if (MY_FILE_ERROR == kwdbts_file_seek(info->file_, info->pos_in_file_, SEEK_SET)) {
        info->error_ = -1;
        return 1;
      }

      info->seek_not_done_ = false;
    }
    k_uint64 write_bytes = kwdbts_file_write(info->file_, buff, length);
    if (write_bytes != length) {
      return -1;
    }

    count -= length;
    buff += length;
    info->pos_in_file_ += length;
  }

  // write the remaining content smaller than IO_SIZE to the buffer
  memcpy(info->write_pos_, buff, count);
  info->write_pos_ += count;

  return 0;
}

int my_b_append(IO_CACHE *info, const k_char *buffer, k_uint64 count) {
  const k_char *buff = buffer;
  lock_append_buffer(info);
  k_uint32 rest_length = info->write_end_ - info->write_pos_;
  if (count <= rest_length) {   // copy
    memcpy(info->write_pos_, buff, count);
    info->write_pos_ += count;
    return 0;
  }

  // full
  memcpy(info->write_pos_, buff, rest_length);
  buff += rest_length;
  count -= rest_length;
  info->write_pos_ += rest_length;
  // flush
  if (0 != kwdbts_flush_io_cache(info, 0)) {
    return 1;
  }

  if (count >= IO_SIZE) {
    k_uint32 length = count & ~(IO_SIZE - 1);
    k_uint64 write_bytes = kwdbts_file_write(info->file_, buff, length);
    if (write_bytes != length) {
      info->error_ = -1;
      return -1;
    }
    // update
    count -= length;
    buff += length;
    info->end_of_file_ += length;
  }

  memcpy(info->write_pos_, buff, count);
  info->write_pos_ += count;

  return 0;
}

k_uint64 kwdbts_file_read(k_int32 fd, k_char *buffer, k_uint64 count) {
  k_int64 savedbytes = 0;
  k_char *buff = buffer;
  for (;;) {
    errno = 0;
    k_int64 readbytes = read(fd, buff, count);
    if (readbytes != static_cast<k_int64>(count)) {
      if ((readbytes == 0 || readbytes == -1) && errno == EINTR) {
        continue; /* Interrupted */
      }

      if (-1 == readbytes)
        return MY_FILE_ERROR; /* Return with error */
      /* readbytes == 0 when EOF. No need to continue in case of EOF */
      if (readbytes != 0) {
        buff += readbytes;
        count -= readbytes;
        savedbytes += readbytes;
        continue;
      }
    }

    readbytes += savedbytes;

    return readbytes;
  }
}

k_uint64 kwdbts_file_write(k_int32 fd, const k_char *buffer, k_uint64 count) {
  int64_t sum_written = 0;
  const k_char *buff = buffer;
  uint errors = 0;
  const k_uint64 initial_count = count;

  if (0 == count) {
    return 0;
  }

  for (;;) {
    errno = 0;
    k_int64 writtenbytes = write(fd, buff, count);
    if (writtenbytes == static_cast<int64_t>(count)) {
      sum_written += writtenbytes;
      break;
    }
    if (writtenbytes != -1) { /* Safeguard */
      sum_written += writtenbytes;
      buff += writtenbytes;
      count -= writtenbytes;
    }

    if (writtenbytes != 0 && writtenbytes != -1)
      continue; /* Retry if something written */

    if (EINTR == errno) {   /* Interrupted, retry */
      continue;
    }
    break;
  }

  return 0 == sum_written ? MY_FILE_ERROR : sum_written;
}

k_int32 kwdbts_flush_io_cache(IO_CACHE *info, int need_append_buffer_lock) {
  bool append_cache = (cache_type::CACHE_SEQ_READ_APPEND == info->type_);
  if (0 == append_cache) {
    need_append_buffer_lock = 0;
  }

  if (cache_type::CACHE_WRITE == info->type_ || append_cache) {
    LOCK_APPEND_BUFFER;

    k_uint32 length = info->write_pos_ - info->write_buffer_;   // data in buffer
    if (length > 0) {
      k_uint64 pos_in_file = info->pos_in_file_;    // current pos
      if (0 == append_cache && true == info->seek_not_done_) {
        if (MY_FILEPOS_ERROR == kwdbts_file_seek(info->file_, pos_in_file, SEEK_SET)) {
           UNLOCK_APPEND_BUFFER;
           info->error_ = -1;
          return -1;
        }
        info->seek_not_done_ = false;
      }
      if (0 == append_cache) {
        info->pos_in_file_ += length;   // update write po
      }

      info->write_end_ = (info->write_buffer_ + info->buffer_length_ -
                          ((pos_in_file + length) & (IO_SIZE - 1)));

      if (length == kwdbts_file_write(info->file_, info->write_buffer_, length))
        info->error_ = 0;
      else
        info->error_ = -1;
      if (0 == append_cache) {
        info->end_of_file_ =
            std::max(info->end_of_file_, (pos_in_file + length));
      } else {
        info->end_of_file_ += (info->write_pos_ - info->append_read_pos_);
        assert(info->end_of_file_ == kwdbts_file_tell(info->file_));
      }

      info->append_read_pos_ = info->write_pos_ = info->write_buffer_;
      if (kwdbts_file_sync(info->file_)) {
        UNLOCK_APPEND_BUFFER;
        return -1;
      }
      ++info->disk_writes_;
      UNLOCK_APPEND_BUFFER;
      return info->error_;
    }
  }
  UNLOCK_APPEND_BUFFER;
  return 0;
}

}  // namespace kwdbts
