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

#pragma once

#include <stdio.h>
#include <unistd.h>
#include <functional>
#include <mutex>

#include "kwdb_type.h"

namespace kwdbts {

#define MY_FILE_ERROR     ((k_uint64)-1)
#define MY_FILEPOS_ERROR  (~(k_uint64)0)
#define MY_READ_ERROR     ((k_uint32)-1)
#define FN_REFLEN         512     /* Max length of full path-name */
constexpr const k_uint32 IO_SIZE{4096};

// IO_CACHE type
enum cache_type {
  CACHE_UNKNOW = -1,

  CACHE_READ = 1,         // read buffer
  CACHE_WRITE,            // write buffer
  CACHE_SEQ_READ_APPEND,  // Sequential reads and writes

  CACHE_MAX = 0xFFFFFFFE
};

struct IO_CACHE;

using ReadFunc = std::function<int(IO_CACHE *, k_char *, size_t)>;
using WriteFunc = std::function<int(IO_CACHE *, const k_char *, size_t)>;

/* Used when caching files */
struct IO_CACHE {
  ~IO_CACHE();
  k_uint64 pos_in_file_{0};           // The current read-write file location
  k_uint64 end_of_file_{0};           // The location at the end of the file
  k_uint64 buffer_length_{0};         // buffer length
  k_uint64 read_length_{0};           // read length
  k_char *read_pos_{nullptr};         // The current read buffer location
  k_char *read_end_{nullptr};         // Read where the buffer ends
  k_char *buffer_{nullptr};           // read buffer
  k_char *write_buffer_{nullptr};     // write buffer
  k_char *append_read_pos_{nullptr};  // Used in SEQ_READ_APPEND as a read
                                      // location in the write cache
  k_char *write_pos_{nullptr};        // Write to the buffer location
  k_char *write_end_{nullptr};        // Write where the buffer ends
  std::mutex append_buffer_lock_;     // used by SEQ_READ_APPEND
  ReadFunc read_func_{nullptr};       // the pointer which reads data
  WriteFunc write_func_{nullptr};     // the pointer which write data
  cache_type type_{cache_type::CACHE_UNKNOW};  // IO_CACHE type
  k_uint32 disk_writes_{
      0};  // Records the number of times it was written to disk

  k_char *file_name_{nullptr};  // file name
  k_char *dir_{nullptr};        // file directory
  k_char *prefix_{nullptr};     // name prefix
  k_int32 file_{-1};            // file desc
  k_int32 error_{0};            // err code
  bool alloced_buffer_{false};  // alloc mem,bool
  bool seek_not_done_{false};   // Whether it needs to perform a seek
};

/**
 * IO_CACHE init
 * 
 * @return
 *    true : initialized
 *    false : not initialized
*/
inline bool my_b_inited(const IO_CACHE *info) {
  return info->buffer_ != nullptr;
}

/**
 * @brief 
 *                      Open file and initialize an IO_CACHE object
 * @param cache         IO_CACHE
 * @param dir           directory
 * @param prefix        name prefix
 * @param filename      file name 
 * @param cache_size    IO_CACHE size
 * @param offset        file offset 
 * @param type          IO_CACHE type
 * 
 * @return true         Failed
 * @return false        Success
 */
extern bool open_cached_file(IO_CACHE *cache, const char *dir,
                const char *prefix, const char *filename, size_t cache_size, k_uint64 offset, enum cache_type type);

/**
 * @brief 
 *                        Initialize an IO_CACHE object
 * @param info            IO_CACHE object
 * @param file            file desc
 * @param cachesize       cache size 
 * @param seek_offset     the initial location of the file
 * @param type            find type
 * 
 * @return int
 *        0   Success
 *        #   Error
 */
extern int init_io_cache(IO_CACHE *info, k_int32 file, k_uint64 cachesize, k_uint64 seek_offset, enum cache_type type);

/**
 * @brief 
 *                        Reinit an IO_CACHE object - CACHE_READ <-> CACHE_WRITE
 * @param info            IO_CACHE object
 * @param type            find type
 * @param seek_offset     the initial location of the file
 * 
 * @return true           Error
 * @return false          Success
 */
extern bool reinit_io_cache(IO_CACHE *info, enum cache_type type, k_uint64 seek_offset);

/**
 * @brief 
 *                Free an IO_CACHE object
 * @param info    IO_CACHE object
 * @return int
 *    0 Success
 *    # Error
 */
extern int end_io_cache(IO_CACHE *info);

/**
 * Sets the file position
 * 
 * @param   fd      file desc
 * @param   offset  file offset
 * @param   whence  offset type：
 *                      SEEK_SET:header + offset
 *                      SEEK_CUR:curpos + offset
 *                      SEEK_END:filelen + offset
 * @return
 *    MY_FILEPOS_ERROR  error;
 *    other             file pointer
*/
extern k_uint64 kwdbts_file_seek(k_int32 fd, k_int64 offset, k_int32 whence);

/**
 * locate file pos
 * 
 * @param   fd      file desc
 * 
 * @return
 *    MY_FILEPOS_ERROR  error;
 *    other             the pos of file pointer
*/
extern k_uint64 kwdbts_file_tell(k_int32 fd);

/**
 * Sync data in file to disk
 * 
 * @param fd  file desc
 * 
 * @return
 *    0   ok
 *    -1  error
*/
extern k_int32 kwdbts_file_sync(k_int32 fd);

/**
 * open file
*/
extern k_int32 kwdbts_file_open(const char *filename, enum cache_type type);

extern bool convert_dirname(char *buff, const char *dir, const char *prefix, const char *filename);

/**
 * Sequential Read  CACHE_READ
 *
 * @param info    IO_CACHE
 * @param buffer  buffer
 * @param count   len which want to read
 *
 * @note
 *    This function is called from the my_b_read() macro only if there are not
 *    enough characters in the buffer to satisfy the request
 *
 * @return
 *    0   success
 *    1   err:
 *        if info->error == -1，read error.
 *        info->error include the length of Buffer
 */
extern int my_b_read(IO_CACHE *info, k_char *buffer, k_uint64 count);
extern int _my_b_read(IO_CACHE *info, k_char *buffer, k_uint64 count);

/**
 * Append Read  CACHE_SEQ_READ_APPEND
 * 
 * @param info    IO_CACHE
 * @param buffer  buffer
 * @param count   read size
 * 
 * @return
 *  0  success
 *  1  failed to read
*/
extern int _my_b_seq_read(IO_CACHE *info, k_char *buffer, k_uint64 count);

/**
 * Sequential Write  CACHE_WRITE
 * 
 * @param info    IO_CACHE
 * @param buffer  write buffer
 * @param count   buffer size
 * 
 * @return
 *    1   flush failed
 *    0   On success
 *    -1  error
*/
extern int my_b_write(IO_CACHE *info, const k_char *buffer, k_uint64 count);
extern int _my_b_write(IO_CACHE *info, const k_char *buffer, k_uint64 count);

/**
 * Append Write CACHE_SEQ_READ_APPEND
 * 
 * @param info    IO_CACHE
 * @param buffer  Cache of files to be written
 * @param count   buffer size
 * 
 * @return
 *    0   success  
 *    -1  write error
 *    1   flush error
*/
extern int my_b_append(IO_CACHE *info, const k_char *buffer, k_uint64 count);

/**
 * read buffer from the temporary file
 * 
 * @param fd        file desc
 * @param buffer    recv buffer
 * @param count     buffer size
 * 
 * @return
 *    MY_FILE_ERROR   read error
 *    the number of bytes read, on success.
*/
extern k_uint64 kwdbts_file_read(k_int32 fd, k_char *buffer, k_uint64 count);

/**
 * write buffer to the temporary file
 * 
 * @param fd        file desc
 * @param buffer    file data to be written
 * @param count     buffer size
 * 
 * @return
 *   0  if count == 0
 *   the number of bytes written, on success.
 *   the actual number of bytes written, on partial success (if less than Count bytes could be written).
 *   MY_FILE_ERROR, On failure.  
*/
extern k_uint64 kwdbts_file_write(k_int32 fd, const k_char *buffer, k_uint64 count);

/**
 * flush write cache
 *
 * @param info                          IO_CACHE
 * @param need_append_buffer_lock       Whether or not to add a lock
 *
 * @return
 *    0   success;
 *    -1  error;
 */
extern k_int32 kwdbts_flush_io_cache(IO_CACHE *info, int need_append_buffer_lock);

}  // namespace kwdbts
