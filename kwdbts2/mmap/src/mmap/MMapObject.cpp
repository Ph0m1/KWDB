// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.


#include "mmap/MMapObject.h"
#include "BigObjectUtils.h"
#include "BigObjectApplication.h"

MMapObject::MMapObject() {
  mem_length_ = 0;
  mem_vtree_ = mem_ns_ = mem_data_ = mem_data_ext_ = nullptr;
  meta_data_ = nullptr;
  is_ns_needed_ = false;
  obj_type_ = 0;
}

MMapObject::~MMapObject()
{
}


int  MMapObject::openNameService(ErrorInfo &err_info, int flags) {

  return 0;
}

int MMapObject::swap(MMapObject &rhs) {
    MMapFile::swap(rhs);
    std::swap(mem_length_, rhs.mem_length_);
    std::swap(meta_data_, rhs.meta_data_);
    std::swap(mem_vtree_, rhs.mem_vtree_);
    std::swap(mem_ns_, rhs.mem_ns_);
    std::swap(mem_data_, rhs.mem_data_);
    std::swap(mem_data_ext_, rhs.mem_data_ext_);
    std::swap(obj_name_, rhs.obj_name_);
    std::swap(hrchy_info_, rhs.hrchy_info_);
    std::swap(hrchy_index_, rhs.hrchy_index_);
    std::swap(measure_info_, rhs.measure_info_);
    std::swap(measure_index_, rhs.measure_index_);
    std::swap(is_ns_needed_, rhs.is_ns_needed_);

    return 0;
}


int MMapObject::open(const string &file_path, const std::string &db_path, const string &tbl_sub_path, int cc,
  int flags) {
  int error_code = MMapFile::open(file_path, db_path, tbl_sub_path, flags);
  if (error_code < 0) {
    return error_code;
  }
  if (file_length_ >= (off_t) sizeof(MMapMetaData)) {
    hrchy_info_.clear();
    // initialize all pointers first
    initSection();
    if (meta_data_->magic != cc) {
      return BOENOOBJ;
    }

//    const char *p;
//	p = (const char*) addr(meta_data_->attribute_offset);
//	hrchy_info_ = readAttributeInfo(
//	    (char *) addr(meta_data_->attribute_offset), meta_data_->level);
//	hrchy_info_ = readAttributeInfo(meta_data_->attribute_offset);
    error_code = readColumnInfo(meta_data_->attribute_offset,
      meta_data_->level, hrchy_info_);
    if (error_code < 0) {
      return error_code;
    }
    hrchy_index_.clear();
//	measure_index_.clear();
    hrchy_index_.setInfo(hrchy_info_);
//	measure_info_ = readAttributeInfo(
//	    (char *) addr(meta_data_->measure_info),
//	    meta_data_->num_measure_info);
//	measure_info_ = readAttributeInfo(meta_data_->measure_info);
    error_code = readColumnInfo(meta_data_->measure_info,
      meta_data_->num_measure_info, measure_info_);
    if (error_code < 0) {
      return error_code;
    }
    measure_index_.setInfo(measure_info_);

//	p = (const char*) addr(meta_data_->dimension_info);
//	for (int i = 0; i < meta_data_->num_dimension_info; i++) {
//	    DimensionInfo dim;
//	    dim.name = string(p);
//	    p += dim.name.size() + 1;
//	    dim.url = string(p);
//	    p += dim.url.size() + 1;
//	    dimension_info_.push_back(dim);
//	}

        ErrorInfo err_info;
        if (openNameService(err_info, flags_) < 0) {
            return err_info.errcode;
        }

        mem_length_ = meta_data_->length;
    }
    if (mem_length_ == 0)
        mem_length_ = file_length_;

    obj_name_ = getURLObjectName(file_path_);

    return 0;
}

int MMapObject::close() {
  munmap();
  mem_length_ = 0;
  mem_vtree_ = mem_ns_ = mem_data_ = mem_data_ext_ = nullptr;
  meta_data_ = nullptr;
  return 0;
}

int MMapObject::initMetaData()
{
    int err_code = 0;
    if (file_length_ < (off_t) sizeof(MMapMetaData))
        err_code = memExtend(sizeof(MMapMetaData));
    if (err_code < 0)
        return err_code;
    memset(meta_data_, 0, sizeof(MMapMetaData));
    meta_data_->meta_data_length = sizeof(MMapMetaData);
    return 0;
}

void MMapObject::initSection() {
  meta_data_ = (MMapMetaData*) mem_;

  if (meta_data_->struct_type & ST_VTREE) {
    mem_vtree_ = (void*) addr(meta_data_->vtree_offset);
  }

  if (meta_data_->struct_type & ST_NS) {
    mem_ns_ = (void*) addr(meta_data_->ns_offset);
  }
  if (meta_data_->struct_type & ST_DATA) {
    mem_data_ = (void*) addr(meta_data_->data_offset);
  }
//  if (meta_data_->struct_type & ST_SUPP) {
//    mem_supp_ = (void*) addr(meta_data_->supp_offset);
//  }
}

int MMapObject::memExtend(off_t offset, size_t ps)
{
    off_t new_mem_len = mem_length_ + offset;
    if (file_length_ < new_mem_len) {
        int err_code = mremap(getPageOffset(new_mem_len, ps));
        if (err_code < 0)
            return err_code;
        initSection();
    } else {
        ///TODO: trim table
    }
    mem_length_ = new_mem_len;
    return 0;
}

off_t MMapObject::urlCopy(const string &source_url, off_t offset,
    size_t ps) {
    off_t len = source_url.size() + 1 + offset;
    off_t prev_len;
    off_t limit;
    int err_code = 0;

    limit = mem_length_;
    if (meta_data_->struct_type & ST_VTREE)
        limit = min(limit, meta_data_->vtree_offset);
    if (meta_data_->struct_type & ST_NS)
        limit = min(limit, meta_data_->ns_offset);
    if (meta_data_->struct_type & ST_DATA)
        limit = min(limit, meta_data_->data_offset);

    if (len + meta_data_->meta_data_length <= limit) {
        prev_len = meta_data_->meta_data_length;
        meta_data_->meta_data_length += len;
    } else {
        prev_len = mem_length_;
    err_code = memExtend(len, ps);
	if (err_code < 0) {
	    memLen() = fileLen();
	    return prev_len;
	}
	meta_data_->meta_data_length = mem_length_;
    }

    if (err_code >= 0)
        strcpy((char *) ((intptr_t) addr(prev_len) + offset),
            source_url.c_str());

    return prev_len;
}


int MMapObject::readColumnInfo(off_t offset, int size,
    vector<AttributeInfo> &attr_info)
{
    ColumnInfo *cols = (ColumnInfo *) addr(offset);
    for (int i = 0; i < size; ++i) {
        AttributeInfo ainfo;
        ainfo.name = string(cols[i].name);
        memcpy(&(ainfo.type), &(cols[i].type), AttrInfoTailSize);
        if (ainfo.type == STRING)
            is_ns_needed_ = true;
        attr_info.push_back(ainfo);
    }
    return 0;
}

int MMapObject::getIndex(const AttributeInfoIndex &idx,
    const string &col_name) const {
    int col = idx.getIndex(col_name);
    if (col < 0) {
        // col_name = obj.name
        string dim = getDimension(col_name);
        string attr = getAttribute(col_name);
        if (dim == obj_name_) {
            col = idx.getIndex(attr);
        }
    }
    return col;
}

void MMapObject::setColumnInfo(ColumnInfo *col_attr, const AttributeInfo &a_info) {
    strncpy(col_attr->name, a_info.name.c_str(), COLUMNATTR_LEN);
    memcpy(&(col_attr->type), &(a_info.type), AttrInfoTailSize);
}

void MMapObject::setColumnInfo(int i, const AttributeInfo &a_info) {
    ColumnInfo *col_attr = (ColumnInfo *) addr(meta_data_->attribute_offset);
    setColumnInfo(&(col_attr[i]), a_info);
}

void MMapObject::changeColumnName(int idx, const string &new_name)
{
    ColumnInfo *col_attr = (ColumnInfo *) addr(meta_data_->attribute_offset);
    strncpy(col_attr[idx].name, new_name.c_str(), COLUMNATTR_LEN);
}

off_t MMapObject::writeColumnInfo(const vector<AttributeInfo> &attr_info,
    int &err_code)
{
    off_t start_addr = memLen();
    off_t len = attr_info.size() * sizeof(ColumnInfo);
    err_code = memExtend(len);
    if (err_code < 0) {
        memLen() = fileLen();
        return err_code;
    }
    meta_data_->meta_data_length += len;

    ColumnInfo *col_attr = (ColumnInfo *) addr(start_addr);
    for (size_t i = 0; i < attr_info.size(); ++i) {
        setColumnInfo(&(col_attr[i]), attr_info[i]);
//        strncpy(col_attr[i].name, attr_info[i].name.c_str(), COLUMNATTR_LEN);
//        memcpy(&(col_attr[i].type), &(attr_info[i].type), AttrInfoTailSize);
    }

    return start_addr;
}

/*
off_t MMapObject::writeAttributeInfo(const vector<AttributeInfo> &attr_info)
{
//    off_t start_addr = memLen();
//    off_t prev_len = memLen();
//
//    memExtend(attr_info.size() * AttrInfoTailSize);
//    for (size_t i = 0; i < attr_info.size(); ++i) {
//	memcpy(addr(prev_len), (void *) &(attr_info[i].type), AttrInfoTailSize);
//	prev_len += AttrInfoTailSize;
//    }
//
//    meta_data_->meta_data_length += attr_info.size() * (AttrInfoTailSize);
//    for (size_t i = 0; i < attr_info.size(); ++i) {
//	urlCopy(attr_info[i].name.c_str());
//    }

    off_t first_attr_info_addr = 0;
    off_t prev_attr_info_addr;
    for (size_t i = 0; i < attr_info.size(); ++i) {
	off_t attr_info_addr = writeAttributeInfo(attr_info[i]);

	if (i == 0) {
	    first_attr_info_addr = attr_info_addr;
	}
	else {
	    *(attributeInfoNext(prev_attr_info_addr)) = attr_info_addr;
	}
	prev_attr_info_addr = attr_info_addr;
    }

    return first_attr_info_addr;
//    return start_addr;
}
*/

int MMapObject::writeAttributeURL(
    const string &source_url,
    const string &ns_url,
    const string &description)
{
//    assign(meta_data_->data_model_url, urlCopy(dm_url));
    //	string file_name = getFileName(file_path_);
//    string file_name = file_path_;
//    assign(meta_data_->file_url, urlCopy(file_name));
//    assign(meta_data_->source_url, urlCopy(source_url));
//    strncpy(meta_data_->source_url, source_url.c_str(), COLUMNATTR_LEN);
//    assign(meta_data_->root_xpath, urlCopy(root_xpath));
//    assign(meta_data_->ns_offset, urlCopy(ns_url, sizeof(void *)));

    urlCopy(&(meta_data_->source_url[0]), source_url.c_str());
    urlCopy(&(meta_data_->ns_url[0]), ns_url.c_str());
    assign(meta_data_->description, urlCopy(description));

    int err_code = 0;
//    if (measure_info) {
////        measure_info_ = *measure_info;
//        meta_data_->num_measure_info = measure_info->size();
//        assign(meta_data_->measure_info, writeColumnInfo(*measure_info, err));
////        assign(meta_data_->measure_info, writeAttributeInfo(*measure_info));
//    }
//    assign(meta_data_->distinct_attr, meta_data_->meta_data_length);
//    assign(meta_data_->dimension, urlCopy(dim));
//    assign(meta_data_->nameservice_dim, urlCopy(ns_dim));
//    urlCopy(&(meta_data_->nameservice_dim[0]), ns_dim.c_str());

//    assign(meta_data_->alias, urlCopy(alias));

//    size_t distinct_len = sizeof(int64_t) * meta_data_->level;
//    err_code = memExtend(distinct_len);
//    if (err_code < 0)
//        return err_code;
//    meta_data_->meta_data_length += distinct_len;
    meta_data_->num_measure_info = measure_info_.size();
#if defined(__x86_64__) || defined(_M_X64)
    off_t col_off = writeColumnInfo(measure_info_, err_code);
    if (err_code < 0) {
      return err_code;
    }
    assign(meta_data_->measure_info, col_off);
#else
    off_t tmp = writeColumnInfo(measure_info_, err_code);
    assign(meta_data_->measure_info, tmp);
#endif

    meta_data_->level = hrchy_info_.size();
#if defined(__x86_64__) || defined(_M_X64)
    col_off = writeColumnInfo(hrchy_info_, err_code);
    if (err_code < 0) {
      return err_code;
    }
    assign(meta_data_->attribute_offset, col_off);
#else
    tmp = writeColumnInfo(hrchy_info_, err_code);
    assign(meta_data_->attribute_offset, tmp);
#endif

    return err_code;
}

//void MMapObject::setDimensionInfo(const vector<DimensionInfo> *dim)
//{
//    if (dim == nullptr)
//	return;
//
//    meta_data_->dimension_info = memLen();
//    meta_data_->num_dimension_info = dim->size();
//    for (int i = 0; i < (int) dim->size(); i++) {
//	const DimensionInfo &di = dim->at(i);
//	urlCopy(di.name.c_str());
//	urlCopy(di.url.c_str());
//    }
//    dimension_info_ = *dim;
//}

vector<string> MMapObject::hierarchyAttribute() const
{
    return rank();
}

vector<string> MMapObject::rank() const
{
    return AttributeInfoToString(hrchy_info_);
//    std::vector<std::string> rank_hrchy;
//    for (unsigned int i = 0; i < hrchy_info_.size(); i++)
//	rank_hrchy.push_back(hrchy_info_[i].name);
//    return rank_hrchy;
}

vector<AttributeInfo> MMapObject::hierarchyInfo() const
{
    //	vector<AttributeInfo> hrchy_info;
    //	p = (const char*)addr(meta_data_->attribute_offset);
    //	for (size_t i = 0; i < hrchy_info.size(); ++i) {
    //		AttributeInfo info;
    //		info.name = hrchy_info[i];
    //		memcpy(&info + sizeof(string), p, AttrInfoTailSize);
    //		p += AttrInfoTailSize;
    //	}
    return hrchy_info_;
}

int MMapObject::getFactColumnInfo(int index, AttributeInfo &ainfo) const {
    if (index < 0 || index >= (int)measure_info_.size())
        return -1;
    ainfo = measure_info_[index];
    return index;
}

int MMapObject::getFactColumnInfo(const string &name,
    AttributeInfo &ainfo) const {
    int idx = measure_index_.getIndex(name);
    if (idx >= 0) {
        ainfo = measure_info_[idx];
        return idx;
    }
    return -1;
}


void MMapObject::updateAttributeInfoType(int col, int type) {
    ColumnInfo *cols = (ColumnInfo *) addr(meta_data_->attribute_offset);
    cols[col].type = type;  // first is type.
    hrchy_info_[col].type = type;
}

void MMapObject::updateAttributeInfoMaxLen(int col, int len) {
    ColumnInfo *cols = (ColumnInfo *) addr(meta_data_->attribute_offset);
    if (cols[col].max_len < len) {
        cols[col].max_len = len;
    }

    if (hrchy_info_[col].max_len < len) {
        hrchy_info_[col].max_len = len;
    }
}

/*
int MMapObject::startRead(ErrorInfo &err_info)
{
    pthread_rwlock_rdlock(&(this->rwlock_));
//    if (status_ != OBJ_READY) {
        pthread_rwlock_unlock(&(this->rwlock_));
        err_info.errcode = BOERLOCK;
//        BOErrorToString(err_info, getURLObjectName(URL()));
        return -1;
//    }
#if defined(DEBUG_LOCK)
    cout << "Read lock: " << URL() << endl;
#endif
    return 0;
}

int MMapObject::startWrite(ErrorInfo &err_info)
{
    pthread_rwlock_wrlock(&(this->rwlock_));
 //   if (status_ != OBJ_READY) {
        pthread_rwlock_unlock(&(this->rwlock_));
        err_info.errcode = BOEWLOCK;
//        BOErrorToString(err_info, getURLObjectName(URL()));
        return -1;
//    }
#if defined(DEBUG_LOCK)
    cout << "Write lock: " << URL() << endl;
#endif
    return 0;
}


int MMapObject::stopRead()
{
    pthread_rwlock_unlock(&(this->rwlock_));
#if defined(DEBUG_LOCK)
    cout << "Read unlock: " << URL() << endl;
#endif
    return 0;
}

int MMapObject::stopWrite()
{
    pthread_rwlock_unlock(&(this->rwlock_));
#if defined(DEBUG_LOCK)
    cout << "Write unlock: " << URL() << endl;
#endif
    return 0;
}
*/
//
//void MMapObject::setMeasureInfo(const vector<AttributeInfo> *smi)
//{
//	if (smi == nullptr)
//		return;
//	measure_info_.series = *smi;
//	meta_data_->.num_measure_info = smi->size();
//	meta_data_->.measure_info = writeAttributeInfo(*(vector<AttributeInfo> *)smi);
//}

///*-------------------------------
// * read-write locking mechanism
// * ------------------------------
// */
//
//int MMapObject::initLock() {
//	this->many_readers = 0;
//	this->one_writer = 0;
//
//	pthread_mutex_init(&(this->mutex), NULL);
//	pthread_cond_init(&(this->lock_is_freed), NULL);
//	return 0;
//}
//
//int MMapObject::destroyLock() {
//	pthread_mutex_destroy(&(this->mutex));
//	pthread_cond_destroy(&(this->lock_is_freed));
//
//	return 0;
//}
//
//int MMapObject::startReading(int64_t t_ts) {
//	pthread_mutex_lock(&(this->mutex));
//	/* Begin of Critical section */
//
//	/*
//	 * Wait, until THE writer finishes writing.
//	 */
//	while (this->one_writer > 0) {
//		/////// dead-lock prevention using wait-die
//		if (t_ts > getTimeStamp()) {
//			pthread_mutex_unlock(&mutex);
//			return -1;
//		}
//		///////
//		cout << "Hang in startReading. # of writer: " << this->one_writer << endl;
//		pthread_cond_wait(&(this->lock_is_freed), &(this->mutex));
//	}
//	//increase the number of readers
//	this->many_readers++;
//
//	/* End of Critical section */
//	pthread_mutex_unlock(&(this->mutex));
//	return 0;
//}
//
//int MMapObject::startWriting(int64_t t_ts) {
//	pthread_mutex_lock(&(this->mutex));
//	/* Begin of Critical section */
//
//	/**
//	 * Wait, until both
//	 * 1) THE writer has finished writing.
//	 * 2) ALL readers have finished reading.
//	 */
//	while (this->one_writer || this->many_readers) {
//		/////// dead-lock prevention using wait-die
//		if (t_ts > getTimeStamp()) {
//			pthread_mutex_unlock(&mutex);
//			return -1;
//		}
//		///////
//
//		cout << "Hang in startWriting. # of writer " << this->one_writer << " , # of readers : " << this->many_readers << endl;
//		pthread_cond_wait(&(this->lock_is_freed), &(this->mutex));
//	}
//	//obtain THE writer lock
//	this->one_writer++;
//
//	//Logically won't happen
////	if(this->one_writer > 1) {
////		std::cerr << "There are " << this->one_writer << " writers right now!" << std::endl;
////
////		//TODO: reset writer lock?
////		//this->one_writer = 0;
////		//pthread_cond_wait(&(this->lock_is_freed), &(this->mutex));
////
////		/* End of Critical section */
////		pthread_mutex_unlock(&(this->mutex));
////		return -1;
////	}
//
//	/* End of Critical section */
//	pthread_mutex_unlock(&(this->mutex));
//	return 1;
//}
//
//int MMapObject::stopReading() {
//	pthread_mutex_lock(&(this->mutex));
//	/* Begin of Critical section */
//
//	if (this->many_readers == 0) {
//		//No one is reading
//
//		/* End of Critical section */
//		pthread_mutex_unlock(&(this->mutex));
//		return -1;
//	} else {
//		/**
//		 * A reader finishes reading, release its lock.
//		 */
//		this->many_readers--;
//
//		if (this->many_readers == 0) {
//			pthread_cond_signal(&(this->lock_is_freed));
//		}
//		//Logically won't happen
////		else if(this->many_readers < 0) {
////			std::cerr << "There are " << this->many_readers << " reader right now!" << std::endl;
////
////			//TODO: reset reader lock?
////			//this->many_readers = 0;
////			//pthread_cond_wait(&(this->lock_is_freed), &(this->mutex));
////
////			/* End of Critical section */
////			pthread_mutex_unlock(&(this->mutex));
////			return -1;
////		}
//
//		/* End of Critical section */
//		pthread_mutex_unlock(&(this->mutex));
//		return 0;
//	}
//}
//
//int MMapObject::stopWriting() {
//	pthread_mutex_lock(&(this->mutex));
//	/* Begin of Critical section */
//
//	if (this->one_writer == 0) {
//		//no one is writing
//
//		/* End of Critical section */
//		pthread_mutex_unlock(&(this->mutex));
//		return -1;
//	} else {
//		/**
//		 * The writer finishes writing, release its lock
//		 */
//		this->one_writer = 0;
//
//		pthread_cond_broadcast(&(this->lock_is_freed));
//
//		/* End of Critical section */
//		pthread_mutex_unlock(&(this->mutex));
//		return 0;
//	}
//}
