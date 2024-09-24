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

#include <sys/types.h>
#include <sys/mman.h>
#include <dirent.h>
#include <errno.h>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <assert.h>
#include "utils/big_table_utils.h"
#include "sys_utils.h"
#include "mmap/mmap_tag_column_table_aux.h"
#include "ts_table.h"

/*
 * @Description: set lsn of file
 * @IN: lsn: the lsn to be set
 * @Return: void
 */
void MMapTagColumnTable::setLSN(kwdbts::TS_LSN lsn) {
  if (m_ptag_file_->memAddr()) {
    reinterpret_cast<TagColumnMetaData*>(m_ptag_file_->memAddr())->m_lsn = lsn;
  }

  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (!m_cols_[i]->isPrimaryTag()) {
      m_cols_[i]->setLSN(lsn);
    }
  }
  m_bitmap_file_->setLSN(lsn);
  m_index_->setLSN(lsn);
  m_meta_file_->setLSN(lsn);
}

/*
 * @Description: get min lsn of files
 * @IN:
 * @Return: min lsn
 */
kwdbts::TS_LSN MMapTagColumnTable::getLSN() {
  kwdbts::TS_LSN minLSN = reinterpret_cast<TagColumnMetaData*>(m_ptag_file_->memAddr())->m_lsn;
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (!m_cols_[i]->isPrimaryTag()) {
      minLSN = (m_cols_[i]->getLSN() > minLSN) ? minLSN : m_cols_[i]->getLSN();
    }
  }
  minLSN = (m_bitmap_file_->getLSN() > minLSN)
    ? minLSN : m_bitmap_file_->getLSN();
  minLSN = (m_index_->getLSN() > minLSN) ? minLSN : m_index_->getLSN();
  minLSN = (m_meta_file_->getLSN() > minLSN) ? minLSN : m_meta_file_->getLSN();

  return minLSN;
}

/*
 * @Description: set drop flag bit
 * @IN:
 * @Return: void
 */
void MMapTagColumnTable::setDrop() {
  if (m_ptag_file_->memAddr()) {
    reinterpret_cast<TagColumnMetaData*>(m_ptag_file_->memAddr())->m_droped = true;
  }

  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (!m_cols_[i]->isPrimaryTag()) {
      m_cols_[i]->setDrop();
    }
  }
  m_bitmap_file_->setDrop();
  m_index_->setDrop();
  m_meta_file_->setDrop();
  m_hps_file_->setDrop();
}

bool MMapTagColumnTable::isDroped() {
  if (m_ptag_file_->memAddr()) {
    return reinterpret_cast<TagColumnMetaData*>(m_ptag_file_->memAddr())->m_droped;
  } else {
    return false;
  }
}

/*
 * @Description: flush files
 * @IN:
 * @Return: 0
 */
int MMapTagColumnTable::sync_with_lsn(kwdbts::TS_LSN lsn) {
  setLSN(lsn);

  m_ptag_file_->sync(MS_SYNC);
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (!m_cols_[i]->isPrimaryTag()) {
      m_cols_[i]->sync(MS_SYNC);
    }
  }

  m_bitmap_file_->sync(MS_SYNC);
  m_index_->sync(MS_SYNC);
  m_meta_file_->sync(MS_SYNC);

  return 0;
}

void MMapTagColumnTable::sync(int flags) {
  m_ptag_file_->sync(flags);
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (!m_cols_[i]->isPrimaryTag()) {
      m_cols_[i]->sync(flags);
    }
  }

  m_bitmap_file_->sync(flags);
  m_index_->sync(flags);
  m_meta_file_->sync(flags);
}

TagTuplePack MMapTagColumnTable::GenTagPack(const char* primarytag, int len) {
  vector<TagInfo> schema;
  for (auto col: m_cols_) {
      schema.push_back(col->attributeInfo());
  }

  TagTuplePack packer(schema);
  TagTableRowID row;
  row = m_index_->get(primarytag, len);
  if (row == 0) {
    return std::move(packer);
  }

  const char *valPtr = nullptr;
  size_t valLen = 0;
  for (int col = 0; col < m_cols_.size(); ++col) {
    if (m_cols_[col]->attributeInfo().m_tag_type == PRIMARY_TAG) {
      valPtr = columnValueAddr(row, col);
      valLen = m_cols_[col]->attributeInfo().m_length;
    } else {
      bool isnull = isNull(row, col);
      if (isnull) {
	valPtr = nullptr;
	valLen = 0;
      } else {
	if (m_cols_[col]->attributeInfo().m_data_type == DATATYPE::VARSTRING ||
	    m_cols_[col]->attributeInfo().m_data_type == DATATYPE::VARBINARY) {
	  valPtr = m_cols_[col]->getVarValueAddr(row);
	  // the length is added 1 in MMapStringFile::push_back_nolock.
	  valLen = *reinterpret_cast<const uint16_t *>(valPtr) - 1;
	  valPtr = valPtr + 2;
	} else {
	  valPtr = m_cols_[col]->rowAddrNoNullBitmap(row);
	  valLen = m_cols_[col]->attributeInfo().m_length;
	}
      }
    }
    packer.fillData(valPtr, valLen);
  }

  uint32_t entityId = 0;
  uint32_t subgroupId = 0;
  const char* idPtr = entityIdStoreAddr(row);
  entityId = *reinterpret_cast<const uint32_t *>(idPtr);
  subgroupId = *reinterpret_cast<const uint32_t *>(idPtr + 4);
  packer.setEntityId(entityId);
  packer.setSubgroupId(subgroupId);

  return std::move(packer);
}

/*
 * @Description: do undo for create table in non-downtime scenario
 * @IN: path: the path of table
 * @IN: tbl_sub_path: the db of table
 * @IN: attrInfos: the column infos of table
 * @IN: encoding: encoding flags
 * @Return: >=0, success; <0 failed.
 *
 * In non-downtime scenario, all file should be opened,
 * so just set drop mark and remove.
 */
int MMapTagColumnTable::CreateTableForUndo(const string& path, string& tbl_sub_path,
                                           vector<TagInfo>& attrInfos,
                                           int encoding) {
  int rc = 0;

  return rc;
}

/*
 * @Description: do redo for create table in downtime scenario
 * @IN: path: the path of table
 * @IN: tbl_sub_path: the db of table
 * @IN: attrInfos: the column infos of table
 * @IN: encoding: encoding flags
 * @Return: >=0, success; <0 failed.
 *
 * In non-downtime scenario, all file should be opened,
 * so just set drop mark and remove.
 *
 * Currently, the recover process try to do open, if the open is success,
 * don't do redo; otherwise, tstable will remove directory.
 * so there's no need to do anything here.
 */
int MMapTagColumnTable::CreateTableForRedo(const string& path, string& tbl_sub_path,
                                           vector<TagInfo>& attrInfos,
                                           int encoding) {
  int rc = 0;

  return rc;
}

/*
 * @Description: scan the primary tag file to find the row matched
 *               the primary tag
 * @IN: groupid: groupid
 * @IN: entityid: entityid
 * @IN: primaryTag: the value of primaryTag
 * @Return: the row number matching the primaryTag.
 *
 * this is a bottleneck
 */
size_t MMapTagColumnTable::getRowNo(uint32_t entityid, uint32_t groupid,
				    void* primaryTag) {
  uint64_t egBuf;
  unsigned char* ptr = (unsigned char*) (&egBuf);
  memcpy(ptr, &entityid, sizeof(entityid));
  memcpy(ptr + sizeof(entityid), &groupid, sizeof(groupid));

  size_t i = size();
  for (; i > 0; i--) {
    if (*reinterpret_cast<uint64_t*>(entityIdStoreAddr(i)) == egBuf) {
      return i;
    }
  }

  LOG_WARN("not get the row matching with entityid %u and groupid %u",
    entityid, groupid);
  return 0;
}

/*
 * @Description: do undo for insert in no downtime scenario
 * @IN: groupid: groupid
 * @IN: entityid: entityid
 * @IN: primary_tag: the info required to do undo
 * @Return: >=0, success; <0 failed.
 *
 */
int MMapTagColumnTable::InsertForUndo(uint32_t groupid, uint32_t entityid,
				      const TSSlice& primary_tag) {
  int rc = 0;

  uint32_t groupid0;
  uint32_t entityid0;

  // don't do undo when tag table insert successfully
  if (getEntityIdGroupId(primary_tag.data, primary_tag.len, entityid0, groupid0)
    >= 0) {
    return rc;
  }

  size_t rowNo = getRowNo(entityid, groupid, primary_tag.data);
  if (rowNo == 0) {
    // don't find in primarytag column
    return rc;
  } else {
    setDeleteMark(rowNo);
    return 0;
  }
}

/*
 * @Description: do redo for insert in downtime scenario
 * @IN: groupid: groupid
 * @IN: entityid: entityid
 * @IN: payload: the info required to do redo
 * @Return: >=0, success; <0 failed.
 *
 */
int MMapTagColumnTable::InsertForRedo(uint32_t groupid, uint32_t entityid,
				      const TSSlice& primary_tag,
				      const TSSlice& tag) {
  uint32_t groupid0, entityid0;

  // don't do undo when tag table insert successfully
  if (getEntityIdGroupId(primary_tag.data, primary_tag.len, entityid0, groupid0)
    >= 0) {
    return 0;
  }
  uint32_t hashpoint;
  hashpoint = TsTable::GetConsistentHashId(primary_tag.data, primary_tag.len);
  size_t rowNo = getRowNo(entityid, groupid, primary_tag.data);
  if (rowNo == 0) {
    // don't find in primarytag column
    return insert(entityid, groupid, hashpoint, tag.data);
  } else {
    setDeleteMark(rowNo);
    return insert(entityid, groupid, hashpoint, tag.data);
  }
}

/*
 * @Description: do undo for delete in no downtime scenario
 * @IN: groupid: groupid
 * @IN: entityid: entityid
 * @IN: payload: the info required to do undo
 * @Return: >=0, success; <0 failed.
 *
 */
int MMapTagColumnTable::DeleteForUndo(uint32_t groupid, uint32_t entityid,
				      const TSSlice& primary_tag,
				      const TSSlice& tag_pack) {
  size_t rowNo;

  rowNo = getRowNo(entityid, groupid, primary_tag.data);
  if (rowNo == 0) {
    if (tag_pack.data == nullptr) {
      return -1;
    }

    vector<TagInfo> schema;
    for (auto col: m_cols_) {
      schema.push_back(col->attributeInfo());
    }
    uint32_t hashpoint = 0;
    if (!EngineOptions::isSingleNode()) {
     getHashpointByRowNum(rowNo, &hashpoint);
    }
    TagTuplePack tag(schema, tag_pack.data, tag_pack.len);
    return insert(entityid, groupid, hashpoint, tag.getTags().data);
  } else {
    unsetDeleteMark(rowNo);
    int rc = m_index_->put(reinterpret_cast<char*>(record(rowNo)),
      m_meta_data_->m_primary_tag_store_size - k_entity_group_id_size, rowNo);
    if (rc >= 0) {
      return 0;
    } else {
      setDeleteMark(rowNo);
      return -1;
    }
  }

  return -1;
}

/*
 * @Description: do redo for delete in downtime scenario
 * @IN: groupid: groupid
 * @IN: entityid: entityid
 * @IN: payload: the info required to do redo
 * @Return: >=0, success; <0 failed.
 *
 */
int MMapTagColumnTable::DeleteForRedo(uint32_t groupid, uint32_t entityid,
				      const TSSlice& primary_tag) {
  size_t rowNo;
  rowNo = m_index_->get(primary_tag.data, primary_tag.len);
  if (rowNo > 0) {
    setDeleteMark(rowNo);
  } else {
    rowNo = getRowNo(entityid, groupid, primary_tag.data);
    if (rowNo == 0) {
      return 0;
    } else {
      // there is residue in primarytag, delete it.
      setDeleteMark(rowNo);
      return 0;
    }
  }
  return 0;
}

int MMapTagColumnTable::UpdateForRedo(uint32_t groupid, uint32_t entityid,
                                      const TSSlice& primary_tag, const TSSlice& tag) {
  size_t rowNo;
  rowNo = m_index_->get(primary_tag.data, primary_tag.len);
  if (rowNo > 0) {
    // setDeleteMark(rowNo);
    ErrorInfo err_info;
    DeleteTagRecord(primary_tag.data, primary_tag.len, err_info);
    if (err_info.errcode < 0) {
      LOG_ERROR("UpdateForRedo failed, delete_tag_record error, error msg: %s", err_info.errmsg.c_str())
      return err_info.errcode;
    }
  }

  rowNo = getRowNo(entityid, groupid, primary_tag.data);
  if (rowNo > 0) {
    // there is residue in primarytag, delete it.
    setDeleteMark(rowNo);
  }

  uint32_t hashpoint = TsTable::GetConsistentHashId(primary_tag.data, primary_tag.len);
  return insert(entityid, groupid, hashpoint, tag.data);
}

int MMapTagColumnTable::UpdateForUndo(uint32_t group_id, uint32_t entity_id, const TSSlice& primary_tag,
                                      const TSSlice& new_tag, const TSSlice& old_tag) {
  int rc = 0;
  ErrorInfo err_info;
  rc = DeleteTagRecord(primary_tag.data, primary_tag.len, err_info);
  if (rc < 0) {
    return rc;
  }

  if (old_tag.data == nullptr) {
    return -1;
  }

  vector<TagInfo> schema;
  for (auto col: m_cols_) {
    schema.push_back(col->attributeInfo());
  }
  uint32_t hashpoint = TsTable::GetConsistentHashId(primary_tag.data, primary_tag.len);
  TagTuplePack tag(schema, old_tag.data, old_tag.len);
  return insert(entity_id, group_id, hashpoint, tag.getTags().data);
}

/*
 * @Description: do undo for drop in no downtime scenario
 * @Return: >=0, success; <0 failed.
 *
 */
int MMapTagColumnTable::DropTableForUndo() {
  return remove();
}

/*
 * @Description: do redo for drop in downtime scenario
 * @Return: >=0, success; <0 failed.
 *
 */
int MMapTagColumnTable::DropTableForRedo() {
  return remove();
}

int MMapTagColumnTable::AlterTableForUndo(uint32_t groupid, uint32_t entityid,
                                          TagInfo& oldInfo, TagInfo& newInfo,
					  int opCode) {
  switch (opCode) {
    case 1:
      return AlterRenameRU(oldInfo, newInfo, false);
      break;
    case 2:
      return AlterAlterRU(oldInfo, newInfo, false);
      break;
    case 3:
      return AlterAddRU(newInfo, false);
      break;
    case 4:
      return AlterDropRU(oldInfo, false);
      break;
    default:
      return -1;
  }

  return -1;
}

int MMapTagColumnTable::AlterTableForRedo(uint32_t groupid, uint32_t entityid,
                                          TagInfo& oldInfo, TagInfo& newInfo,
					  int opCode) {
  switch (opCode) {
    case 1:
      return AlterRenameRU(oldInfo, newInfo, true);
      break;
    case 2:
      return AlterAlterRU(oldInfo, newInfo, true);
      break;
    case 3:
      return AlterAddRU(newInfo, true);
      break;
    case 4:
      return AlterDropRU(oldInfo, true);
      break;
    default:
      return -1;
  }

  return -1;
}

TagInfo* MMapTagColumnTable::getTagInfo(uint32_t colId, int& colIdx) {
  colIdx = -1;
  TagInfo* cols = reinterpret_cast<TagInfo*>
    (static_cast<uint8_t*>(m_meta_file_->startAddr())
     + m_meta_data_->m_column_info_offset);
  for (int i = 0; i < m_meta_data_->m_column_count; i++) {
    if (cols[i].m_id == colId) {
      colIdx = i;
      return (cols + i);
    }
  }

  return nullptr;
}

TagColumn* MMapTagColumnTable::getTagCol(uint32_t colId, int& colIdx) {
  colIdx = -1;
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (m_cols_[i]->attributeInfo().m_id == colId) {
      colIdx = i;
      return m_cols_[i];
    }
  }

  return nullptr;
}

int MMapTagColumnTable::findSerialNo(TagInfo& info) {
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (m_cols_[i]->attributeInfo().m_id < info.m_id) {
      continue;
    } else {
      return i;
    }
  }

  return m_cols_.size();
}

static int copyFile(const string& des, const string& src, int flag) {
  int rc = 0;
  if (IsExists(des)) {
    rc = remove(des.c_str());
    //LOG_ERROR("copyFile, remove file %s", des.c_str());
    if (rc != 0) {
      LOG_ERROR("failed to remove file %s, errno: %d",
        des.c_str(), errno);
      return rc;
    }
  }

  int desFd = open(des.c_str(), flag | O_CREAT | O_RDWR,
                   S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
  if (desFd < 0) {
    LOG_ERROR("failed to open destination file %s, errno: %d",
      des.c_str(), errno);
    return desFd;
  }

  int srcFd = open(src.c_str(), O_RDONLY);
  if (srcFd < 0) {
    LOG_ERROR("failed to open source file %s, errno: %d",
      src.c_str(), errno);
    close(desFd);
    return srcFd;
  }

  char buf[8 * 1024];
  size_t leftRSize = 0;
  struct stat statBuf;
  rc = fstat(srcFd, &statBuf);
  if (rc != 0) {
    LOG_ERROR("failed to fstat on source file %s, errno: %d",
      src.c_str(), errno);
    close(desFd);
    close(srcFd);
    return rc;
  }

  if (statBuf.st_size <= 0) {
    LOG_WARN("source file %s size is 0", src.c_str());
    close(desFd);
    close(srcFd);
    return -1;
  }
  leftRSize = statBuf.st_size;

  while (leftRSize > 0) {
    int rSize = read(srcFd, buf, 8 * 1024);
    if (rSize < 0) {
      LOG_ERROR("failed to read source file %s, errno: %d",
        src.c_str(), errno);
      rc = -1;
      close(desFd);
      close(srcFd);
      return rc;
    }

    leftRSize -= rSize;
    int leftWSize = rSize;
    while (leftWSize > 0) {
      int wSize = write(desFd, buf + (rSize - leftWSize), leftWSize);
      if (wSize < 0) {
	LOG_ERROR("failed to write destination file %s, errno: %d",
      des.c_str(), errno);
        rc = -1;
        close(desFd);
        close(srcFd);
        return rc;
      }
      leftWSize -= wSize;
    }
  }

  close(desFd);
  close(srcFd);

  //struct stat statBuf1;
  //stat(des.c_str(), &statBuf1);
  //LOG_ERROR("copyFile, fstat des file %s size %ld", des.c_str(), statBuf1.st_size);
  return 0;
}

static size_t lsnOffset(const TagTableFileType fileType) {
  switch (fileType) {
    case TTFT_META:
      return lsnOffsetInMeta();
    case TTFT_HEADER:
    case TTFT_TAG:
      return lsnOffsetInTag();
    case TTFT_INDEX:
      return lsnOffsetInHashIndex();
    case TTFT_STR:
      return lsnOffsetInStr();
    case TTFT_PRIMARYTAG:
      return lsnOffsetInPrimaryTag();
    case TTFT_HASHPOINT:
      return 0;
    case TTFT_UNKNOWN:
    default:
      return -1;
  }

  return -1;
}

static size_t dropOffset(const TagTableFileType fileType) {
  // kwdbts::TS_LSN is uint64_t, maybe use kwdbts::TS_LSN universally
  return lsnOffset(fileType) + sizeof(uint64_t);
}

static int setDrop(const string& fileName, const TagTableFileType fileType,
		   const bool drop) {
  int fd = ::open(fileName.c_str(), O_RDWR);
  if (fd < 0) {
    LOG_ERROR("failed to open file %s, errno: %d", fileName.c_str(), errno);
    return -1;
  }

  int rc = ::lseek(fd, dropOffset(fileType), SEEK_SET);
  if (rc < 0) {
    LOG_ERROR("failed to lseek file %s, errno: %d", fileName.c_str(), errno);
    ::close(fd);
    return -1;
  }

  char dropFlag = drop ? static_cast<char>(1) : static_cast<char>(0);
  rc = ::write(fd, &dropFlag, 1);
  if (rc != 1) {
    LOG_ERROR("failed to write file %s, errno: %d", fileName.c_str(), errno);
    ::close(fd);
    return -1;
  }

  ::close(fd);
  return 0;
}

static int getLsnAndDrop(const string& fileName,
			 const TagTableFileType fileType,
			 uint64_t& lsn, bool& drop) {
  int fd = ::open(fileName.c_str(), O_RDWR);
  if (fd < 0) {
    LOG_ERROR("failed to open file %s, errno: %d", fileName.c_str(), errno);
    return -1;
  }

  int rc = ::lseek(fd, lsnOffset(fileType), SEEK_SET);
  if (rc < 0) {
    LOG_ERROR("failed to lseek file %s, errno: %d", fileName.c_str(), errno);
    ::close(fd);
    return -1;
  }

  constexpr size_t bufSize = sizeof(uint64_t) + sizeof(char);
  char buf[bufSize] = {0};
  size_t left = bufSize;
  while (left > 0) {
    rc = ::read(fd, buf + bufSize - left, left);
    if (rc < 0) {
      LOG_ERROR("failed to read file %s, errno: %d", fileName.c_str(), errno);
      ::close(fd);
      return -1;
    }

    left -= rc;
  }
  ::close(fd);

  lsn = *reinterpret_cast<uint64_t*>(buf);
  if (buf[sizeof(uint64_t)] == static_cast<char>(0)) {
    drop = false;
  } else {
    drop = true;
  }

  return 0;
}

string MMapTagColumnTable::getFileDir() {
  size_t lastSep = m_ptag_file_->realFilePath().find_last_of('/');
  return m_ptag_file_->realFilePath().substr(0, lastSep);
}

int MMapTagColumnTable::buildMetaName(bool isold, string &metaFileName) {
    string baseFileName = getFileDir() + '/' + name() + ".mt";
    metaFileName = isold ? baseFileName + ".bak" : baseFileName;

    return 0;
}

int MMapTagColumnTable::buildFileName(const TagInfo& info, bool isOld,
                                      string& priFileName,
				      string& secFileName) {
  string baseFileName = getFileDir() + '/' + name() + '.'
    + to_string(info.m_id);

  priFileName = isOld ? baseFileName + ".bak" : baseFileName;
  if (info.m_data_type == VARSTRING || info.m_data_type == VARBINARY) {
    secFileName = isOld ? baseFileName + ".s.bak" : baseFileName + ".s";
  }

  return 0;
}

bool MMapTagColumnTable::isEqualTagInfo(const TagInfo& info1,
					const TagInfo& info2) {
  if (info1.m_id == info2.m_id
      && info1.m_data_type == info2.m_data_type
      && info1.m_length == info2.m_length
      // If info2 is from wal, m_offset not be set, so ignore it.
      //&& info1.m_offset == info2.m_offset
      && info1.m_size == info2.m_size
      && info1.m_tag_type == info2.m_tag_type) {
    return true;
  } else {
    return false;
  }
}

bool MMapTagColumnTable::isOldMetaFileExist() {
  string oldFileName;
  buildMetaName(true, oldFileName);

  return IsExists(oldFileName);
}

bool MMapTagColumnTable::isOldFileExist(TagInfo& info) {
  string priOldFileName;
  string secOldFileName;
  buildFileName(info, true, priOldFileName, secOldFileName);

  bool rc = IsExists(priOldFileName);
  if (secOldFileName.empty()) {
    return rc;
  }

  bool rc1 = IsExists(secOldFileName);

  return rc && rc1;
}

int MMapTagColumnTable::flipOldMetaFile(bool toOld) {
  string oldFileName;
  buildMetaName(true, oldFileName);
  string newFileName;
  buildMetaName(false, newFileName);

  int rc = 0;
  if (toOld) {
    rc = copyFile(oldFileName, newFileName, m_flags_);
    if (rc != 0) {
      return rc;
    }
  } else {
    rc = copyFile(newFileName, oldFileName, m_flags_);
    if (rc != 0) {
      return rc;
    }
  }

  return rc;
}

int MMapTagColumnTable::flipOldFile(TagInfo& info, bool toOld) {
  string priFileName;
  string secFileName;
  buildFileName(info, false, priFileName, secFileName);

  string priOldFileName;
  string secOldFileName;
  buildFileName(info, true, priOldFileName, secOldFileName);

  int rc = 0;
  if (toOld) {
    rc = copyFile(priOldFileName, priFileName, m_flags_);
    if (rc != 0) {
      return rc;
    }

    if (!secFileName.empty()) {
      rc = copyFile(secOldFileName, secFileName, m_flags_);
    }
  } else {
    rc = copyFile(priFileName, priOldFileName, m_flags_);
    if (rc != 0) {
      return rc;
    }

    if (!secOldFileName.empty()) {
      rc = copyFile(secFileName, secOldFileName, m_flags_);
    }
  }

  return rc;
}

void MMapTagColumnTable::recalcOffset() {
  uint32_t colOffset = 0;
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    TagColumn* tagCol = m_cols_[i];
    // For primary tag, only the offset maybe change.
    if (tagCol->attributeInfo().m_tag_type != PRIMARY_TAG) {
      if (tagCol->attributeInfo().m_data_type == DATATYPE::VARSTRING ||
          tagCol->attributeInfo().m_data_type == DATATYPE::VARBINARY) {
        tagCol->attributeInfo().m_size = sizeof(intptr_t);
      }
    }

    tagCol->attributeInfo().m_offset = colOffset;
    colOffset += tagCol->attributeInfo().m_size;
  }

  return;
}

void MMapTagColumnTable::recalcRecordSize() {
  m_meta_data_->m_header_size = 1 + ((m_cols_.size() + 7) / 8);
  m_meta_data_->m_bitmap_size = ((m_cols_.size() + 7) / 8);

  uint32_t recSize = 0;
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    recSize += m_cols_[i]->attributeInfo().m_size;
  }

  m_meta_data_->m_record_store_size = recSize + k_entity_group_id_size;
  m_meta_data_->m_record_size = recSize;

  m_meta_data_->m_record_store_size += m_meta_data_->m_header_size;
  m_meta_data_->m_record_size += m_meta_data_->m_bitmap_size;

  return;
}

// m_meta_data_ is read from back meta file, don't need recalc.
// in the future, replace the function with recalcOffset.
void MMapTagColumnTable::updateOffsetAndSize() {
  uint32_t col_offset = 0;
  for (auto it : m_cols_) {
    // For primary tag, only the offset maybe change.
    if (it->attributeInfo().m_tag_type != PRIMARY_TAG) {
      if (it->attributeInfo().m_data_type == DATATYPE::VARSTRING ||
          it->attributeInfo().m_data_type == DATATYPE::VARBINARY) {
        it->attributeInfo().m_size = sizeof(intptr_t);
      }
    }

    it->attributeInfo().m_offset = col_offset;
    col_offset += it->attributeInfo().m_size;

    //  construct the output with MMapTagColumnTable::initColumn
    //  and MMapTagColumnTable::init
    /*
    LOG_ERROR("%s%s,m_id=%d,m_data_type=%d,m_length=%d,m_offset=%d,"
	      "m_size=%d,m_tag_type=%d",
	      db_path_.c_str(), m_db_name_.c_str(),
	      it->attributeInfo().m_id, it->attributeInfo().m_data_type,
	      it->attributeInfo().m_length, it->attributeInfo().m_offset,
	      it->attributeInfo().m_size, it->attributeInfo().m_tag_type);
    */
  }

  /*
  LOG_ERROR("%s%s, m_magic=%d,m_record_size=%d,m_record_store_size=%d,"
	    "m_header_size=%d,m_bitmap_size=%d,m_primary_tag_size=%d,"
	    "m_primary_tag_store_size=%d,m_column_count=%d,"
	    "m_column_info_offset=%d,m_record_start_offset=%d,"
	    "m_row_count=%ld,m_reserve_row_count=%ld,"
	    "m_valid_row_count=%ld,m_mem_length=%ld,"
	    "m_entitygroup_id=%ld", db_path_.c_str(), m_db_name_.c_str(),
	    m_meta_data_->m_magic,m_meta_data_->m_record_size,
	    m_meta_data_->m_record_store_size,m_meta_data_->m_header_size,
	    m_meta_data_->m_bitmap_size,m_meta_data_->m_primary_tag_size,
	    m_meta_data_->m_primary_tag_store_size,
	    m_meta_data_->m_column_count,
	    m_meta_data_->m_column_info_offset,
	    m_meta_data_->m_record_start_offset,
	    m_meta_data_->m_row_count,m_meta_data_->m_reserve_row_count,
	    m_meta_data_->m_valid_row_count,m_meta_data_->m_mem_length,
	    m_meta_data_->m_entitygroup_id);
  */
}

TagColumn* MMapTagColumnTable::openTagCol(int32_t idx, const TagInfo& info) {
  TagColumn* tagCol = new TagColumn(idx, info);
  if (!tagCol) {
    LOG_ERROR("failed to new TagColumn object for the tag table %s%s",
      m_db_name_.c_str(), m_name_.c_str());
    return nullptr;
  }

  int rc = tagCol->open(m_name_ + "." + std::to_string(info.m_id),
                        m_db_path_, m_db_name_, (m_flags_ & ~O_CREAT));
  if (rc < 0) {
    LOG_ERROR("failed to open the tag file(%u) of the tag table %s%s",
      info.m_id, m_db_name_.c_str(), m_name_.c_str());
    delete tagCol;
    return nullptr;
  }

  return tagCol;
}

void MMapTagColumnTable::cleanTagCol(const TagInfo& info, bool ext) {
    string priFileName;
    string secFileName;
    buildFileName(info, false, priFileName, secFileName);
    if (ext) {
      priFileName += ".0000001";
      secFileName += ".0000001";
    }
    ::remove(priFileName.c_str());
    if (!secFileName.empty()) {
      ::remove(secFileName.c_str());
    }

    return;
}

int MMapTagColumnTable::AlterRenameRU(TagInfo& oldInfo, TagInfo& newInfo,
				      bool isRedo) {
  // the tag table has no name info.
  // do nothing
  return 0;
}

int MMapTagColumnTable::AlterAlterRU(TagInfo& oldInfo, TagInfo& newInfo,
				     bool isRedo) {
  int colIdx = -1;
  if (isRedo) {
    // In a checkpoint cycle, there is and only one ddl op.
    // If the op don't success, it should not been redo.
    TagInfo* oldTagInfo = getTagInfo(oldInfo.m_id, colIdx);
    if (oldTagInfo != nullptr) {
      LOG_ERROR("the tag(%u) still exist in the tag table %s%s, can't do redo",
        oldInfo.m_id, m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }

    TagInfo* newTagInfo = getTagInfo(newInfo.m_id, colIdx);
    if (newTagInfo == nullptr) {
      LOG_ERROR("the tag(%u) don't exist in the tag table %s%s, can't do redo",
        newInfo.m_id, m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }

    TagColumn* newTagCol = getTagCol(newInfo.m_id, colIdx);
    if ((newTagCol == nullptr) ||
        ((newTagInfo->m_data_type != newInfo.m_data_type) ||
         (newTagInfo->m_length != newInfo.m_length))) {
      LOG_ERROR("the metadata info don't match with files in the tag table "
        "%s%s, can't do redo",
        m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }

    return 0;
  } else {
    bool hasOldMeta = isOldMetaFileExist();
    if (hasOldMeta == false) {
      // alter don't change files, don't need undo;
      LOG_INFO("don't need do something about the tag table %s%s%s, except clean added files.",
               m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      cleanTagCol(oldInfo, true);
      return 0;
    }
#if 0
    else {
      TagInfo* oldTagInfo = getTagInfo(oldInfo.m_id, colIdx);
      if ((oldTagInfo != nullptr) && (isEqualTagInfo(*oldTagInfo, oldInfo))) {
        LOG_INFO("don't need do something about the tag table %s%s%s, "
		 "except to mark mt.bak to remove and clean added files.",
		 db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
	cleanTagCol(oldInfo, true);
	string oldMetaFileName;
	buildMetaName(true, oldMetaFileName);
	::setDrop(oldMetaFileName, TTFT_META, true);
	// cloneMetaData failed, don't need undo
	return 0;
      }
    }
#endif
    // recovery meta
    int rc = flipOldMetaFile(false);
    if (rc != 0) {
      LOG_ERROR("failed to rename meta file of the tag table %s%s%s, undo failed",
                m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }

    TagInfo metaInfo = {0x00};
    metaInfo.m_offset = 0;
    metaInfo.m_size = kwdbts::EngineOptions::pageSize();
    TagColumn* metaFile = new TagColumn(-1, metaInfo);
    if (!metaFile) {
      LOG_ERROR("failed to new TagColumn object for the tag table %s%s%s",
                m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }
    rc = metaFile->open(m_name_ + ".mt",
                        m_db_path_, m_db_name_, (m_flags_ & ~O_CREAT));
    if (rc < 0) {
       LOG_ERROR("failed to open meta file of the tag table %s%s%s, undo failed.",
                 m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      delete metaFile;
      return -1;
    }
    metaFile->unsetDrop();

    //m_meta_file_->remove();
    delete m_meta_file_;
    m_meta_file_ = metaFile;
    setMetaData();

    bool hasOldFile = isOldFileExist(oldInfo);
    if (hasOldFile == false) {
      TagColumn* tagCol = getTagCol(oldInfo.m_id, colIdx);
      if (tagCol == nullptr) {
	// The tag file must be old, open it;
	int serialNo = findSerialNo(oldInfo);
	tagCol = openTagCol(serialNo, oldInfo);
	if (tagCol == nullptr) {
    LOG_WARN("failed to open the tag file(%u) of the tag table %s%s%s, undo failed",
             oldInfo.m_id, m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
	  return -1;
	}

	vector<TagColumn*>::iterator it = m_cols_.begin();
	while (serialNo-- > 0) {
	    ++it;
	}
	m_cols_.insert(it, tagCol);
	tagCol->unsetDrop();
	return 0;
      } else {
	// This is crash undo, check whether info is equal.
	if (isEqualTagInfo(tagCol->attributeInfo(), oldInfo)) {
	  return 0;
	} else {
	  LOG_ERROR("the meta of the tag table %s%s%s was damaged, undo failed",
              m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
	  return -1;
	}
      }
    }

    TagColumn* tagCol = getTagCol(oldInfo.m_id, colIdx);
    if (tagCol != nullptr) {
      vector<TagColumn*>::iterator it = m_cols_.begin();
      while (tagCol != *it) {
        ++it;
      }
      m_cols_.erase(it);
      tagCol->remove();
      delete tagCol;
    }

    int serialNo = findSerialNo(oldInfo);
    rc = flipOldFile(oldInfo, false);
    if (rc != 0) {
      LOG_ERROR("failed to rename the tag file(%u) of the tag table %s%s%s, undo failed",
                oldInfo.m_id, m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }

    tagCol = openTagCol(serialNo, oldInfo);
    if (tagCol == nullptr) {
      LOG_WARN("failed to open the tag file(%u) of the tag table %s%s%s, undo failed.",
               oldInfo.m_id, m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }
    tagCol->unsetDrop();

    vector<TagColumn*>::iterator it = m_cols_.begin();
    while (serialNo-- > 0) {
      ++it;
    }
    m_cols_.insert(it, tagCol);
    updateOffsetAndSize();

    string priOldFileName;
    string secOldFileName;
    buildFileName(oldInfo, true, priOldFileName, secOldFileName);
    ::setDrop(priOldFileName, TTFT_TAG, true);
    if (!secOldFileName.empty()) {
      ::setDrop(secOldFileName, TTFT_STR, true);
    }

    string oldMetaFileName;
    buildMetaName(true, oldMetaFileName);
    ::setDrop(oldMetaFileName, TTFT_META, true);

    return 0;
  }

  return -1;
}

int MMapTagColumnTable::AlterAddRU(TagInfo& newInfo, bool isRedo) {
  int colIdx = -1;
  if (isRedo) {
    TagInfo* tagInfo = getTagInfo(newInfo.m_id, colIdx);
    // In a checkpoint cycle, there is and only one ddl op.
    // If the op don't success, it should not been redo.
    if (tagInfo == nullptr) {
      LOG_ERROR("the tag(%u) don't exist in the tag table %s%s, can't do redo",
        newInfo.m_id, m_db_name_.c_str(), m_name_.c_str());
      return -1;
    } else {
      TagColumn* tagCol = getTagCol(newInfo.m_id, colIdx);
      if ((tagCol == nullptr) ||
          ((tagInfo->m_data_type != newInfo.m_data_type) ||
           (tagInfo->m_size != newInfo.m_size))) {
        LOG_ERROR("the metadata info don't match with files in the tag table "
          "%s%s, can't do redo",
		  m_db_name_.c_str(), m_name_.c_str());
        return -1;
      }
    }

    return 0;
  } else {
    bool hasOldMeta = isOldMetaFileExist();
    if (hasOldMeta == false) {
      // Corresponding to add tag and backup meta error in AddTagColumn.
      // new tag don't been in meta and m_cols.
      LOG_INFO("remove add files from the tag table %s%s%s",
               m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      cleanTagCol(newInfo);
      return 0;
    }

    int rc = flipOldMetaFile(false);
    if (rc != 0) {
      LOG_ERROR("failed to rename meta file of the tag table %s%s%s, undo failed",
                m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }

    TagInfo metaInfo = {0x00};
    metaInfo.m_offset = 0;
    metaInfo.m_size = kwdbts::EngineOptions::pageSize();
    TagColumn* metaFile = new TagColumn(-1, metaInfo);
    if (!metaFile) {
        LOG_ERROR("failed to new TagColumn object for the tag table %s%s",
          m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }
    rc = metaFile->open(m_name_ + ".mt",
                        m_db_path_, m_db_name_, (m_flags_ & ~O_CREAT));
    if (rc < 0) {
      LOG_ERROR("failed to open meta file of the tag table %s%s%s, undo failed.",
                m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      delete metaFile;
      return -1;
    }
    metaFile->unsetDrop();

    //m_meta_file_->remove();
    delete m_meta_file_;
    m_meta_file_ = metaFile;
    setMetaData();

    TagColumn* tagCol = getTagCol(newInfo.m_id, colIdx);
    if (tagCol == nullptr) {
      string oldMetaFileName;
      buildMetaName(true, oldMetaFileName);
      ::setDrop(oldMetaFileName, TTFT_META, true);
      return 0;
    }

    if (m_cols_.back() != tagCol) {
      LOG_ERROR("the added tag(%u) in the tag table %s%s%s is not the last, undo failed.",
                newInfo.m_id, m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }
    m_cols_.pop_back();
    tagCol->remove();
    delete tagCol;

    string oldMetaFileName;
    buildMetaName(true, oldMetaFileName);
    ::setDrop(oldMetaFileName, TTFT_META, true);

    return 0;
  }

  return -1;
}

int MMapTagColumnTable::AlterDropRU(TagInfo& oldInfo, bool isRedo) {
  int colIdx = -1;
  if (isRedo) {
    TagInfo* tagInfo = getTagInfo(oldInfo.m_id, colIdx);
    TagColumn* tagCol = getTagCol(oldInfo.m_id, colIdx);
    if ((tagInfo == nullptr) && (tagCol == nullptr)) {
      return 0;
    } else {
      // In a checkpoint cycle, there is and only one ddl op.
      // If the op don't success, it should not been redo.
      LOG_ERROR("the metadata info don't match with files in the tag table "
        "%s%s, can't do redo",
        m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }
  } else {
    bool hasOldMeta = isOldMetaFileExist();
    if (hasOldMeta == false) {
      // alter should don't change files, only reopen tag file.
      TagColumn* tagCol = getTagCol(oldInfo.m_id, colIdx);
      if (tagCol == nullptr) {
	// This is a error undo, corresponding to "tag don't exist" error,
	// backup meta error in DropTagColumn. Only need reopen tag file.
	int serialNo = findSerialNo(oldInfo);
	tagCol = openTagCol(serialNo, oldInfo);
	if (tagCol == nullptr) {
    LOG_WARN("failed to open the tag file(%u) of the tag table %s%s%s, undo failed.",
             oldInfo.m_id, m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
	  return -1;
	}

	vector<TagColumn*>::iterator it = m_cols_.begin();
	while (serialNo-- > 0) {
	    ++it;
	}
	m_cols_.insert(it, tagCol);
	tagCol->unsetDrop();
	updateOffsetAndSize();
	return 0;
      } else {
	// This is crash undo, check whether info is equal.
	if (isEqualTagInfo(tagCol->attributeInfo(), oldInfo)) {
	  return 0;
	} else {
	  LOG_ERROR("the meta of the tag table %s%s%s was damaged, undo failed",
              m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
	  return -1;
	}
      }
    }

    // recovery meta
    int rc = flipOldMetaFile(false);
    if (rc != 0) {
      LOG_ERROR("failed to rename meta file of the tag table %s%s%s, undo failed.",
                m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }

    TagInfo metaInfo = {0x00};
    metaInfo.m_offset = 0;
    metaInfo.m_size = kwdbts::EngineOptions::pageSize();
    TagColumn* metaFile = new TagColumn(-1, metaInfo);
    if (!metaFile) {
      LOG_ERROR("failed to new TagColumn object for the tag table %s%s%s",
                m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }
    rc = metaFile->open(m_name_ + ".mt",
                        m_db_path_, m_db_name_, (m_flags_ & ~O_CREAT));
    if (rc < 0) {
      LOG_ERROR("failed to open the meta file of the tag table %s%s%s, undo failed.",
                m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      delete metaFile;
      return -1;
    }
    metaFile->unsetDrop();

    //m_meta_file_->remove();
    delete m_meta_file_;
    m_meta_file_ = metaFile;
    setMetaData();

    bool hasOldFile = isOldFileExist(oldInfo);
    if (hasOldFile == false) {
      TagColumn* tagCol = getTagCol(oldInfo.m_id, colIdx);
      if (tagCol == nullptr) {
	// This is a error undo, corresponding to change meta error,
	// backup tag error in DropTagColumn. Only need reopen tag file.
	int serialNo = findSerialNo(oldInfo);
	tagCol = openTagCol(serialNo, oldInfo);
	if (tagCol == nullptr) {
    LOG_WARN("failed to open the tag file(%u) of the tag table %s%s%s, undo failed.",
             oldInfo.m_id, m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
	  return -1;
	}

	vector<TagColumn*>::iterator it = m_cols_.begin();
	while (serialNo-- > 0) {
	    ++it;
	}
	m_cols_.insert(it, tagCol);
	tagCol->unsetDrop();
	updateOffsetAndSize();
	return 0;
      } else {
	// This is crash undo, check whether info is equal.
	if (isEqualTagInfo(tagCol->attributeInfo(), oldInfo)) {
	  return 0;
	} else {
	  LOG_ERROR("the meta of the tag table %s%s%s was damaged, undo failed",
              m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
	  return -1;
	}
      }
    }

    TagColumn* tagCol = getTagCol(oldInfo.m_id, colIdx);
    if (tagCol != nullptr) {
      vector<TagColumn*>::iterator it = m_cols_.begin();
      while (tagCol != *it) {
        ++it;
      }
      m_cols_.erase(it);
      tagCol->remove();
      delete tagCol;
    }

    int serialNo = findSerialNo(oldInfo);
    rc = flipOldFile(oldInfo, false);
    if (rc != 0) {
      LOG_ERROR("failed to rename the tag file(%u) of the tag table %s%s%s, undo failed",
                oldInfo.m_id, m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      delete tagCol;
      return -1;
    }

    tagCol = openTagCol(serialNo, oldInfo);
    if (tagCol == nullptr) {
      LOG_WARN("failed to open the tag file(%u) of the tag table %s%s%s, undo failed.",
               oldInfo.m_id, m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }
    tagCol->unsetDrop();

    vector<TagColumn*>::iterator it = m_cols_.begin();
    while (serialNo-- > 0) {
      ++it;
    }
    m_cols_.insert(it, tagCol);
    updateOffsetAndSize();

    string priOldFileName;
    string secOldFileName;
    buildFileName(oldInfo, true, priOldFileName, secOldFileName);
    ::setDrop(priOldFileName, TTFT_TAG, true);
    if (!secOldFileName.empty()) {
      ::setDrop(secOldFileName, TTFT_STR, true);
    }

    string oldMetaFileName;
    buildMetaName(true, oldMetaFileName);
    ::setDrop(oldMetaFileName, TTFT_META, true);

    return 0;
  }

  return -1;
}

void CleanTagFiles(const string dirPath, uint64_t tableId, uint64_t cutoffLsn) {
  DIR* dirDes = opendir(dirPath.c_str());
  if (dirDes == nullptr) {
    LOG_WARN("failed to open %s, errno: %d, CleanTagFiles failed",
      dirPath.c_str(), errno);
    return;
  }

  string fileNamePrefix(to_string(tableId) + ".tag.");
  string fileNameSuffix(".bak");
  struct dirent* dirEntry = nullptr;
  while ((dirEntry = readdir(dirDes)) != nullptr) {
    if (dirEntry->d_type != DT_REG) {
      continue;
    }

    const string dirEntryName(dirEntry->d_name);
    if (dirEntryName.substr(0, fileNamePrefix.size()) != fileNamePrefix) {
      continue;
    }

    string fileNameStem;
    const string dirEntryNameSuffix
	= dirEntryName.substr(dirEntryName.find_last_of('.'));
    if (dirEntryNameSuffix == fileNameSuffix) {
      fileNameStem = dirEntryName.substr(fileNamePrefix.size(),
        dirEntryName.size() - fileNamePrefix.size() - fileNameSuffix.size());
    } else {
      fileNameStem = dirEntryName.substr(fileNamePrefix.size(),
        dirEntryName.size() - fileNamePrefix.size());
    }

    size_t dotPos = fileNameStem.find_last_of('.');
    if (dotPos != string::npos) {
      fileNameStem = fileNameStem.substr(dotPos + 1);
    }

    TagTableFileType fileType;
    if (fileNameStem == "ht") {
      fileType = TTFT_INDEX;
    } else if (fileNameStem == "header") {
      fileType = TTFT_HEADER;
    } else if (fileNameStem == "pt") {
      fileType = TTFT_PRIMARYTAG;
    } else if (fileNameStem == "mt") {
      fileType = TTFT_META;
    } else if (fileNameStem == "s") {
      fileType = TTFT_STR;
    } else if (fileNameStem == "hps") {
      fileType = TTFT_HASHPOINT;
    } else {
      fileType = TTFT_TAG;
    }

    string filePath = dirPath + dirEntryName;
    uint64_t lsn;
    bool willDrop;
    int rc = getLsnAndDrop(filePath, fileType, lsn, willDrop);
    if (rc < 0) {
      LOG_WARN("can't resolve the drop mark of %s, clean the tag file failed",
	    filePath.c_str());
      continue;
    }

    if (willDrop && (lsn < cutoffLsn)) {
      rc = remove(filePath.c_str());
      LOG_INFO("CleanTagFiles remove the file %s", filePath.c_str());
      if (rc < 0) {
        LOG_WARN("failed to remove the file %s, please remove it manually", filePath.c_str());
      }
    }
  }

  closedir(dirDes);
  return;
}

constexpr size_t TagTuplePack::versionOffset_;
constexpr size_t TagTuplePack::txnOffset_;
constexpr size_t TagTuplePack::dbIdOffset_;
constexpr size_t TagTuplePack::tabIdOffset_;
constexpr size_t TagTuplePack::entityIdOffset_;
constexpr size_t TagTuplePack::subgroupIdOffset_;
constexpr size_t TagTuplePack::flagOffset_;
constexpr size_t TagTuplePack::dataOffset_;

void TagTuplePack::calcDataMaxSizeAndLens() {
  dataMaxSize_ = dataOffset_;
  for (int i = 0; i < schema_.size(); i++) {
    if (schema_[i].m_tag_type == PRIMARY_TAG) {
      // the defined length, even if the actual length is short.
      priTagLen_ += schema_[i].m_length;
      if (flag_ == TTPFLAG_ALL) {
        fixTagLen_ += schema_[i].m_length;
      }
    } else if (schema_[i].m_tag_type == GENERAL_TAG) {
      if (flag_ == TTPFLAG_ALL) {
        if (schema_[i].m_data_type == DATATYPE::VARSTRING ||
	    schema_[i].m_data_type == DATATYPE::VARBINARY) {
	  // fixed part, offset: is from tag beginning, the first byte after
	  // tag length.
	  fixTagLen_ += 8;
	  varTagLen_ += 2 + schema_[i].m_length; // length+data
	} else {
	  fixTagLen_ += schema_[i].m_length;
	}
      }
    }
  }

  dataMaxSize_ += 2; // primary tag Len: don't contain the 2bytes length.
  dataMaxSize_ += priTagLen_;
  if (flag_ == TTPFLAG_ALL) {
    // bitMap contains the bits for primary tags.
    bitMapLen_ = (schema_.size() + 7) / 8;
    dataMaxSize_ += bitMapLen_;
    dataMaxSize_ += fixTagLen_ + varTagLen_;
    dataMaxSize_ += 4; // tagLen: don't contain the 4bytes length.
  }

  return;
}

void TagTuplePack::setVersion(uint32_t id) {
  if (isMemOwner_ && data_ != nullptr) {
    *reinterpret_cast<uint32_t *>(data_ + versionOffset_) = id;
  }
}

uint32_t TagTuplePack::getVersion() {
  return *reinterpret_cast<uint32_t *>(data_ + versionOffset_);
}

void TagTuplePack::setDBId(uint32_t id) {
  if (isMemOwner_ && data_ != nullptr) {
    *reinterpret_cast<uint32_t *>(data_ + dbIdOffset_) = id;
  }
}

uint32_t TagTuplePack::getDBId() {
  return *reinterpret_cast<uint32_t *>(data_ + dbIdOffset_);
}

void TagTuplePack::setTabId(uint64_t id) {
  if (isMemOwner_ && data_ != nullptr) {
    *reinterpret_cast<uint64_t *>(data_ + tabIdOffset_) = id;
  }
}

uint64_t TagTuplePack::getTabId() {
  return *reinterpret_cast<uint64_t *>(data_ + tabIdOffset_);
}

void TagTuplePack::setEntityId(uint32_t id) {
  if (isMemOwner_ && data_ != nullptr) {
    *reinterpret_cast<uint32_t *>(data_ + entityIdOffset_) = id;
  }
}

uint32_t TagTuplePack::getEntityId() {
  return *reinterpret_cast<uint32_t *>(data_ + entityIdOffset_);
}

void TagTuplePack::setSubgroupId(uint32_t id) {
  if (isMemOwner_ && data_ != nullptr) {
    *reinterpret_cast<uint32_t *>(data_ + subgroupIdOffset_) = id;
  }
}

uint32_t TagTuplePack::getSubgroupId() {
  return *reinterpret_cast<uint32_t *>(data_ + subgroupIdOffset_);
}

void TagTuplePack::initFlag() {
  flag_ = static_cast<TTPFlag>(*reinterpret_cast<short *>(data_ + flagOffset_));
}

void TagTuplePack::setFlag() {
  *reinterpret_cast<short *>(data_ + flagOffset_) = static_cast<short>(flag_);
}

void TagTuplePack::setBitMap() {
  int bytePos = curTag_ / 8;
  int bitPos = curTag_ % 8;

  uint8_t *ptr = reinterpret_cast<uint8_t *>(data_ + curBitMapOffset_);
  ptr[bytePos] = ptr[bytePos] | static_cast<uint8_t>(1 << bitPos);
}

int TagTuplePack::fillData(const char *val, size_t len) {
  if (! isMemOwner_) {
    return -1;
  }

  if (curTag_ == 0) {
    if (data_ == nullptr) {
      data_ = new char[dataMaxSize_];
      if (data_ == nullptr) {
	return -1;
      }
    }
    memset(data_, 0, dataMaxSize_);
    setFlag();

    dataLen_ = 0;
    curPriTagOffset_ = dataOffset_ + 2;
    curBitMapOffset_ = curPriTagOffset_ + priTagLen_ + 4;
    curTagOffset_ = curBitMapOffset_ + bitMapLen_;
    curVarOffset_ = curTagOffset_ + fixTagLen_;
  }

  while (curTag_ < schema_.size()) {
    if ((flag_ == TTPFLAG_PRITAG) &&
	(schema_[curTag_].m_tag_type != PRIMARY_TAG)) {
      ++curTag_; // skip general tags
    } else {
      break;
    }
  }

  if (schema_[curTag_].m_tag_type == PRIMARY_TAG) {
    fillParimary(val, len, schema_[curTag_].m_length);
    ++curTag_;
  } else if (schema_[curTag_].m_tag_type == GENERAL_TAG) {
    if ((schema_[curTag_].m_data_type == DATATYPE::VARSTRING) ||
	(schema_[curTag_].m_data_type == DATATYPE::VARBINARY)) {
      fillTag(val, len, schema_[curTag_].m_length, true);
    } else {
      fillTag(val, len, schema_[curTag_].m_length, false);
    }
    ++curTag_;
  }

  if (curTag_ == schema_.size()) {
    curTag_ = 0;
    *reinterpret_cast<uint16_t *>(data_ + dataOffset_) =
      static_cast<uint16_t>(priTagLen_);
    if (flag_ == TTPFLAG_PRITAG) {
      dataLen_ = curPriTagOffset_;
    } else if (flag_ == TTPFLAG_ALL) {
      *reinterpret_cast<uint32_t *>(data_ + curPriTagOffset_) =
	static_cast<uint32_t>(curVarOffset_ - curPriTagOffset_ - 4);
      dataLen_ = curVarOffset_;
    }
  }

  return 0;
}

void TagTuplePack::fillParimary(const char *val, size_t len, size_t defLen) {
  size_t copyLen = (len < defLen) ? len : defLen;
  memcpy(data_ + curPriTagOffset_, val, copyLen);
  curPriTagOffset_ += defLen;

  if (flag_ == TTPFLAG_ALL) {
    memcpy(data_ + curTagOffset_, val, copyLen);
    curTagOffset_ += defLen;
  }

  return;
}

void TagTuplePack::fillTag(const char *val, size_t len, size_t defLen,
			    bool isVar) {
    size_t copyLen = (len < defLen) ? len : defLen;
    if (isVar) {
      if (val != nullptr) {
	// offset is from tag
	uint64_t offset = curVarOffset_ - curBitMapOffset_;
	memcpy(data_ + curTagOffset_, &offset, 8);
	curTagOffset_ += 8;

	uint16_t len0 = (uint16_t)copyLen;
	memcpy(data_ + curVarOffset_, &len0, 2);
	curVarOffset_ += 2;

	memcpy(data_ + curVarOffset_, val, copyLen);
	curVarOffset_ += copyLen;
      } else {
	// don't set the offset value.
	curTagOffset_ += 8;
	setBitMap();
      }
    } else {
      if (val != nullptr) {
	memcpy(data_ + curTagOffset_, val, copyLen);
	curTagOffset_ += defLen;
      } else {
	// Fixed-length data occupies storage space even if it is null
	curTagOffset_ += defLen;
	setBitMap();
      }
    }

    return;
}

const TSSlice TagTuplePack::getData() {
  return TSSlice{data_, dataLen_};
}

const TSSlice TagTuplePack::getPrimaryTags() {
  size_t len = *reinterpret_cast<short *>(data_ + dataOffset_);
  return TSSlice{data_ + dataOffset_ + 2, len};
}

const TSSlice TagTuplePack::getTags() {
  if (flag_ == TTPFLAG_ALL) {
    size_t priLen = *reinterpret_cast<short *>(data_ + dataOffset_);

    size_t tagOffset = dataOffset_ + 2 + priLen;
    size_t tagLen = *reinterpret_cast<uint32_t *>(data_ + tagOffset);
    return TSSlice{data_ + tagOffset + 4, tagLen};
  } else {
    return TSSlice{nullptr, 0};
  }
}

TagTuplePack::TagTuplePack(TagTuplePack &&tag) noexcept
    : schema_(tag.schema_),
      data_(tag.data_),
      dataLen_(tag.dataLen_),
      dataMaxSize_(tag.dataMaxSize_),
      flag_(tag.flag_),
      isMemOwner_(tag.isMemOwner_),
      priTagLen_(tag.priTagLen_),
      bitMapLen_(tag.bitMapLen_),
      fixTagLen_(tag.fixTagLen_),
      varTagLen_(tag.varTagLen_) {
  tag.data_ = nullptr;
}

TagTuplePack& TagTuplePack::operator=(TagTuplePack &&tag) noexcept {
  if (this != &tag) {
    free();
    schema_ = tag.schema_;
    data_ = tag.data_;
    dataLen_ = tag.dataLen_;
    dataMaxSize_ = tag.dataMaxSize_;
    flag_ = tag.flag_;
    isMemOwner_ = tag.isMemOwner_;
    priTagLen_ = tag.priTagLen_;
    bitMapLen_ = tag.bitMapLen_;
    fixTagLen_ = tag.fixTagLen_;
    varTagLen_ = tag.varTagLen_;

    tag.data_ = nullptr;
  }

  return *this;
}

void TagTuplePack::free() {
  if (isMemOwner_ && (data_ != nullptr)) {
    delete[] data_;
  }
}
