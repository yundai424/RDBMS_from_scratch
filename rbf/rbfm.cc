#include <math.h>

#include "rbfm.h"
#include "logger.h"

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;

RecordBasedFileManager &RecordBasedFileManager::instance() {
  static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
  return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() : pfm_(&PagedFileManager::instance()) {}

RecordBasedFileManager::~RecordBasedFileManager() { delete _rbf_manager; }

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {
  return pfm_->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
  return pfm_->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
  RC ret = pfm_->openFile(fileName, fileHandle);
  if (!ret) {
    // init the RBFM
    PID page_num = fileHandle.getNumberOfPages();
    for (PID i = 0; i < page_num; ++i) {
      loadNextPage(fileHandle);
    }
  }
  return ret;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
  // should not dump pages here, because we will dump the page after each operation

//  for (auto page : pages_) {
//    page->dump(fileHandle);
//  }
  return pfm_->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
  // use the array of field offsets method for variable length record introduced in class as the format of record
  // each record has a leading series of bytes indicating the pointers to each field
  if (!total_num_fields_) total_num_fields_ = recordDescriptor.size();
  auto data_to_be_inserted = serializeRecord(recordDescriptor, data);
  size_t total_size = data_to_be_inserted.size();
  const static size_t MAX_SIZE = PAGE_SIZE - sizeof(unsigned) * 3;
  if (total_size > MAX_SIZE) {
    DB_ERROR << "data size " << total_size << " larger than MAX_SIZE " << MAX_SIZE;
    return -1;
  }

  Page *page = findAvailableSlot(total_size, fileHandle);
  page->load(fileHandle);
  rid = page->insertData(data_to_be_inserted.data(), data_to_be_inserted.size());
  page->dump(fileHandle);

  free_slots_[page->free_space].insert(page);

  return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
  if (rid.pageNum >= fileHandle.getNumberOfPages()) {
    DB_DEBUG << "RID NOT EXIST";
    return -1;
  }

  Page *p = pages_[rid.pageNum].get();
  p->load(fileHandle);
  if (rid.slotNum >= p->records_offset.size()) {
    DB_DEBUG << "RID NOT EXIST";
    p->freeMem();
    return -1;
  }

  auto &record_offset = p->records_offset[rid.slotNum];
  if (record_offset.first == p->pid) {
    // in same page
    p->readData(record_offset.second, data, recordDescriptor);
  } else {
    // redirect to another page
    Page *redirect_p = pages_[record_offset.first].get();
    redirect_p->load(fileHandle);
    redirect_p->readData(record_offset.second, data, recordDescriptor);
    redirect_p->freeMem();
  }
  p->freeMem();

  return 0;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
  // parse null indicators
  int fields_num = recordDescriptor.size();
  int indicator_bytes_num = int(ceil(double(fields_num) / 8));
  std::vector<bool> null_indicators = parseNullIndicator((const unsigned char *) data, recordDescriptor.size());

  // the real data position
  const char *real_data = ((char *) data) + indicator_bytes_num;

  for (int i = 0; i < recordDescriptor.size(); ++i) {
    std::cout << recordDescriptor[i].name << ": ";
    if (null_indicators[i]) std::cout << "NULL";
    else {
      if (recordDescriptor[i].type == AttrType::TypeInt) {
        int *tmp = (int *) real_data;
        real_data += sizeof(int);
        std::cout << *tmp;
      } else if (recordDescriptor[i].type == AttrType::TypeReal) {
        float *tmp = (float *) real_data;
        real_data += sizeof(float);
        std::cout << *tmp;
      } else {
        // VarChar
        int char_len = *((int *) (real_data));
        real_data += sizeof(int);
        char str[char_len + 1];
        for (int k = 0; k < char_len + 1; ++k) str[k] = 0;
        memcpy(str, real_data, char_len);
        real_data += char_len;
        std::cout << str;
      }
    }
    std::cout << " ";
  }
  std::cout << std::endl;
  return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
  return -1;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
  return -1;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {
  return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
  return -1;
}

/**
 * ========= Utility functions ==========
 */


void RecordBasedFileManager::loadNextPage(FileHandle &fileHandle) {
  // construct a Page object, read page to buffer, parse meta in corresponding data to initialize in-memory variables,
  //   then free buffer up
  std::shared_ptr<Page> cur_page = std::make_shared<Page>(pages_.size());
  cur_page->load(fileHandle); // load and parse
  cur_page->freeMem();
  free_slots_[cur_page->free_space].insert(cur_page.get());
  pages_.push_back(cur_page);
}

void RecordBasedFileManager::appendNewPage(FileHandle &file_handle) {
  char new_page[PAGE_SIZE];
  Page::initPage(new_page);
  file_handle.appendPage(new_page);
  loadNextPage(file_handle);
}

std::vector<bool> RecordBasedFileManager::parseNullIndicator(const unsigned char *data, unsigned fields_num) {
  std::vector<bool> null_indicators;
  int indicator_bytes_num = int(ceil(double(fields_num) / 8));
  unsigned char indicator_bytes[indicator_bytes_num];
  unsigned char *pt = indicator_bytes;
  memcpy(indicator_bytes, data, indicator_bytes_num);

  for (auto i = 0; i < fields_num; ++i) {
    unsigned char mask = 1 << (7 - (i % 8));
    null_indicators.push_back((mask & (*pt)) != 0);
    if (i % 8 == 7) pt++;
  }
  return null_indicators;
}

std::vector<char>
RecordBasedFileManager::serializeRecord(const std::vector<Attribute> &recordDescriptor,
                                        const void *data) {
  // parse null indicators
  int fields_num = recordDescriptor.size();
  int indicator_bytes_num = int(ceil(double(fields_num) / 8));
  std::vector<bool> null_indicators = parseNullIndicator((const unsigned char *) data, recordDescriptor.size());

  // the real data position
  const char *real_data = ((char *) data) + indicator_bytes_num;

  // create directories: "array of field offsets"
  // the first element in the array indicates the number of fields in this record
  // then the following elements indicate the END of records instead of head
  std::vector<directory_t> directories{directory_t(fields_num)};
  directory_t offset = entryDirectoryOverheadLength(fields_num); // offset from the head of encoded record
  int raw_offset = indicator_bytes_num;
  for (int i = 0; i < fields_num; ++i) {
    // when a field is NULL, the directory has value of -1
    if (!null_indicators[i]) {
      if (recordDescriptor[i].type == TypeVarChar) {
        // we also store the int which indicate varchar len
        int char_len = *((int *) ((char *) data + raw_offset));
        offset += char_len + sizeof(int);
        raw_offset += char_len + sizeof(int);
      } else {
        offset += recordDescriptor[i].length;
        raw_offset += recordDescriptor[i].length;
      }
      directories.push_back(offset);
    } else {
      directories.push_back(-1);
    }
  }
  std::vector<char> decoded_data(offset, 0);
  size_t real_data_size = offset - sizeof(directory_t) * directories.size();
  memcpy(decoded_data.data(), directories.data(), sizeof(directory_t) * directories.size());
  memcpy(decoded_data.data() + sizeof(directory_t) * directories.size(), real_data, real_data_size);

  return decoded_data;
}

void RecordBasedFileManager::deserializeRecord(const std::vector<Attribute> &recordDescriptor,
                                               void *out,
                                               const char *src) {

  // 1. make null indicator
  int indicator_bytes_num = int(ceil(double(recordDescriptor.size()) / 8));
  unsigned char indicator_bytes[indicator_bytes_num];
  memset(indicator_bytes, 0, indicator_bytes_num);
  unsigned char *pt = indicator_bytes;
  directory_t *dir_pt = (directory_t *) src;
  directory_t field_num = *dir_pt++;
  if (field_num != recordDescriptor.size()) {
    DB_ERROR << "field num not matched! fields_num in directory: " << field_num << " fields_num in descriptor: "
             << recordDescriptor.size();
    throw std::runtime_error("field num not matched");
  }
  std::vector<int> fields_offset;
  for (int i = 0; i < field_num; ++i) {
    fields_offset.push_back(*dir_pt++);
    if (fields_offset.back() == -1) {
      unsigned char mask = 1 << (7 - (i % 8));
      *pt = *pt | mask;
    }
    if (i % 8 == 7) ++pt;
  }

  memcpy(out, indicator_bytes, indicator_bytes_num);
  char *out_pt = (char *) out;
  out_pt += indicator_bytes_num;

  // 2. write data
  size_t directory_size = entryDirectoryOverheadLength(field_num);
  src += directory_size; // begin of real data
  size_t prev_offset = directory_size;
  for (int i = 0; i < fields_offset.size(); ++i) {
    if (fields_offset[i] == -1) continue; // null
    unsigned field_size = fields_offset[i] - prev_offset;
    // here we don't need to handle varchar as special case,
    // since we already store that int for varchar len
    memcpy(out_pt, src, field_size);
    src += field_size;
    out_pt += field_size;
    prev_offset = fields_offset[i];
  }

}

Page *RecordBasedFileManager::findAvailableSlot(size_t size, FileHandle &file_handle) {
  // find the first available free slot to insert data
  // will also handle creating new page / new slot when there's no available one
  auto it = free_slots_.lower_bound(size + sizeof(unsigned)); // greater or equal
  if (it == free_slots_.end()) {
    appendNewPage(file_handle);
    it = free_slots_.lower_bound(size + sizeof(unsigned));
  }
  auto &available_pages = it->second;
  Page *res = *available_pages.begin();
  available_pages.erase(res);
  if (available_pages.empty()) {
    free_slots_.erase(it);
  }
  return res;
}

// move the records behind the given slot ahead to fill the empty spaces
void RecordBasedFileManager::rearrange(FreeSlot &slot, size_t total_size) {

}



