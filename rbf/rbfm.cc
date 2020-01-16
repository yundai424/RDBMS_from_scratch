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
  for (auto page : pages_) {
    page->dump(fileHandle);
  }
  return pfm_->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
  // use the array of field offsets method for variable length record introduced in class as the format of record
  // each record has a leading series of bytes indicating the pointers to each field
  auto data_to_be_inserted = deserializeRecord(recordDescriptor, data);
  size_t total_size = data_to_be_inserted.size();
  const static size_t MAX_SIZE = PAGE_SIZE - sizeof(unsigned) * 2;
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
  // TODO : seems that recordDescriptor has nothing to do here
  Page *p = pages_[rid.pageNum].get();
  p->load(fileHandle);
  auto &record_offset = p->records_offset[rid.slotNum];
  if (record_offset.first == p->pid) {
    // in same page
    p->readData(record_offset.second, data);
  } else {
    // redirect to another page
    Page *redirect_p = pages_[record_offset.first].get();
    redirect_p->load(fileHandle);
    redirect_p->readData(record_offset.second, data);
    redirect_p->freeMem();
  }
  p->freeMem();

  return 0;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
  return -1;
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

std::vector<char>
RecordBasedFileManager::deserializeRecord(const std::vector<Attribute> &recordDescriptor,
                                          const void *data) {
  // parse null indicators
  std::vector<bool> null_indicators;
  int fields_num = recordDescriptor.size();
  int indicator_bytes_num = int(ceil(double(fields_num) / 8));
  unsigned char indicator_bytes[indicator_bytes_num];
  unsigned char *pt = indicator_bytes;
  memcpy(indicator_bytes, data, indicator_bytes_num);

  for (auto i = 0; i < fields_num; ++i) {
    unsigned char mask = 1;
    mask = mask << (i % 8);
    null_indicators.push_back(mask & (*pt));
    if (i % 8 == 7) pt++;
  }

  // the real data position
  const char *real_data = ((char *) data) + indicator_bytes_num;

  // create directories: "array of field offsets"
  // the first element in the array indicates the number of fields in this record
  // then the following elements indicate the END of records instead of head
  std::vector<directory_t> directories;
  directory_t offset = sizeof(directory_t) * fields_num; // offset from the head of encoded record
  int raw_offset = indicator_bytes_num;
  for (int i = 0; i < fields_num; ++i) {
    // when a field is NULL, the directory just points to the end of the previous record,
    //    which means this field has 0 length -> a NULL field
    if (!null_indicators[i]) {
      if (recordDescriptor[i].type == TypeVarChar) {
        int char_len;
        memcpy(&char_len, (unsigned char *) data + raw_offset, sizeof(int));
        offset += char_len;
        raw_offset += char_len;
      } else {
        offset += recordDescriptor[i].length;
        raw_offset += recordDescriptor[i].length;
      }
    }
    directories.push_back(offset);
  }
  std::vector<char> decoded_data(offset, 0);
  size_t real_data_size = offset - sizeof(directory_t) * fields_num;
  memcpy(decoded_data.data(), directories.data(), sizeof(directory_t) * directories.size());
  memcpy(decoded_data.data() + sizeof(directory_t) * directories.size(), real_data, real_data_size);

  return decoded_data;
}

void RecordBasedFileManager::serializeRecord(void *out, const char *src) {
  directory_t *pt = (directory_t *) out;
  directory_t field_num = *pt++;
  size_t size = 0;
  // notice that some field might be empty
  for (unsigned i = 0; i < field_num; ++i) {
    size = std::max(size, size_t(*pt++));
  }
  memcpy(out, src, size);
}

Page *RecordBasedFileManager::findAvailableSlot(size_t size, FileHandle &file_handle) {
  // find the first available free slot to insert data
  // will also handle creating new page / new slot when there's no available one
  auto it = free_slots_.lower_bound(size); // greater or equal
  if (it == free_slots_.end()) {
    appendNewPage(file_handle);
    it = free_slots_.lower_bound(size);
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



