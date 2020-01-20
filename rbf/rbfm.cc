#include <math.h>

#include "rbfm.h"

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
  auto data_to_be_inserted = serializeRecord(recordDescriptor, data);
  if (data_to_be_inserted.first != 0)
    return -1;  // varchar longer than upper limit
  size_t total_size = data_to_be_inserted.second.size();
//  DB_DEBUG << "TOTAL SIZE " << total_size;
  if (total_size > Page::MAX_SIZE) {
    DB_ERROR << "data size " << total_size << " larger than MAX_SIZE " << Page::MAX_SIZE;
    return -1;
  }

  Page *page = findAvailableSlot(total_size, fileHandle);
  page->load(fileHandle);
  rid = page->insertData(data_to_be_inserted.second.data(), data_to_be_inserted.second.size());
  page->dump(fileHandle);

//  free_slots_[page->free_space].insert(page);
  return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
  return readRecordImpl(fileHandle, recordDescriptor, rid, data);
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
  auto res = loadPageWithRid(rid, fileHandle);
  if (!res.first) {
    DB_WARNING << "deleteRecord failed, RID invalid";
    return -1;
  }

  Page *origin_page = res.second;

  auto &offset = origin_page->records_offset[rid.slotNum];
  if (offset.first == origin_page->pid) {
    // in origin page
    auto data_begin = offset.second;
    offset.second = Page::INVALID_OFFSET;
    origin_page->deleteRecord(data_begin);
  } else {
    // redirect to another page
    PID redirect_pid = offset.first;
    SID redirect_sid = offset.second;
    offset = {rid.pageNum, Page::INVALID_OFFSET};

    Page *redirect_page = pages_[redirect_pid].get();
    redirect_page->load(fileHandle);
    auto &redirect_offset = redirect_page->records_offset[redirect_sid];
    auto data_begin = redirect_offset.second;
    redirect_offset = {redirect_page->pid, Page::INVALID_OFFSET};
    redirect_page->deleteRecord(data_begin);
    redirect_page->dump(fileHandle);
  }

  origin_page->dump(fileHandle);

  return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
  auto ret = loadPageWithRid(rid, fileHandle);
  if (!ret.first) {
    DB_WARNING << "updateRecord failed, RID invalid";
    return -1;
  }

  Page *origin_page = ret.second;

  /*
   * serialize data just like insert
   */
  auto data_to_be_inserted = serializeRecord(recordDescriptor, data);
  if (data_to_be_inserted.first != 0) {
    origin_page->freeMem();
    return -1;  // varchar longer than upper limit
  }
  size_t new_size = data_to_be_inserted.second.size();
//  DB_DEBUG << "updateRecord TOTAL SIZE " << total_size;
  if (new_size > Page::MAX_SIZE) {
    origin_page->freeMem();
    DB_ERROR << "data size " << new_size << " larger than MAX_SIZE " << Page::MAX_SIZE;
    return -1;
  }

  /*
   * update record
   */
  auto &origin_offset = origin_page->records_offset[rid.slotNum];
  Page *cur_page = origin_page;
  auto &cur_offset = origin_offset;
  if (origin_offset.first != origin_page->pid) {
    // redirected to another page
    cur_page = pages_[origin_offset.first].get();
    cur_page->load(fileHandle);
    cur_offset = cur_page->records_offset[origin_offset.second];
  }

  size_t old_size = Page::getRecordSize(cur_page->data + cur_offset.second);
  // here we should compare real_free_space_, since we don't need to allocate another slot directory
  if (new_size > old_size && (new_size - old_size) > cur_page->real_free_space_) {
    // become too large that current page can not fit

    // 1. delete from cur_page
    auto cur_data_begin = cur_offset.second;
    cur_offset = {cur_page->pid, Page::INVALID_OFFSET};
    cur_page->deleteRecord(cur_data_begin);

    // 2. insert into new_page
    Page *new_page = findAvailableSlot(new_size, fileHandle);
    if (new_page != origin_page) new_page->load(fileHandle);
    /*
     * be careful! might redirected back to origin page, which means new_page == origin_page,
     * in that case we should re-use old directory and sid instead of create a new one
     */

    if (new_page == origin_page) {
      SID origin_sid = rid.slotNum;
      new_page->insertData(data_to_be_inserted.second.data(), new_size, origin_sid);
      // new_page->records_offset[origin_sid] will be updated accordingly
    } else {
      RID new_rid = new_page->insertData(data_to_be_inserted.second.data(), new_size);
      new_page->records_offset[new_rid.slotNum].first = Page::FORWARDED_SLOT;
      origin_page->records_offset[rid.slotNum] = {new_rid.pageNum, new_rid.slotNum};
    }
    if (new_page != origin_page) new_page->dump(fileHandle);
  } else {
    // shift backward/forward inside cur_page
    size_t shift_offset = std::abs(int(old_size) - int(new_size));
    cur_page->shiftAfterRecords(cur_offset.second, shift_offset, new_size > old_size);
    memcpy(cur_page->data + cur_offset.second, data_to_be_inserted.second.data(), new_size);
  }

  origin_page->dump(fileHandle);
  if (cur_page != origin_page) cur_page->dump(fileHandle);

  return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {

  auto it = std::find(recordDescriptor.begin(), recordDescriptor.end(), attributeName);
  if (it == recordDescriptor.end()) {
    DB_WARNING << "No such attribute: " << attributeName;
    return -1;
  }
  int field_idx = it - recordDescriptor.begin();

  return readRecordImpl(fileHandle, recordDescriptor, rid, data, field_idx);
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
  return -1;
}

/**************************************
 *
 * ========= Utility functions ==========
 *
 *************************************/


RC RecordBasedFileManager::readRecordImpl(FileHandle &fileHandle,
                                          const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid,
                                          void *data,
                                          int field_idx) {
  auto ret = loadPageWithRid(rid, fileHandle);
  if (!ret.first) {
    DB_WARNING << "readRecord failed, rid not exist";
    return -1;
  }

  Page *origin_page = ret.second;

  auto &record_offset = origin_page->records_offset[rid.slotNum];
  if (record_offset.first == origin_page->pid) {
    // in the same page: directly read the record starting at PageOffset
    origin_page->readData(record_offset.second, data, recordDescriptor, field_idx);
  } else if (record_offset.first == Page::FORWARDED_SLOT) {
    DB_ERROR << "Illegal direct access to a forwarded slot: " << rid.pageNum << " " << rid.slotNum;
    return -1;
  } else {
    // redirect to another page: the PageOffset entry actually stores the RID at the exact page
    Page *redirect_page = pages_[record_offset.first].get();
    redirect_page->load(fileHandle);
    // if redirected, we use offset to indicate SID in the redirected page
    PageOffset real_offset = redirect_page->records_offset[record_offset.second].second;
    redirect_page->readData(real_offset, data, recordDescriptor, field_idx);
    redirect_page->freeMem();
  }
  origin_page->freeMem();

  return 0;
}

void RecordBasedFileManager::loadNextPage(FileHandle &fileHandle) {
  // construct a Page object, read page to buffer, parse meta in corresponding data to initialize in-memory variables,
  //   then free buffer up
  std::shared_ptr<Page> cur_page = std::make_shared<Page>(pages_.size());
  cur_page->load(fileHandle); // load and parse
  cur_page->freeMem();
//  free_slots_[cur_page->free_space].insert(cur_page.get());
  pages_.push_back(cur_page);
}

void RecordBasedFileManager::appendNewPage(FileHandle &file_handle) {
  if (pages_.size() == Page::FORWARDED_SLOT) {
    DB_ERROR << "Exceed max page num " << Page::FORWARDED_SLOT;
    throw std::runtime_error("exceed max page num");
  }
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

std::pair<RC, std::vector<char>>
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
        if (char_len > recordDescriptor[i].length) {
          // varchar longer than upper limit
          return {-1, std::vector<char>()};
        }
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
  size_t directory_size = sizeof(directory_t) * directories.size();
  size_t real_data_size = offset - directory_size;

  memcpy(decoded_data.data(), directories.data(), directory_size);
  memcpy(decoded_data.data() + directory_size, real_data, real_data_size);

  return {0, decoded_data};
}

RC RecordBasedFileManager::deserializeRecord(const std::vector<Attribute> &recordDescriptor,
                                             void *out,
                                             const char *src,
                                             int field_idx) {

  // 1. make null indicator
  int indicator_bytes_num = int(ceil(double(recordDescriptor.size()) / 8));
  unsigned char indicator_bytes[indicator_bytes_num];
  memset(indicator_bytes, 0, indicator_bytes_num);
  unsigned char *pt = indicator_bytes;
  directory_t *dir_pt = (directory_t *) src;
  directory_t field_num = *dir_pt++;
  if (field_num != recordDescriptor.size()) {
    DB_ERROR << "field num not matched. " << recordDescriptor.size() << " given in recordDescriptor, " << field_num
             << " found in data";
    return -1;
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

  char *out_pt = (char *) out;
  if (field_idx == ALL_FIELD) {
    memcpy(out, indicator_bytes, indicator_bytes_num);
    out_pt += indicator_bytes_num;
  }


  // 2. write data
  size_t directory_size = entryDirectoryOverheadLength(field_num);
  if (field_idx == ALL_FIELD) {
    /*
     * read all field
     */
    src += directory_size; // begin of real data
    size_t prev_offset = directory_size;
    for (int o : fields_offset) {
      if (o == -1) continue; // null
      unsigned field_size = o - prev_offset;
      // here we don't need to handle varchar as special case,
      // since we already store that int for varchar len
      memcpy(out_pt, src, field_size);
      src += field_size;
      out_pt += field_size;
      prev_offset = o;
    }
  } else {
    /*
     * read field specified by field_idx
     */
    // TODO: not sure how to handle NULL field
    if (fields_offset[field_idx] == -1) {
      return 0;
    }
    size_t field_start = directory_size; // if all previous fields are null then it should be derectory_size
    for (int i = field_idx - 1; i >= 0; --i)
      if (fields_offset[i] != -1) field_start = fields_offset[i];

    size_t field_size = fields_offset[field_idx] - field_start;
    DB_DEBUG << "Read attribute with size " << field_size;
    memcpy(out_pt, src + field_start, field_size);
  }

  return 0;
}

Page *RecordBasedFileManager::findAvailableSlot(size_t size, FileHandle &file_handle) {
  // find the first available free slot to insert data
  // will also handle creating new page / new slot when there's no available one
  for (auto p : pages_) {
    if (p->free_space >= size + sizeof(unsigned)) return p.get();
  }
  appendNewPage(file_handle);
  return pages_.back().get();
}

std::pair<bool, Page *> RecordBasedFileManager::loadPageWithRid(const RID &rid, FileHandle &file_handle) {
  if (rid.pageNum >= pages_.size()) {
    DB_WARNING << "RID invalid, page num " << rid.pageNum << " no exist";
    return {false, nullptr};
  }
  Page *origin_page = pages_[rid.pageNum].get();
  origin_page->load(file_handle);

  if (rid.slotNum > origin_page->records_offset.size()
      || origin_page->records_offset[rid.slotNum].second == Page::INVALID_OFFSET) {
    DB_WARNING << "RID invalid, slot num " << rid.slotNum << " no exist";
    origin_page->freeMem();
    return {false, nullptr};
  }
  return {true, origin_page};
}

/**************************************
 *
 * ========= PAGE CLASS ==========
 *
 *************************************/

Page::Page(PID page_id) : pid(page_id), data(nullptr), data_end(0) {}

Page::~Page() {
  if (data) freeMem();
}

void Page::load(FileHandle &handle) {
  if (!data) data = (char *) malloc(PAGE_SIZE);
  handle.readPage(pid, data);
  parseMeta();
  maintainFreeSpace();
//  DB_DEBUG << "page after load" << ToString();
}

void Page::freeMem() {
  free(data);
  data = nullptr;
}

RID Page::insertData(const char *new_data, size_t size, SID sid) {
  if (sid == FIND_NEW_SID) sid = findNextSlotID();
  if (sid == records_offset.size()) {
    // new slot
    records_offset.emplace_back(pid, data_end);
    real_free_space_ -= sizeof(int);
  } else {
    // use previous deleted slot
    records_offset[sid] = {pid, data_end};
  }
  memcpy(data + data_end, new_data, size);

  data_end += size;
  real_free_space_ -= size;
  maintainFreeSpace();
  checkDataend();
//  DB_ERROR << data_end;
  return {pid, sid};
}

RC Page::deleteRecord(size_t record_begin_offset) {
  size_t record_size = getRecordSize(data + record_begin_offset);
  shiftAfterRecords(record_begin_offset, record_size, false);
//  real_free_space_ -= record_size;
  maintainFreeSpace();
  return 0;
}

void Page::readData(PageOffset record_offset,
                    void *out,
                    const std::vector<Attribute> &recordDescriptor,
                    int field_idx) {
//  DB_DEBUG << print_bytes(data,40);
  RecordBasedFileManager::deserializeRecord(recordDescriptor, out, data + record_offset);
}

void Page::dump(FileHandle &handle) {
  if (not data) {
    throw std::runtime_error("dump empty data");
  }
//  DB_DEBUG << "page before dump:" << ToString();
  // dump meta
  dumpMeta();

  handle.writePage(pid, data);
  freeMem();
}

void Page::parseMeta() {

  /*
   * remember to reset in-memory data
   */
  records_offset.clear();
  invalid_slots_.clear();

  unsigned *pt = (unsigned *) (data + PAGE_SIZE) - 1;
  real_free_space_ = *pt--;
  unsigned num_slots = *pt--;

  // scan record offsets from back to front
  for (int i = 0; i < num_slots; ++i) {
    records_offset.push_back(decodeDirectory(*pt--));
    if (records_offset.back().second != INVALID_OFFSET) {
    } else {
      // invalid(deleted) entry
      invalid_slots_.insert(i);
    }
  }
  data_end = PAGE_SIZE - (num_slots + 2) * sizeof(unsigned) - real_free_space_;
}

void Page::dumpMeta() {
  unsigned num_slots = records_offset.size();
  unsigned *pt = (unsigned *) (data + PAGE_SIZE) - 1;
  *pt-- = real_free_space_;
  *pt-- = records_offset.size();
  // scan record offsets from back to front
  for (int i = 0; i < num_slots; ++i) {
    *pt-- = encodeDirectory(records_offset[i]);
  }
}

//std::string Page::ToString() const {
//  std::ostringstream oss;
//  oss << "Page: " << pid << " free space:" << free_space << " real free space " << real_free_space_ << "\n";
//  for (int i = 0; i < records_offset.size(); ++i) {
//    oss << "\tRecord " << i << " offset " << records_offset[i].first << "," << records_offset[i].second << "\n";
//  }
//  return oss.str();
//}

void Page::initPage(char *page_data) {
  // assume page_data is PAGE_SIZE
  // reserve 2 ints, one for freespace, one for num_slots
  *((int *) (page_data + PAGE_SIZE) - 1) = PAGE_SIZE - 2 * sizeof(unsigned);
  *((int *) (page_data + PAGE_SIZE) - 2) = 0; // initial num_slots
}

RC Page::shiftAfterRecords(size_t record_begin_offset, size_t shift_size, bool forward) {
  // function that shifts the records after the record beginning at record_begin_offset
  // since records are continuous, we only need to find the start and size of the chunk

  // get the START of the chunk of records to be moved
  size_t chunk_start = PAGE_SIZE;
  for (auto &offset: records_offset) {
    if (offset.first != pid) continue;           // forwarding to another slot: no need to shift
    if (offset.second <= record_begin_offset || offset.second == INVALID_OFFSET)
      continue;  // locates before it or deleted
    chunk_start = std::min(chunk_start, size_t(offset.second));
    if (forward) offset.second += shift_size;
    else offset.second -= shift_size;
  }
  size_t chunk_size = data_end - chunk_start;
  if (forward) {
    memcpy(data + chunk_start + shift_size, data + chunk_start, chunk_size);
    memset(data + chunk_start, 0, shift_size);
    real_free_space_ -= shift_size;
    DB_DEBUG << "Page " << pid << " move data chunk start from " << chunk_start << "(size " << chunk_size
             << ") forward " << shift_size << " bytes";
  } else {
    memcpy(data + chunk_start - shift_size, data + chunk_start, chunk_size);
    if (shift_size > chunk_size)
      memset(data + chunk_start - shift_size + chunk_size,
             0,
             shift_size); // in case there could be some non-zeros after shifting backward
    real_free_space_ += shift_size;
    DB_DEBUG << "Page " << pid << " move data chunk start from " << chunk_start << "(size " << chunk_size << ") back "
             << shift_size << " bytes";
  }
  maintainFreeSpace();
  return 0;
}

size_t Page::getRecordSize(const char *begin) {

  directory_t *dir_pt = (directory_t *) (begin);
  directory_t field_num = *dir_pt++;

  int last_field_end = 0;
  for (int i = 0; i < field_num; ++i) {
    // if null, will be -1
    last_field_end = std::max(last_field_end, int(*dir_pt++));
  }
  return last_field_end;
}

void Page::checkDataend() {
  size_t last_record_begin = 0;
  for (auto &record:records_offset) {
    if (record.second != INVALID_OFFSET)
      last_record_begin = std::max(last_record_begin, std::size_t(record.second));
  }

  size_t last_record_size = getRecordSize(data + last_record_begin);
  if (last_record_size + last_record_begin != data_end)
    throw std::runtime_error(
        "data end not match " + std::to_string(last_record_size + last_record_begin) + ":" + std::to_string(data_end));

}



