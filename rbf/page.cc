#include <sstream>

#include "page.h"
#include "pfm.h"
#include "rbfm.h"

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

RID Page::insertData(const char *new_data, size_t size) {
  SID sid = findNextSlotID();
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

void Page::readData(PageOffset page_offset, void *out, const std::vector<Attribute> &recordDescriptor) {
//  DB_DEBUG << print_bytes(data,40);
  RecordBasedFileManager::deserializeRecord(recordDescriptor, out, data + page_offset);
}

void Page::dump(FileHandle &handle) {
  if (not data) {
    DB_WARNING << "Try to dump empty data";
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

std::string Page::ToString() const {
  std::ostringstream oss;
  oss << "Page: " << pid << " free space:" << free_space << " real free space " << real_free_space_ << "\n";
  for (int i = 0; i < records_offset.size(); ++i) {
    oss << "\tRecord " << i << " offset " << records_offset[i].first << "," << records_offset[i].second << "\n";
  }
  return oss.str();
}

void Page::initPage(char *page_data) {
  // assume page_data is PAGE_SIZE
  // reserve 2 ints, one for freespace, one for num_slots
  *((int *) (page_data + PAGE_SIZE) - 1) = PAGE_SIZE - 2 * sizeof(unsigned);
  *((int *) (page_data + PAGE_SIZE) - 2) = 0; // initial num_slots
}

void Page::checkDataend() {
  size_t last_record_begin = 0;
  for (auto &record:records_offset) {
    if (record.second != INVALID_OFFSET)
      last_record_begin = std::max(last_record_begin, std::size_t(record.second));
  }

  directory_t *dir_pt = (directory_t *) (data + last_record_begin);
  directory_t field_num = *dir_pt++;

  int last_field_end = 0;
  for (int i = 0; i < field_num; ++i) {
    last_field_end = std::max(last_field_end, int(*dir_pt++));
  }
  if (last_field_end + last_record_begin != data_end)
    throw std::runtime_error("data end not match " + std::to_string(last_field_end + last_record_begin) + ":" + std::to_string(data_end));

}
