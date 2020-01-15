#include <sstream>

#include "page.h"
#include "pfm.h"
#include "rbfm.h"

Page::Page(PID page_id) : pid(page_id) {}

Page::~Page() {
  if (data) freeMem();
}

void Page::load(FileHandle &handle) {
  if (!data) data = (char *) malloc(PAGE_SIZE);
  handle.readPage(pid, data);
  parseMeta();
}

void Page::freeMem() {
  free(data);
  data = nullptr;
}

RID Page::insertData(const char *new_data, size_t size) {
  memcpy(data + data_end, new_data, size);

  SID sid = findNextSlotID();
  if (sid == records_offset.size()) {
    // new slot
    free_space -= sizeof(unsigned);
    records_offset.emplace_back(pid, data_end);
  } else {
    // use previous deleted slot
    records_offset[sid] = {pid, data_end};
  }

  data_end += size;
  free_space -= size;
  return {pid, sid};
}

void Page::readData(PageOffset offset, void *out) {
  RecordBasedFileManager::decodeRecord(out, data + offset);
}

void Page::dump(FileHandle &handle) {
  if (not data) {
    DB_WARNING << "Try to dump empty data";
    throw std::runtime_error("dump empty data");
  }
  // dump meta
  dumpMeta();

  handle.writePage(pid, data);
  freeMem();
}

void Page::parseMeta() {
  unsigned *pt = (unsigned *) (data + PAGE_SIZE) - 1;
  free_space = *pt--;
  unsigned num_slots = *pt--;
  int data_offset = 0;
  // scan record offsets from back to front
  for (int i = 0; i < num_slots; ++i) {
    records_offset.push_back(decodeDirectory(*pt--));
    if (records_offset.back().second != INVALID_OFFSET)
      data_offset += records_offset.back().second;
  }
  data_end = data_offset;
}

void Page::dumpMeta() {
  unsigned num_slots = records_offset.size();
  unsigned *pt = (unsigned *) (data + PAGE_SIZE) - 1;
  *pt-- = free_space;
  *pt-- = records_offset.size();
  // scan record offsets from back to front
  for (int i = 0; i < num_slots; ++i) {
    *pt-- = encodeDirectory(records_offset[i]);
  }
}

SID Page::findNextSlotID() const {
  for (int i = 0; i < records_offset.size(); ++i) {
    if (records_offset[i].second == INVALID_OFFSET) return i;
  }
  return records_offset.size();
}

std::string Page::ToString() const {
  std::ostringstream oss;
  oss << "Page: " << pid << " free space:" << free_space << "\n";
  for (int i = 0; i < records_offset.size(); ++i) {
    oss << "\tRecord " << i << " offset " << records_offset[i].first << "," << records_offset[i].second << "\n";
  }
  return oss.str();
}

//vector<pair<unsigned, unsigned>> Page::getRecordOffsets() const {
//  std::vector<std::pair<unsigned, unsigned >> record_offsets;
//  unsigned *pt = (unsigned *) (data + PAGE_SIZE) - 3;
//  for (int i = 0; i < num_slots; i++) {
//    auto parsed = decodeDirectory(*pt);
//    record_offsets.push_back(parsed);
//    --pt;
//  }
//  return record_offsets;
//}

void Page::initPage(char *page_data) {
  // assume page_data is PAGE_SIZE
  *((int *) (page_data + PAGE_SIZE) - 1) = PAGE_SIZE - 3 * sizeof(unsigned); // initial free_space
  *((int *) (page_data + PAGE_SIZE) - 2) = 0; // initial num_slots
}

void Page::writeNumSlotAndFreeSpace() {
  *((int *) (data + PAGE_SIZE) - 1) = free_space;
  *((int *) (data + PAGE_SIZE) - 2) = num_slots;
}


