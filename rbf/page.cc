#include <sstream>

#include "page.h"
#include "pfm.h"
#include "logger.h"

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

  SID sid = num_slots;
  num_slots++;
  free_space -= sizeof(unsigned);

  // TODO: update record offset at the end of file
  data_end += size;
  free_space -= size;
  return {pid, sid};
}

void Page::dump(FileHandle &handle) {
  if (not data) {
    DB_WARNING << "Try to dump empty data";
    throw std::runtime_error("dump empty data");
  }
  handle.writePage(pid, data);
  freeMem();
}

void Page::parseMeta() {
  unsigned *pt = (unsigned *) (data + PAGE_SIZE) - 1;
  num_slots = *pt--;
  free_space = *pt;
//  int data_offset = 0;
//  // scan record offsets from back to front
//  for (int i = 0; i < num_slots; ++i) {
//
//    if (*pt == -1) {
//      records_offset.push_back(-1);
//    } else {
//      records_offset.push_back(data_offset);
//      data_offset += *pt;
//    }
//    --pt;
//  }
//  data_end = data_offset;
//  size_t meta_size = sizeof(unsigned) * (1 + records_offset.size());
//  free_space = PAGE_SIZE - data_offset - meta_size;
}

std::string Page::ToString() const {
  std::ostringstream oss;
  oss << "Page: " << pid << " free space:" << free_space << "\n";
//  for (int i = 0; i < records_offset.size(); ++i) {
//    oss << "\tRecord " << i << " offset " << records_offset[i] << "\n";
//  }
  return oss.str();
}

std::vector<std::pair<unsigned, RID>> Page::getRecordOffsets() {
  std::vector<std::pair<unsigned, RID>> record_offsets;
  unsigned *pt = (unsigned *) (data + PAGE_SIZE) - 3;
  for (int i = 0; i < num_slots; i++) {
    auto parsed = decodeDirectory(*pt);
    record_offsets.push_back(parsed);
    --pt;
  }
  return record_offsets;
}

void Page::initPage(char *page_data) {
  // assume page_data is PAGE_SIZE
  *((int *) (page_data + PAGE_SIZE) - 1) = PAGE_SIZE - 2 * sizeof(unsigned); // initial free_space
  *((int *) (page_data + PAGE_SIZE) - 2) = 0; // initial num_slots
}

void Page::writeNumSlotAndFreeSpace() {
  *((int *) (data + PAGE_SIZE) - 1) = free_space;
  *((int *) (data + PAGE_SIZE) - 2) = num_slots;
}

