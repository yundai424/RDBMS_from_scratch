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

  SID sid = findNextSlotID();
  if (sid == records_offset.size()) {
    // new slot
    free_space -= sizeof(unsigned);
    records_offset.push_back(data_end);
  } else {
    // use previous deleted slot
    records_offset[sid] = data_end;
  }
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
  // parse meta at the end of page
  // the last int is slot_num, and previous slot_num int storage the size of each record
  // -1 mean invalid (deleted)
  int *pt = (int *) (data + PAGE_SIZE) - 1;
  int slot_num = *pt;
  pt -= slot_num;
  int data_offset = 0;
  for (int i = 0; i < slot_num; ++i) {
    if (*pt == -1) {
      records_offset.push_back(-1);
    } else {
      records_offset.push_back(data_offset);
      data_offset += *pt;
    }
    ++pt;
  }
  data_end = data_offset;
  size_t meta_size = sizeof(unsigned) * (1 + records_offset.size());
  free_space = PAGE_SIZE - data_offset - meta_size;
}

SID Page::findNextSlotID() {
  for (int i = 0; i < records_offset.size(); ++i) {
    if (records_offset[i] == -1) return i;
  }
  return records_offset.size();
}

std::string Page::ToString() const {
  std::ostringstream oss;
  oss << "Page: " << pid << " free space:" << free_space << "\n";
  for (int i = 0; i < records_offset.size(); ++i) {
    oss << "\tRecord " << i << " offset " << records_offset[i] << "\n";
  }
  return oss.str();
}

void Page::initPage(char *page_data) {
  // assume page_data is PAGE_SIZE
  *((int *) (page_data + PAGE_SIZE) - 1) = 0;
}

