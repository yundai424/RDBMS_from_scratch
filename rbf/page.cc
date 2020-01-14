#include "page.h"

Page::Page(unsigned page_id, FileHandler * file_handle) : pid(page_id), handle_(file_handle) {}

void Page::Load() {
  fs.seekg(begin + PAGE_SIZE - sizeof(unsigned short));
  unsigned short record_num;
  fs >> record_num;

}

void Page::Save() {

}