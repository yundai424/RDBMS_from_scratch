#include "page.h"

Page::Page(unsigned page_id, std::fstream &fstream) : pid(page_id), fs(fstream) {}

void Page::Load() {
  fs.seekg(begin + PAGE_SIZE - sizeof(unsigned short));
  unsigned short record_num;
  fs >> record_num;

}

void Page::Save() {

}