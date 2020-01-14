#ifndef CS222_FALL19_PAGE_H
#define CS222_FALL19_PAGE_H

#include <fstream>
#include <vector>

#include "types.h"

typedef unsigned short SID;

struct Page; // Forward declarations
struct FreeSlot {
  Page *page;
  size_t size;
  size_t begin; // relative to page begin

  bool operator<(const FreeSlot &rhs) const {
    return size < rhs.size;
  }
};

struct Page {

  static inline size_t getPos(PageNum page_num) {
    return OFFSET + page_num * PAGE_SIZE;
  }

  unsigned pid;
  size_t begin; // begin offset
  std::vector<size_t> records_offset; // offset

  Page(unsigned page_id, std::fstream &fstream);

  void Load();

  void Save();

};

#endif //CS222_FALL19_PAGE_H
