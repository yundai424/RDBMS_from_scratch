#ifndef CS222_FALL19_PAGE_H
#define CS222_FALL19_PAGE_H


#define PAGE_SIZE 4096 //TODO: move to types.h later

#include <fstream>
#include <vector>

typedef unsigned short SID;


struct Page; // Forward declarations
struct FreeSlot {
  Page * page;
  size_t  size;
  size_t begin; // relative to page begin

  bool operator<(const FreeSlot & rhs) const {
    return size < rhs.size;
  }
};

struct Page {
  unsigned pid;
  size_t begin; // begin offset
  std::fstream &fs;
  std::vector<size_t> records_offset; // offset

  Page(unsigned page_id, std::fstream &fstream);

  void Load();

  void Save();


};

#endif //CS222_FALL19_PAGE_H
