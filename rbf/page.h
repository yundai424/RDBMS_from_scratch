#ifndef CS222_FALL19_PAGE_H
#define CS222_FALL19_PAGE_H

#include <fstream>
#include <vector>

#include "types.h"

class FileHandle;

typedef unsigned short SID; // slod it
typedef unsigned PID; // page id

struct Page; // Forward declarations

struct FreeSlot {
  Page *page;
  size_t size;
  size_t begin; // relative to page begin

  bool operator<(const FreeSlot &rhs) const {
    return size < rhs.size;
  }
};

class Page {

  std::vector<int> records_offset; // offset, could be negative (-1 means invalid)
  size_t data_end;
  char *data;

  void parseMeta();

  SID findNextSlotID();

 public:

  PID pid;
  size_t free_space; // free_space = real_free_space - sizeof(unsigned), for meta

  explicit Page(PID page_id);

  ~Page();

  void load(FileHandle &handle);

  void dump(FileHandle &handle);

  void freeMem();

  RID insertData(const char *new_data, size_t size);

  std::string ToString() const;

  static void initPage(char *page_data);

  static unsigned encodeRecordOffset(size_t offset, RID pointer);

  static RID decodeRecordOffset(RID rid);


  /**
   * switch data beginning from begin forward by length
   * @param begin
   * @param length
   * @return
   */
  RC switchFoward(size_t begin, size_t length);

 private:

  void updateCounter(int counter);

};

#endif //CS222_FALL19_PAGE_H
