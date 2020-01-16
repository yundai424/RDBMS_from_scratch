#ifndef CS222_FALL19_PAGE_H
#define CS222_FALL19_PAGE_H

#include <fstream>
#include <vector>

#include "types.h"
#include "logger.h"

class FileHandle;

typedef unsigned short SID; // slod it
typedef unsigned PID; // page id
typedef unsigned PageOffset; // offset inside page, should be [0,4096)

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

  size_t data_end;
  char *data;

 public:

  PID pid;
  size_t free_space; // free_space = real_free_space - sizeof(unsigned), for meta
  std::vector<std::pair<PID, PageOffset >> records_offset; // offset, could be negative (4095 means invalid)

  explicit Page(PID page_id);

  ~Page();

  void load(FileHandle &handle);

  void dump(FileHandle &handle);

  void freeMem();

  RID insertData(const char *new_data, size_t size);

  void readData(PageOffset page_offset, void *out, const std::vector<Attribute> &recordDescriptor);


  std::string ToString() const;

  static void initPage(char *page_data);

//  std::vector<std::pair<unsigned, unsigned>> getRecordOffsets() const;

  /**
   * switch data beginning from begin forward by length
   * @param begin
   * @param length
   * @return
   */
  RC switchFoward(size_t begin, size_t length);

 private:

  static constexpr unsigned INVALID_OFFSET = 0xfff;
  static constexpr RC REDIRECT = 2;

  void parseMeta();

  void dumpMeta();

  SID findNextSlotID() const;

  void writeNumSlotAndFreeSpace();

  static inline std::pair<PID, PageOffset> decodeDirectory(unsigned directory) {
    // high 20 bit represent page num, low 12 bit represent offset in page
    unsigned first = (directory & 0xfffff000) >> 12; // first 20 bits
    //TODO: consider the case of forwarding pointer

    return {(directory & 0xfffff000) >> 12, directory & 0xfff};
  }

  static inline unsigned encodeDirectory(std::pair<PID, PageOffset> page_offset) {
    //TODO: consider the case of forwarding pointer
    static const unsigned MAX_PID = 0xfffff;
    if (page_offset.first > MAX_PID) { // exceed 16 bits
      DB_ERROR << "Page id " << page_offset.first << " larger than 0xFFFFF";
      throw std::runtime_error("Page id overflow");
    }
    return (page_offset.first << 12) + page_offset.second;
  }

};

#endif //CS222_FALL19_PAGE_H
