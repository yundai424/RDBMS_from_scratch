#ifndef _rbfm_h_
#define _rbfm_h_

#include "pfm.h"

#include <unordered_set>
#include <functional>
#include <unordered_map>
#include <algorithm>
#include <cmath>

typedef unsigned short SID; // slod it
typedef unsigned PID; // page id
typedef unsigned PageOffset; // offset inside page, should be [0,4096)
typedef short directory_t; // directories before real data, to indicate offset for each field

static const directory_t MAX_FIELD_NUM = INT16_MAX;
static const PID INVALID_PID = UINT32_MAX;

// Record ID
struct RID {
  unsigned pageNum;    // page number
  unsigned short slotNum;    // slot number in the page
  std::string inline toString() const {
    return "<" + std::to_string(pageNum) + "," + std::to_string(slotNum) + ">";
  }
};

struct RIDHash {
  std::size_t operator()(const RID &rid) const { return ((size_t) rid.pageNum << 32) ^ ((size_t) rid.slotNum); }
};

// Attribute
typedef enum {
  TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
  std::string name;  // attribute name
  AttrType type;     // attribute type
  AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
  EQ_OP = 0, // no condition// =
  LT_OP,      // <
  LE_OP,      // <=
  GT_OP,      // >
  GE_OP,      // >=
  NE_OP,      // !=
  NO_OP       // no condition
} CompOp;

struct EnumHash {
  template<typename T, typename std::enable_if<std::is_enum<T>::value, T>::type * = nullptr>
  std::size_t operator()(T t) const {
    return static_cast<std::size_t>(t);
  }
};

/******************************************
 *
 * =========== CUSTOM CLASSES ============
 *
*****************************************/


/**
 * abstraction of a page, embedded in a vector in rbfm
 */
class Page {
  friend class RecordBasedFileManager;
  friend class FileHandle;
  size_t data_end;
  char *data;
  unsigned real_free_space_;
  std::unordered_set<SID> invalid_slots_;

 public:

  static const size_t MAX_SIZE;
  static const SID FIND_NEW_SID;
  static const unsigned INVALID_OFFSET;  // PageOffset value to indicate a deleted slot
  static const unsigned REDIRECT_PID; // PID value to indicate the slot is forwarded from other slot

  PID pid;
  /*
   * free_space = real_free_space_                (there're deleted directory we can reuse,)
   * free_space = real_free_space_ - sizeof(int)  (all offset directories are full)
   */
  unsigned free_space;
  std::vector<std::pair<PID, PageOffset >> records_offset; // offset (4095 means invalid)

  explicit Page(PID page_id);

  ~Page();

  void load(FileHandle &handle);

  void dump(FileHandle &handle);

  void freeMem();

  /**
   *
   * @param new_data
   * @param size
   * @param sid only specify this when updateRecord redirect back to origin page
   * @return
   */
  RID insertData(const char *new_data, size_t size, SID sid = FIND_NEW_SID);

  /**
   * erase data and update free space accordingly
   * @param record_begin_offset begin offset of record
   * @return
   */
  RC deleteRecord(size_t record_begin_offset);

  /**
   * assume data exist in this page (already redirected, if so)
   * @param record_offset begin offset of record
   * @param out
   * @param recordDescriptor
   * @param projected_fields
   * @param cmp
   * @param cond_field
   * @param cond_value
   * @return if return code is COND_NOT_SATISFIED, nothing will be written to out
   */
  RC readData(PageOffset record_offset,
              void *out,
              const std::vector<std::vector<Attribute>> &recordDescriptors,
              const std::vector<std::string> &projected_fields,
              CompOp cmp = CompOp::NO_OP,
              const std::string &cond_field = "",
              const void *cond_value = nullptr);


//  std::string ToString() const;

  static void initPage(char *page_data);

  /**
   * all record data after `after_offset` will be shift `switch_offset` bytes forward/backward
   * @param record_begin_offset
   * @param shift_size
   * @param forward
   * @return
   */
  RC shiftAfterRecords(size_t record_begin_offset, size_t shift_size, bool forward);

 private:

  void parseMeta();

  void dumpMeta();

  void checkDataend(); // for debug

  inline SID findNextSlotID();

  inline void maintainFreeSpace();

  static inline std::pair<PID, PageOffset> decodeDirectory(unsigned directory);

  static inline unsigned encodeDirectory(std::pair<PID, PageOffset> page_offset);

  /**
   * the record size include the heading directories
   * @param begin
   * @return
   */
  static size_t getRecordSize(const char *begin);

};

SID Page::findNextSlotID() {
  if (invalid_slots_.empty()) return records_offset.size();
  SID ret = *invalid_slots_.begin();
  invalid_slots_.erase(invalid_slots_.begin());
  return ret;
}

void Page::maintainFreeSpace() {
  free_space = invalid_slots_.empty() ? real_free_space_ - sizeof(int) : real_free_space_;
}

std::pair<PID, PageOffset> Page::decodeDirectory(unsigned directory) {
  // high 20 bit represent page num, low 12 bit represent offset in page
  unsigned first = (directory & 0xfffff000) >> 12; // first 20 bits

  return {(directory & 0xfffff000) >> 12, directory & 0xfff};
}

unsigned Page::encodeDirectory(std::pair<PID, PageOffset> page_offset) {
  static const unsigned MAX_PID = 0xfffff;
  if (page_offset.first > MAX_PID) { // exceed 16 bits
    throw std::runtime_error("Page id overflow");
  }
  return (page_offset.first << 12) + page_offset.second;
}

/********************************************************************
* The scan iterator is NOT required to be implemented for Project 1 *
********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

//  RBFM_ScanIterator is an iterator to go through records
//  The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RecordBasedFileManager;

class RBFM_ScanIterator {
  friend class Page;

  RecordBasedFileManager *rbfm_;
  FileHandle *file_handle_;
  std::shared_ptr<Page> page_;
  PID pid_;
  SID sid_;
  std::vector<std::vector<Attribute>> schemas_;
  std::vector<std::string> projected_fields_;
  std::string cond_field_;
  CompOp comp_op_;
  const void *value_;
  bool init_;

 public:
  RBFM_ScanIterator();

  ~RBFM_ScanIterator() = default;;

  // Never keep the results in the memory. When getNextRecord() is called,
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);

  RC close();

  RC init(
      FileHandle &fileHandle,
      RecordBasedFileManager *rbfm,
      const std::vector<std::vector<Attribute>> &schemas,
      const std::string &conditionAttribute,
      CompOp compOp,
      const void *value,
      const std::vector<std::string> &attributeNames);

};

class RecordBasedFileManager {
  friend class RBFM_ScanIterator;
  friend class RelationManager;
 public:

  static const RC COND_NOT_SATISFIED;

  static RecordBasedFileManager &instance();                          // Access to the _rbf_manager instance

  RC createFile(const std::string &fileName);                         // Create a new record-based file

  RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

  RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

  RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first,
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.

  // Insert a record into a file
  RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  // Read a record identified by the given rid.
  RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

  // Print the record that is passed to this utility method.
  // This method will be mainly used for debugging/testing.
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data);

  /*****************************************************************************************************
  * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
  * are NOT required to be implemented for Project 1                                                   *
  *****************************************************************************************************/
  // Delete a record identified by the given rid.
  RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                  const RID &rid);

  // Read an attribute given its name and the rid.
  RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                   const std::string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  RC scan(FileHandle &fileHandle,
          const std::vector<Attribute> &recordDescriptor,
          const std::string &conditionAttribute,
          const CompOp compOp,                  // comparision type such as "<" and "="
          const void *value,                    // used in the comparison
          const std::vector<std::string> &attributeNames, // a list of projected attributes
          RBFM_ScanIterator &rbfm_ScanIterator);

 protected:
  RecordBasedFileManager();                                                   // Prevent construction
  ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
  RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
  RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment

 private:
  static RecordBasedFileManager *_rbf_manager;

  PagedFileManager *pfm_;
//  std::vector<std::shared_ptr<Page>> pages_;
//  std::map<size_t, std::unordered_set<Page *>> free_slots_; // assume each page only have one free slot


  /**
   * reuse by `readRecord` and `readAttribute`
   * @param fileHandle
   * @param recordDescriptor schema of different versions
   * @param rid
   * @param data
   * @param projected_fields empty means all
   * @return
   */
  RC readRecordImpl(FileHandle &fileHandle,
                    const std::vector<std::vector<Attribute>> &recordDescriptors,
                    const RID &rid,
                    void *data,
                    const std::vector<std::string> &projected_fields);

  /**
   *
   * @param fileHandle
   * @param recordDescriptor
   * @param data
   * @param rid
   * @param ver
   * @return
   */
  RC insertRecordImpl(FileHandle &fileHandle,
                      const std::vector<Attribute> &recordDescriptor,
                      const void *data,
                      RID &rid,
                      const directory_t ver);

  /**
   *
   * @param fileHandle
   * @param recordDescriptor
   * @param data
   * @param rid
   * @param ver
   * @return
   */
  RC updateRecordImpl(FileHandle &fileHandle,
                      const std::vector<Attribute> &recordDescriptor,
                      const void *data,
                      const RID &rid,
                      const directory_t ver);


  /**
   * load meta of next page into memory
   * @param fileHandle
   */
  void loadNextPage(FileHandle &fileHandle);

  /**
   * append a new page and return the pid
   * @param file_handle
   */
  void appendNewPage(FileHandle &file_handle);

  /**
   * find Available page to insert `size` data, will append new page if all pages are full
   * @param size
   * @param file_handle
   * @return
   */
  Page *findAvailableSlot(size_t size, FileHandle &file_handle);

  static inline directory_t entryDirectoryOverheadLength(int fields_num) {
    return sizeof(directory_t) * (fields_num + 2); // one for field_num, one for version
  }

  /**
   * check Rid and load page, if check valid, Page pointer will be returned, otherwise nullptr
   * @param rid
   * @param file_handle
   * @return
   */
  std::pair<bool, Page *> loadPageWithRid(const RID &rid, FileHandle &file_handle);

  static int inline myStrcmp(const char *s1, const char *s2, int l1, int l2) {
    int min_l = std::min(l1, l2);
    int ret = strncmp(s1, s2, min_l);
    return ret == 0 ? l1 - l2 : ret;
  }

 public:

  /**
   * encode raw data to std::vector<char> which is ready to be inserted into page
   * @param recordDescriptor
   * @param data
   * @param ver
   * @return
   */
  static std::pair<RC, std::vector<char>> serializeRecord(const std::vector<Attribute> &recordDescriptor,
                                                          const void *data,
                                                          const directory_t ver);

  /**
   * decode data from record on page
   * @param recordDescriptor
   * @param out
   * @param src
   * @param projected_fields
   * @param cmp
   * @param cond_field empty means None
   * @param cond_value

   * @return
   */
  static RC deserializeRecord(const std::vector<std::vector<Attribute>> &recordDescriptors,
                              void *out,
                              const char *src,
                              std::vector<std::string> projected_fields,
                              CompOp cmp,
                              const std::string &cond_field,
                              const void *cond_value);

  static bool cmpAttr(CompOp cmp,
                      AttrType type,
                      const void *val1,
                      const void *val2);

  static std::vector<bool> parseNullIndicator(const unsigned char *data, unsigned fields_num);

  static std::vector<char> makeNullIndicator(const std::vector<bool> &null_indicators);

  static int inline nullIndicatorLength(const std::vector<Attribute> &attrs) {
    return int(ceil(double(attrs.size()) / 8));
  }

  static int inline getFieldOffset(const std::vector<Attribute> &attrs, const void *data, int pos) {
    return nullIndicatorLength(attrs) + getRecordLength(attrs, data, pos);
  }

  static int getRecordLength(const std::vector<Attribute> &attrs, const void *data, int pos = -1);
};

#endif // _rbfm_h_
