#ifndef _rbfm_h_
#define _rbfm_h_

#include "pfm.h"

#include <memory>
#include <unordered_set>
#include <string.h>

typedef unsigned short SID; // slod it
typedef unsigned PID; // page id
typedef unsigned PageOffset; // offset inside page, should be [0,4096)
typedef short directory_t; // directories before real data, to indicate offset for each field

static directory_t MAX_FIELD_NUM = INT16_MAX;


// Record ID
typedef struct {
  unsigned pageNum;    // page number
  unsigned short slotNum;    // slot number in the page
} RID;

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
  size_t data_end;
  char *data;
  size_t real_free_space_;
  std::unordered_set<SID> invalid_slots_;

 public:

  static constexpr size_t MAX_SIZE = PAGE_SIZE - 3 * sizeof(int);
  static constexpr SID FIND_NEW_SID = UINT16_MAX;

  PID pid;
  /*
   * free_space = real_free_space_                (there're deleted directory we can reuse,)
   * free_space = real_free_space_ - sizeof(int)  (all offset directories are full)
   */
  size_t free_space;
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
  RID insertData(const char *new_data, size_t size, SID sid=FIND_NEW_SID);


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
   */
  void readData(PageOffset record_offset, void *out, const std::vector<Attribute> &recordDescriptor);

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

  static constexpr unsigned INVALID_OFFSET = 0xfff;  // PageOffset value to indicate a deleted slot
  static constexpr unsigned FORWARDED_SLOT = 0xfffff; // PID value to indicate the slot is forwarded from other slot
  static constexpr RC REDIRECT = 2;

  void parseMeta();

  void dumpMeta();

  void checkDataend(); // for debug

  inline SID findNextSlotID();

  inline void maintainFreeSpace();

  static inline std::pair<PID, PageOffset> decodeDirectory(unsigned directory);

  static inline unsigned encodeDirectory(std::pair<PID, PageOffset> page_offset);

  static size_t getRecordSize(const char * begin);

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
  //TODO: consider the case of forwarding pointer

  return {(directory & 0xfffff000) >> 12, directory & 0xfff};
}

unsigned Page::encodeDirectory(std::pair<PID, PageOffset> page_offset) {
  //TODO: consider the case of forwarding pointer
  static const unsigned MAX_PID = 0xfffff;
  if (page_offset.first > MAX_PID) { // exceed 16 bits
    throw std::runtime_error("Page id overflow");
  }
  return (page_offset.first << 12) + page_offset.second;
}


struct FreeSlot {
  Page *page;
  size_t size;

  bool operator<(const FreeSlot &rhs) const {
    return size < rhs.size;
  }
};

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

class RBFM_ScanIterator {
  friend class Page;

 public:
  RBFM_ScanIterator() = default;;

  ~RBFM_ScanIterator() = default;;

  // Never keep the results in the memory. When getNextRecord() is called,
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data) { return RBFM_EOF; };

  RC close() { return -1; };
};

class RecordBasedFileManager {
 public:
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
  std::vector<std::shared_ptr<Page>> pages_;
//  std::map<size_t, std::unordered_set<Page *>> free_slots_; // assume each page only have one free slot

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
    return sizeof(directory_t) * (fields_num + 1);
  }

  static std::vector<bool> parseNullIndicator(const unsigned char * data, unsigned fields_num);

  bool isRIDValid(const RID &rid, FileHandle &file_handle);

 public:

  /**
   * encode raw data to std::vector<char> which is ready to be inserted into page
   * @param recordDescriptor
   * @param data
   * @return
   */
  static std::pair<RC, std::vector<char>> serializeRecord(const std::vector<Attribute> &recordDescriptor, const void *data);

  /**
   * decode data from record on page
   * @param recordDescriptor
   * @param out
   * @param src
   */
  static void deserializeRecord(const std::vector<Attribute> &recordDescriptor, void *out, const char *src);

  /**
   * Recursively find the exact slot that stores the record, forwarded by potentially a sequence of forwarding pointers
   * @param record_offset
   * @return
   */
  std::pair<Page *, SID> findForwardingSlot(PID pid, SID sid, FileHandle &fileHandle);

};

#endif // _rbfm_h_
