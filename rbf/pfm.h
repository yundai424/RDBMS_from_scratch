#ifndef _pfm_h_
#define _pfm_h_

#include <string>
#include <fstream>

typedef unsigned PageNum;
typedef int RC;

#define PAGE_SIZE 4096

class FileHandle;

class PagedFileManager {
 public:
  static PagedFileManager &instance();                                // Access to the _pf_manager instance

  RC createFile(const std::string &fileName);                         // Create a new file
  RC destroyFile(const std::string &fileName);                        // Destroy a file
  RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
  RC closeFile(FileHandle &fileHandle);                               // Close a file

  static inline bool ifFileExists(const std::string &fileName) {
    std::ifstream ifs(fileName);
    return ifs.good();
  }

 protected:
  PagedFileManager();                                                 // Prevent construction
  ~PagedFileManager();                                                // Prevent unwanted destruction
  PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
  PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

 private:
  static PagedFileManager *_pf_manager;
};

class FileHandle {
 public:
  // variables to keep the counter for each operation
  unsigned readPageCounter;
  unsigned writePageCounter;
  unsigned appendPageCounter;
  std::string name;

  FileHandle();                                                       // Default constructor
  ~FileHandle();                                                      // Destructor

  RC readPage(PageNum pageNum, void *data);                           // Get a specific page
  RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
  RC appendPage(const void *data);                                    // Append a specific page
  unsigned getNumberOfPages();                                        // Get the number of pages in the file
  RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                          unsigned &appendPageCount);                 // Put current counter values into variables

  // write counter to metadata
  RC writeRecord(size_t pos, const void *record, unsigned size);

  RC createFile(const std::string &fileName);
  RC openFile(const std::string &fileName);
  RC closeFile();
  RC updateCounterToFile();

 private:
  static inline size_t getPos(PageNum page_num) {
    return (page_num + 1) * PAGE_SIZE;
  }

  std::fstream _file;
};

#endif