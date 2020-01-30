#ifndef _pfm_h_
#define _pfm_h_

#include <string>
#include <fstream>
#include <utility>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <map>
#include <vector>
#include <ostream>
#include <stdexcept>
#include <memory>
#include <string.h>

/******************************************
 *
 * =========== Logger class ============
 * since we can not create extra files, only put it here:(
 *
*****************************************/
#define MUTE_LOG

enum LogLevel {
  DEBUGGING = 0,
  INFO = 1,
  WARNING = 2,
  ERROR = 3,
  QUIET = 4

};

static const std::map<LogLevel, std::string> &kPrefixMap() {
  static const std::map<LogLevel, std::string> tmp{
      {DEBUGGING, "\e[1;96m[DEBUG]"},
      {INFO, "\e[1;32m[INFO]"},
      {WARNING, "\e[1;33m[WARN]"},
      {ERROR, "\e[1;31m[ERROR]"},
  };
  return tmp;
}

static std::string kPostfix() {
  static const std::string tmp = "\e[0m";
  return tmp;
}

class Logger {
 private:
  LogLevel level_;
  std::string file_path_;
  std::string func_path_;
  int line_num_;
  bool opened_;
  static LogLevel global_level;

  inline static std::string get_file_name(const std::string &path) {
    return path.substr(path.find_last_of("/\\") + 1);
  }

 public:
  Logger() = delete;

  Logger(const Logger &) = delete;

  explicit Logger(LogLevel level, std::string file_path, int line_num, std::string func_path)
      : level_(level), file_path_(move(file_path)), line_num_(line_num), func_path_(move(func_path)),
        opened_(false) {
    std::cout << std::boolalpha;
  }

  ~Logger() {
    if (opened_) std::cout << std::endl;
  }

  template<typename T>
  Logger &operator<<(const T &v) {
    if (level_ < global_level) return *this;
    if (!opened_) {
      file_path_ = get_file_name(file_path_);
      std::cout << std::setw(14) << std::left << std::dec << kPrefixMap().at(level_)
                << " In '" << func_path_ << "' " << file_path_ << ":" << line_num_
                << " " << kPostfix();
      opened_ = true;
    }
    std::cout << v;
    return *this;
  }

  static void SetGlobalLogLevel(LogLevel level) { global_level = level; }

  static LogLevel GetGlobalLogLevel() { return global_level; }
};

template<typename T>
std::ostream &operator<<(std::ostream &os, const std::vector<T> &v) {
  os << "[";
  for (int i = 0; i < v.size(); ++i) {
    os << v[i];
    if (i != v.size() - 1) os << ", ";
  }
  os << "]\n";
  return os;
}

#define DB_DEBUG Logger(LogLevel::DEBUGGING, __FILE__, __LINE__, __FUNCTION__)
#define DB_INFO Logger(::LogLevel::INFO, __FILE__, __LINE__, __FUNCTION__)
#define DB_WARNING Logger(::LogLevel::WARNING, __FILE__, __LINE__, __FUNCTION__)
#define DB_ERROR Logger(::LogLevel::ERROR, __FILE__, __LINE__, __FUNCTION__)

static std::string print_bytes(const void *ptr, int size) {
  std::ostringstream oss;
  const unsigned char *p = (const unsigned char *) ptr;
  int i;
  char buf[4] = {0};
  for (i = 0; i < size; i++) {
    sprintf(buf, "%02hhX ", p[i]);
    oss << buf;
  }
  return oss.str();
}

static std::string print_bits(const void *ptr, int size) {
  std::ostringstream oss;
  const unsigned char *p = (const unsigned char *) ptr;
  int i;
  for (i = 0; i < size; i++) {
    for (int j = 0; i < 8; ++j) {
      unsigned char mask = 1 << (7 - j);
      if (mask & p[i]) std::cout << 1;
      else std::cout << 0;
    }
    oss << ' ';
  }
  return oss.str();
}

/******************************************
 *
 * =========== Logger class ============
 *
*****************************************/

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

class Page;

class FileHandle {
 public:
  // variables to keep the counter for each operation
  unsigned readPageCounter;
  unsigned writePageCounter;
  unsigned appendPageCounter;
  std::string name;
  std::vector<std::shared_ptr<Page>> pages_;
  bool meta_modified_;

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

 private:
  static inline size_t getPos(PageNum page_num) {
    return (page_num + 1) * PAGE_SIZE;
  }

  std::fstream _file;
};

#endif