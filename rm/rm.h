#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <algorithm>
#include <cmath>

#include "../rbf/rbfm.h"

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
  friend class RelationManager;
 public:
  RM_ScanIterator() = default;

  ~RM_ScanIterator() = default;

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);

  RC close();

 private:
  FileHandle file_handle_;
  RBFM_ScanIterator rbfm_scan_iterator_;
};

// Relation Manager
class RelationManager {
 public:
  static const directory_t MAX_SCHEMA_VER;

  static RelationManager &instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

  RC deleteTable(const std::string &tableName);

  RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

  RC insertTuple(const std::string &tableName, const void *data, RID &rid);

  RC deleteTuple(const std::string &tableName, const RID &rid);

  RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

  RC readTuple(const std::string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const std::vector<Attribute> &attrs, const void *data);

  RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const std::string &tableName,
          const std::string &conditionAttribute,
          const CompOp compOp,                  // comparison type such as "<" and "="
          const void *value,                    // used in the comparison
          const std::vector<std::string> &attributeNames, // a list of projected attributes
          RM_ScanIterator &rm_ScanIterator);

// Extra credit work (10 points)
  RC addAttribute(const std::string &tableName, const Attribute &attr);

  RC dropAttribute(const std::string &tableName, const std::string &attributeName);

  void printTables();

 protected:
  RelationManager();                                                  // Prevent construction
  ~RelationManager();                                                 // Prevent unwanted destruction
  RelationManager(const RelationManager &);                           // Prevent construction by copying
  RelationManager &operator=(const RelationManager &);                // Prevent assignment

 private:
  static const int SYSTEM_FLAG;
  static const std::string TABLE_CATALOG_NAME_;
  static const std::string COLUMN_CATALOG_NAME_;
  static const std::vector<Attribute> TABLE_CATALOG_DESC_;
  static const std::vector<Attribute> COLUMN_CATALOG_DESC_;
  RecordBasedFileManager *rbfm_;
  int max_tid_;
  bool init_;

  // TODO: I suspect whether we can store these in memory, or parseCatalog each time
//  std::unordered_map<std::string, std::vector<Attribute>> table_schema_;
  std::unordered_map<std::string, std::vector<std::vector<Attribute>>> table_schema_;
  std::unordered_map<std::string, std::string> table_files_;
  std::unordered_map<std::string, int> table_ids_;
  std::unordered_set<std::string> system_tables_;

  void parseCatalog();

  void inline loadDbIfExist();

  bool inline ifDBExists();

  bool inline ifTableExists(const std::string &tableName);

  /*
   * abstract as a function, in case they change the naming rules :(
   */
  std::string inline getTableFileName(const std::string &tableName, bool is_system_table);

  RC createTableImpl(const std::string &tableName, const std::vector<Attribute> &attrs, bool is_system_table = false);

  RC insertTupleImpl(const std::string &tableName, const void *data, RID &rid, bool is_system = false);

  RC deleteTupleImpl(const std::string &tableName, const RID &rid, bool is_system = false);

  RC updateTupleImpl(const std::string &tableName, const void *data, const RID &rid, bool is_system = false);

  std::vector<char> makeTableRecord(const std::string &table_name, bool is_system = false);

  std::vector<char> makeColumnRecord(const std::string &table_name,
                                     const int idx,
                                     const int ver,
                                     Attribute attr);
};

bool RelationManager::ifDBExists() {
  return table_files_.count(TABLE_CATALOG_NAME_) && table_files_.count(COLUMN_CATALOG_NAME_);
}

bool RelationManager::ifTableExists(const std::string &tableName) {
  return table_files_.count(tableName);
}

std::string RelationManager::getTableFileName(const std::string &tableName, bool is_system_table) {
  return is_system_table ? tableName + ".catalog" : tableName;
}

void RelationManager::loadDbIfExist() {
  /*
   * this part is really tricky, rm in test_util is initialized as static global,
   * but due to the uncertainty of order in static initialization, many other static variable might not be initialized yet
   * so we can not call this function in ctor, instead, we use lazy initialization,
   * all public API should call this funciton first
   */
  if (!init_ &&
      PagedFileManager::ifFileExists(getTableFileName(TABLE_CATALOG_NAME_, true)) &&
      PagedFileManager::ifFileExists(getTableFileName(COLUMN_CATALOG_NAME_, true))) {
//    DB_INFO << "loading existing DB..";
    parseCatalog();
    init_ = true;
  }
}

#endif