#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
 public:
  RM_ScanIterator() = default;

  ~RM_ScanIterator() = default;

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) { return RM_EOF; };

  RC close() { return -1; };
};

// Relation Manager
class RelationManager {
 public:
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

  std::vector<char> makeTableRecord(const std::string &table_name, bool is_system = false);

  std::vector<char> makeColumnRecord(const std::string &table_name,
                                     const int idx,
                                     Attribute attr);

 protected:
  RelationManager();                                                  // Prevent construction
  ~RelationManager();                                                 // Prevent unwanted destruction
  RelationManager(const RelationManager &);                           // Prevent construction by copying
  RelationManager &operator=(const RelationManager &);                // Prevent assignment

 private:
  static const int SYSTEM_FLAG;
  static const std::string TABLE_CATALOG_;
  static const std::string COLUMN_CATALOG_;
  static const std::unordered_set<std::string> system_tables_;
  static FileHandle table_fh_;
  static FileHandle column_fh_;
  RecordBasedFileManager *rbfm_;
  std::unordered_map<std::string, int> table_to_id_;
  std::unordered_map<std::string, std::vector<Attribute>> table_to_attrs_;

  /**
   * parse existed catalog files to memory
   */
  void parseCatalog();

  bool inline ifDBExists();

  bool inline ifTableExists(const std::string &tableName) {
    return table_to_id_.find(tableName) != table_to_id_.end();
  }

  RC createTableImpl(const std::string &tableName, const std::vector<Attribute> &attrs, bool is_system_table=false);
};

bool RelationManager::ifDBExists() {
  for (auto &f : system_tables_) {
    if (!PagedFileManager::ifFileExists(f)) {
      DB_ERROR << "database not created yet!";
      return false;
    }
  }
  return true;
}

#endif