#include "rm.h"

const int RelationManager::SYSTEM_FLAG = 1;

RelationManager &RelationManager::instance() {
  static RelationManager _relation_manager = RelationManager();
  return _relation_manager;
}

RelationManager::RelationManager() : rbfm_(&RecordBasedFileManager::instance()) {}

RelationManager::~RelationManager() = default;

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

RC RelationManager::createCatalog() {

  rbfm_->createFile(catalog_file_name_);

  // TODO

  /*
   * create Tables table, strictly follow below format and naming:
   * Tables (table-id:int, table-name:varchar(50), file-name:varchar(50), is-system:int)
   */
  {
    std::string tableName = "Tables";
    std::vector<Attribute> attrs{{"table-id", AttrType::TypeInt, 4},
                                 {"table-name", AttrType::TypeVarChar, 50},
                                 {"file-name", AttrType::TypeVarChar, 50},
                                 {"file-name", AttrType::TypeInt, 4}};
    createTableImpl(tableName, attrs, true);
  }
  /*
   * create Columns table, strictly follow below format and naming:
   * Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
   */
  {
    std::string tableName = "Tables";
    std::vector<Attribute> attrs{{"table-id", AttrType::TypeInt, 4},
                                 {"column-name", AttrType::TypeVarChar, 50},
                                 {"column-type", AttrType::TypeInt, 4},
                                 {"column-length", AttrType::TypeInt, 4},
                                 {"column-position", AttrType::TypeInt, 4}};
    createTableImpl(tableName, attrs, true);
  }

  return 0;
}

RC RelationManager::deleteCatalog() {
  if (!checkCatalog()) return -1;
  rbfm_->destroyFile(catalog_file_name_);
  return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
  if (!checkCatalog()) return -1;
  return createTableImpl(tableName, attrs);
}

RC RelationManager::deleteTable(const std::string &tableName) {
  if (!checkCatalog()) return -1;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
  if (!checkCatalog()) return -1;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
  if (!checkCatalog()) return -1;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
  if (!checkCatalog()) return -1;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
  if (!checkCatalog()) return -1;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
  if (!checkCatalog()) return -1;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
  if (!checkCatalog()) return -1;
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
  if (!checkCatalog()) return -1;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
  if (!checkCatalog()) return -1;
  return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
  if (!checkCatalog()) return -1;
  return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
  if (!checkCatalog()) return -1;
  return -1;
}

RC RelationManager::createTableImpl(const std::string &tableName,
                                    const std::vector<Attribute> &attrs,
                                    bool is_system_table) {
  if (table_files_.count(tableName)) {
    DB_ERROR << "Table " << tableName << " already exist!";
    return -1;
  }
  std::string table_file_name = tableName + ".db";
  if (is_system_table) {
    table_file_name += ".catalog";
    system_tables_.insert(table_file_name);
  }
  table_files_[tableName] = table_file_name;

  // TODO

  return -1;
}


