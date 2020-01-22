#include "rm.h"

const int RelationManager::SYSTEM_FLAG = 1;
const std::string RelationManager::TABLE_CATALOG_ = "Tables.catalog";
const std::string RelationManager::COLUMN_CATALOG_ = "Columns.catalog";
const std::unordered_set<std::string> RelationManager::system_tables_ = {TABLE_CATALOG_, COLUMN_CATALOG_};

RelationManager &RelationManager::instance() {
  static RelationManager _relation_manager = RelationManager();
  return _relation_manager;
}

RelationManager::RelationManager() : rbfm_(&RecordBasedFileManager::instance()) {}

RelationManager::~RelationManager() = default;

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

RC RelationManager::createCatalog() {
  if (ifDBExists()) return -1;
  /*
   * create Tables table, strictly follow below format and naming:
   * Tables (table-id:int, table-name:varchar(50), file-name:varchar(50), is-system:int)
   */
  table_to_id_[TABLE_CATALOG_] = 0;
  table_to_id_[COLUMN_CATALOG_] = 1;
  std::vector<Attribute> tbl_attrs{{"table-id", AttrType::TypeInt, 4},
                                   {"table-name", AttrType::TypeVarChar, 50},
                                   {"file-name", AttrType::TypeVarChar, 50},
                                   {"is-system", AttrType::TypeInt, 4}};
  std::vector<Attribute> col_attrs{{"table-id", AttrType::TypeInt, 4},
                                   {"column-name", AttrType::TypeVarChar, 50},
                                   {"column-type", AttrType::TypeInt, 4},
                                   {"column-length", AttrType::TypeInt, 4},
                                   {"column-position", AttrType::TypeInt, 4}};

  RID tmp_rid;
  rbfm_->createFile(TABLE_CATALOG_);
  rbfm_->openFile(TABLE_CATALOG_, table_fh_);
  rbfm_->insertRecord(table_fh_, tbl_attrs, makeTableRecord(TABLE_CATALOG_, true).data(), tmp_rid);
  rbfm_->insertRecord(table_fh_, tbl_attrs, makeTableRecord(COLUMN_CATALOG_, true).data(), tmp_rid);
  rbfm_->closeFile(table_fh_);

  /*
   * create Columns table, strictly follow below format and naming:
   * Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
   */
  rbfm_->createFile(COLUMN_CATALOG_);
  rbfm_->openFile(COLUMN_CATALOG_, column_fh_);
  for (int i = 0; i < tbl_attrs.size(); ++i) {
    rbfm_->insertRecord(column_fh_, col_attrs, makeColumnRecord(TABLE_CATALOG_, i, tbl_attrs[i]).data(), tmp_rid);
  }
  for (int i = 0; i < col_attrs.size(); ++i) {
    rbfm_->insertRecord(column_fh_, col_attrs, makeColumnRecord(COLUMN_CATALOG_, i, col_attrs[i]).data(), tmp_rid);
  }
  rbfm_->closeFile(column_fh_);
  return 0;
}

RC RelationManager::deleteCatalog() {
  if (!ifDBExists()) return -1;
  table_fh_.closeFile();
  column_fh_.closeFile();
  for (auto &f : system_tables_) {
    if (rbfm_->destroyFile(f) != 0)
      return -1;
  }
  return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
  if (!ifDBExists()) return -1;
  if (table_to_id_.find(tableName) == table_to_id_.end()) return -1;
  return createTableImpl(tableName, attrs);
}

RC RelationManager::deleteTable(const std::string &tableName) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
  if (!rbfm_->printRecord(attrs, data))
    return -1;
  return 0;
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  return -1;
}

RC RelationManager::createTableImpl(const std::string &tableName,
                                    const std::vector<Attribute> &attrs,
                                    bool is_system_table) {
  table_to_id_[tableName] = table_to_id_.size();
  auto table_data = makeTableRecord(tableName, is_system_table).data();
  RID tbl_id;
  if (insertTuple(TABLE_CATALOG_, table_data, tbl_id) != 0) return -1;
  for (int i = 0; i < attrs.size(); ++i) {
    auto column_data = makeColumnRecord(tableName, i, attrs[i]).data();
    RID col_id;
    if (insertTuple(COLUMN_CATALOG_, column_data, col_id) != 0) return -1;
  }
  return 0;
}


std::vector<char> RelationManager::makeTableRecord(const std::string &table_name, bool is_system) {
  int null_indicator_length = int(ceil(double(4) / 8));
  unsigned table_record_length = null_indicator_length; // null indicator
  table_record_length += sizeof(int); // table id
  std::string file_name = is_system ? table_name : (table_name + ".db");
  table_record_length = table_record_length + sizeof(int) + table_name.size(); // table name
  table_record_length = table_record_length + sizeof(int) + file_name.size(); // file name
  table_record_length += sizeof(int); // is-system

  DB_DEBUG << "table record length of: " << table_name << " is " << table_record_length;
  std::vector<char> table_record(table_record_length, 0);
  unsigned offset = null_indicator_length;

  memcpy(table_record.data() + offset, (char *) (table_to_id_[table_name]), sizeof(int)); // table id
  offset += sizeof(int);
  memcpy(table_record.data() + offset, (char*) table_name.size(), sizeof(int)); // table name length
  offset += sizeof(int);
  memcpy(table_record.data() + offset, table_name.c_str(), table_name.size()); // table name
  offset += table_name.size();
  memcpy(table_record.data() + offset, (char*) file_name.size(), sizeof(int)); // file name length
  offset += sizeof(int);
  memcpy(table_record.data() + offset, file_name.c_str(), file_name.size()); // file name
  offset += file_name.size();
  memcpy(table_record.data() + offset, (char*)((int) is_system), sizeof(int)); // is system
  return table_record;
}

std::vector<char> RelationManager::makeColumnRecord(const std::string &table_name,
                                                    const int idx,
                                                    Attribute attr) {
  int null_indicator_length = int(ceil(double(4) / 8));
  unsigned column_record_length = null_indicator_length; // null indicator
  column_record_length += sizeof(int); // table id
  column_record_length = column_record_length + sizeof(int) + attr.name.size(); // column name
  column_record_length += 3  * sizeof(int); // column type, length, position


  DB_DEBUG << "column record length of: " << table_name << "," << attr.name << " is " << column_record_length;
  std::vector<char> column_record(column_record_length, 0);
  unsigned offset = null_indicator_length;

  memcpy(column_record.data() + offset, (char *) (table_to_id_[table_name]), sizeof(int)); // table id
  offset += sizeof(int);
  memcpy(column_record.data() + offset, (char*) attr.name.size(), sizeof(int)); // column name length
  offset += sizeof(int);
  memcpy(column_record.data() + offset, attr.name.c_str(), attr.name.size()); // column name
  offset += attr.name.size();
  memcpy(column_record.data() + offset, (char *) attr.type, sizeof(attr.type)); // column type
  offset += sizeof(attr.type);
  memcpy(column_record.data() + offset, (char *) attr.length, sizeof(attr.length)); // column length
  offset += sizeof(attr.length);
  memcpy(column_record.data() + offset, (char *) idx, sizeof(idx)); // column pos
  return column_record;
}