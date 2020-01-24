#include "rm.h"

const int RelationManager::SYSTEM_FLAG = 1;
const std::string RelationManager::TABLE_CATALOG_NAME_ = "Tables";
const std::string RelationManager::COLUMN_CATALOG_NAME_ = "Columns";
const std::vector<Attribute> RelationManager::TABLE_CATALOG_DESC_ = {{"tableId", AttrType::TypeInt, 4},
                                                                     {"tableName", AttrType::TypeVarChar, 50},
                                                                     {"fileName", AttrType::TypeVarChar, 50},
                                                                     {"isSystem", AttrType::TypeInt, 4}};
const std::vector<Attribute> RelationManager::COLUMN_CATALOG_DESC_ = {{"tableId", AttrType::TypeInt, 4},
                                                                      {"columnName", AttrType::TypeVarChar, 50},
                                                                      {"columnType", AttrType::TypeInt, 4},
                                                                      {"columnLength", AttrType::TypeInt, 4},
                                                                      {"columnPosition", AttrType::TypeInt, 4}};

RelationManager &RelationManager::instance() {
  static RelationManager _relation_manager = RelationManager();
  return _relation_manager;
}

RelationManager::RelationManager() : rbfm_(&RecordBasedFileManager::instance()), max_tid_(0) {}

RelationManager::~RelationManager() = default;

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

RC RelationManager::createCatalog() {
  if (ifDBExists()) return -1;

  rbfm_->createFile(getTableFileName(TABLE_CATALOG_NAME_, true));
  rbfm_->createFile(getTableFileName(COLUMN_CATALOG_NAME_, true));
  table_files_[TABLE_CATALOG_NAME_] = getTableFileName(TABLE_CATALOG_NAME_, true);
  table_files_[COLUMN_CATALOG_NAME_] = getTableFileName(COLUMN_CATALOG_NAME_, true);
  table_schema_[TABLE_CATALOG_NAME_] = TABLE_CATALOG_DESC_;
  table_schema_[COLUMN_CATALOG_NAME_] = COLUMN_CATALOG_DESC_;
  table_ids_[TABLE_CATALOG_NAME_] = ++max_tid_;
  table_ids_[COLUMN_CATALOG_NAME_] = ++max_tid_;
  system_tables_ = {TABLE_CATALOG_NAME_, COLUMN_CATALOG_NAME_};

  createTableImpl(TABLE_CATALOG_NAME_, TABLE_CATALOG_DESC_, true);
  createTableImpl(COLUMN_CATALOG_NAME_, COLUMN_CATALOG_DESC_, true);

  return 0;
}

RC RelationManager::deleteCatalog() {
  if (!ifDBExists()) return -1;
  for (auto &f : system_tables_) {
    if (rbfm_->destroyFile(f) != 0)
      return -1;
  }
  return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
  if (!ifDBExists() || ifTableExists(tableName)) return -1;

  return createTableImpl(tableName, attrs);
}

RC RelationManager::deleteTable(const std::string &tableName) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;

  // TODO: use scan to find entry in catalog and delete
  char buffer[PAGE_SIZE] = {0};
  // select from TABLE_C_N where tableId == tableId
  RM_ScanIterator rmsi;
  int id = table_ids_.at(tableName);
  scan(TABLE_CATALOG_NAME_, "tableId", EQ_OP, &id, {"table-id"}, rmsi);
  RID tab_rid;
  while (rmsi.getNextTuple(tab_rid, buffer) != RM_EOF) {
    if (deleteTuple(TABLE_CATALOG_NAME_, tab_rid) != 0) {
      rmsi.close();
      return -1;
    }
  }
  rmsi.close();

  // select from TABLE_C_N where tableId == tableId
  scan(COLUMN_CATALOG_NAME_, "tableId", EQ_OP, &id, {"table-id"}, rmsi);
  RID col_rid;
  while (rmsi.getNextTuple(col_rid, buffer) != RM_EOF) {
    if (deleteTuple(COLUMN_CATALOG_NAME_, col_rid) != 0) {
      rmsi.close();
      return -1;
    }
  }
  rmsi.close();

  if (!rbfm_->destroyFile(table_files_[tableName])) return -1;
  table_schema_.erase(tableName);
  table_files_.erase(tableName);
  return 0;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  attrs = table_schema_.at(tableName);
  return 0;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  const auto &recordDescriptor = table_schema_.at(tableName);
  FileHandle fh;
  rbfm_->openFile(table_file, fh);
  RC ret = rbfm_->insertRecord(fh, recordDescriptor, data, rid);
  rbfm_->closeFile(fh);
  return ret;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  const auto &recordDescriptor = table_schema_.at(tableName);
  FileHandle fh;
  rbfm_->openFile(table_file, fh);
  RC ret = rbfm_->deleteRecord(fh, recordDescriptor, rid);
  rbfm_->closeFile(fh);
  return ret;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  const auto &recordDescriptor = table_schema_.at(tableName);
  FileHandle fh;
  rbfm_->openFile(table_file, fh);
  RC ret = rbfm_->updateRecord(fh, recordDescriptor, data, rid);
  rbfm_->closeFile(fh);
  return ret;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  const auto &recordDescriptor = table_schema_.at(tableName);
  FileHandle fh;
  rbfm_->openFile(table_file, fh);
  RC ret = rbfm_->readRecord(fh, recordDescriptor, rid, data);
  rbfm_->closeFile(fh);
  return ret;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
  if (!rbfm_->printRecord(attrs, data))
    return -1;
  return 0;
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  const auto &recordDescriptor = table_schema_.at(tableName);
  FileHandle fh;
  rbfm_->openFile(table_file, fh);
  RC ret = rbfm_->readAttribute(fh, recordDescriptor, rid, attributeName, data);
  rbfm_->closeFile(fh);
  return ret;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  const auto &recordDescriptor = table_schema_.at(tableName);
  FileHandle fh;
  rbfm_->openFile(table_file, fh);
  rm_ScanIterator.rbfm_scan_iterator_.init(fh, rbfm_, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
  RC ret = rbfm_->scan(fh, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_scan_iterator_);
  // should not close file here: need to fetch records via fh later on...
//  rbfm_->closeFile(fh);
  return ret;
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
  if (!is_system_table) {
    // for system table, these must be created before hand
    table_schema_[tableName] = attrs;
    table_files_[tableName] = getTableFileName(tableName, is_system_table);
    rbfm_->createFile(table_files_[tableName]);
    table_ids_[tableName] = ++max_tid_;
  }

  auto table_data = makeTableRecord(tableName, is_system_table);
  rbfm_->printRecord(TABLE_CATALOG_DESC_, table_data.data());
  RID tbl_id;
  if (insertTuple(TABLE_CATALOG_NAME_, table_data.data(), tbl_id) != 0) return -1;
  for (int i = 0; i < attrs.size(); ++i) {
    auto column_data = makeColumnRecord(tableName, i, attrs[i]);
    rbfm_->printRecord(COLUMN_CATALOG_DESC_, column_data.data());
    RID col_id;
    if (insertTuple(COLUMN_CATALOG_NAME_, column_data.data(), col_id) != 0) return -1;
  }
  return 0;
}

std::vector<char> RelationManager::makeTableRecord(const std::string &table_name, bool is_system) {
  /*
   * Tables (table-id:int, table-name:varchar(50), file-name:varchar(50), is-system:int)
   */
  static const int field_num = 4;
  int null_indicator_length = int(ceil(double(field_num) / 8));
  unsigned table_record_length = null_indicator_length; // null indicator
  table_record_length += sizeof(int); // table id
  std::string file_name = getTableFileName(table_name, is_system);
  table_record_length = table_record_length + sizeof(int) + table_name.size(); // table name
  table_record_length = table_record_length + sizeof(int) + file_name.size(); // file name
  table_record_length += sizeof(int); // is-system

  DB_DEBUG << "table record length of: " << table_name << " is " << table_record_length;
  std::vector<char> table_record(table_record_length, 0);
  memset(table_record.data(), 0, null_indicator_length);
  unsigned offset = null_indicator_length;

  int table_id = table_ids_[table_name];
  memcpy(table_record.data() + offset, &table_id, sizeof(int)); // table id
  offset += sizeof(int);
  int table_name_len = table_name.size();
  memcpy(table_record.data() + offset, &table_name_len, sizeof(int)); // table name length
  offset += sizeof(int);
  memcpy(table_record.data() + offset, table_name.c_str(), table_name.size()); // table name
  offset += table_name.size();
  int file_name_len = file_name.size();
  memcpy(table_record.data() + offset, &file_name_len, sizeof(int)); // file name length
  offset += sizeof(int);
  memcpy(table_record.data() + offset, file_name.c_str(), file_name.size()); // file name
  offset += file_name.size();
  int issys = (int) is_system;
  memcpy(table_record.data() + offset, &issys, sizeof(int)); // is system
  return table_record;
}

std::vector<char> RelationManager::makeColumnRecord(const std::string &table_name,
                                                    const int idx,
                                                    Attribute attr) {
  int null_indicator_length = int(ceil(double(4) / 8));
  unsigned column_record_length = null_indicator_length; // null indicator
  column_record_length += sizeof(int); // table id
  column_record_length = column_record_length + sizeof(int) + attr.name.size(); // column name
  column_record_length += 3 * sizeof(int); // column type, length, position


  DB_DEBUG << "column record length of: " << table_name << "," << attr.name << " is " << column_record_length;
  std::vector<char> column_record(column_record_length, 0);
  memset(column_record.data(), 0, null_indicator_length);
  unsigned offset = null_indicator_length;

  // table id
  int table_id = table_ids_[table_name];
  memcpy(column_record.data() + offset, &table_id, sizeof(int));
  offset += sizeof(int);
  // col name length
  int col_name_len = attr.name.size();
  memcpy(column_record.data() + offset, &col_name_len, sizeof(int));
  offset += sizeof(int);
  // col name
  memcpy(column_record.data() + offset, attr.name.c_str(), table_name.size());
  offset += table_name.size();
  // col type
  int col_type = attr.type;
  memcpy(column_record.data() + offset, &col_type, sizeof(int));
  offset += sizeof(int);
  // col len
  memcpy(column_record.data() + offset, &(attr.length), sizeof(int));
  offset += sizeof(int);
  // col pos
  memcpy(column_record.data() + offset, &idx, sizeof(int));
  return column_record;
}

RC RM_ScanIterator::close() {
  rbfm_scan_iterator_.close();
}

RC RM_ScanIterator::getNextTuple(RID & rid, void * data) {
  return rbfm_scan_iterator_.getNextRecord(rid, data);
}