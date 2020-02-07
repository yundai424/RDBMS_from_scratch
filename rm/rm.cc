#include <sys/stat.h>
#include "rm.h"

const int RelationManager::SYSTEM_FLAG = 1;
const std::string RelationManager::TABLE_CATALOG_NAME_ = "Tables";
const std::string RelationManager::COLUMN_CATALOG_NAME_ = "Columns";
const std::vector<Attribute> RelationManager::TABLE_CATALOG_DESC_ = {{"table-id", AttrType::TypeInt, 4},
                                                                     {"table-name", AttrType::TypeVarChar, 50},
                                                                     {"file-name", AttrType::TypeVarChar, 50},
                                                                     {"is-system", AttrType::TypeInt, 4}};
const std::vector<Attribute> RelationManager::COLUMN_CATALOG_DESC_ = {{"table-id", AttrType::TypeInt, 4},
                                                                      {"column-name", AttrType::TypeVarChar, 50},
                                                                      {"column-type", AttrType::TypeInt, 4},
                                                                      {"column-length", AttrType::TypeInt, 4},
                                                                      {"column-position", AttrType::TypeInt, 4},
                                                                      {"column-ver", AttrType::TypeInt, 4}};

const directory_t RelationManager::MAX_SCHEMA_VER = INT16_MAX;

RelationManager &RelationManager::instance() {
  static RelationManager _relation_manager = RelationManager();
  return _relation_manager;
}

RelationManager::RelationManager() : rbfm_(&RecordBasedFileManager::instance()), max_tid_(-1), init_(false) {
  // should do nothing here
}

RelationManager::~RelationManager() = default;

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

RC RelationManager::createCatalog() {
  mkdir("../files", S_IRUSR | S_IWUSR | S_IXUSR);
  loadDbIfExist();
  if (ifDBExists()) {
    return -1;
  }

  rbfm_->createFile(getTableFileName(TABLE_CATALOG_NAME_, true));
  rbfm_->createFile(getTableFileName(COLUMN_CATALOG_NAME_, true));
  table_files_[TABLE_CATALOG_NAME_] = getTableFileName(TABLE_CATALOG_NAME_, true);
  table_files_[COLUMN_CATALOG_NAME_] = getTableFileName(COLUMN_CATALOG_NAME_, true);
  table_schema_[TABLE_CATALOG_NAME_].push_back(TABLE_CATALOG_DESC_);
  table_schema_[COLUMN_CATALOG_NAME_].push_back(COLUMN_CATALOG_DESC_);
  table_ids_[TABLE_CATALOG_NAME_] = ++max_tid_;
  table_ids_[COLUMN_CATALOG_NAME_] = ++max_tid_;
  system_tables_ = {TABLE_CATALOG_NAME_, COLUMN_CATALOG_NAME_};

  createTableImpl(TABLE_CATALOG_NAME_, TABLE_CATALOG_DESC_, true);
  createTableImpl(COLUMN_CATALOG_NAME_, COLUMN_CATALOG_DESC_, true);

  init_ = true;
  return 0;
}

RC RelationManager::deleteCatalog() {
  loadDbIfExist();
  if (!ifDBExists()) return -1;
  for (auto &f : system_tables_) {
    if (rbfm_->destroyFile(table_files_.at(f)) != 0)
      return -1;
  }
  table_files_.clear();
  table_schema_.clear();
  table_ids_.clear();
  system_tables_.clear();
  max_tid_ = -1;
  return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
  loadDbIfExist();
  if (!ifDBExists() || ifTableExists(tableName)) return -1;
  if (system_tables_.count(tableName)) return -1;
  return createTableImpl(tableName, attrs);
}

RC RelationManager::deleteTable(const std::string &tableName) {
  DB_DEBUG << "deleting table `" << tableName << "`";
  loadDbIfExist();
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  if (system_tables_.count(tableName)) {
    DB_ERROR << "Your're not allowed to delete system table";
    return -1;
  }

  char buffer[PAGE_SIZE] = {0};
  // select from TABLE_C_N where tableId == tableId
  RM_ScanIterator rmsi;
  int tid = table_ids_.at(tableName);
  scan(TABLE_CATALOG_NAME_, "table-id", EQ_OP, &tid, {"table-id"}, rmsi);
  RID tab_rid;
  if (rmsi.getNextTuple(tab_rid, buffer) == RM_EOF) {
    DB_ERROR << "Can not find table named `" << tableName << "`";
    return -1;
  }
  rmsi.close();

  if (deleteTupleImpl(TABLE_CATALOG_NAME_, tab_rid, true) != 0) {
    return -1;
  }

  DB_DEBUG << "Delete table `" << tableName << "` with tid " << tid << " rid <" << tab_rid.pageNum << ","
           << tab_rid.slotNum << "> done";

  // select from TABLE_C_N where tableId == tableId
  RM_ScanIterator rmsi2;
  std::vector<RID> cols_to_delete;
  scan(COLUMN_CATALOG_NAME_, "table-id", EQ_OP, &tid, {"table-id"}, rmsi2);
  RID col_rid;
  while (rmsi2.getNextTuple(col_rid, buffer) != RM_EOF) {
    cols_to_delete.push_back(col_rid);
  }
  rmsi2.close();
  for (auto &col_to_delete: cols_to_delete) {
    if (deleteTupleImpl(COLUMN_CATALOG_NAME_, col_to_delete, true) != 0) {
      return -1;
    }
    DB_DEBUG << "Delete column " << " with rid <" << col_to_delete.pageNum << "," << col_to_delete.slotNum << "> done";
  }

  if (rbfm_->destroyFile(table_files_[tableName])) return -1;
  table_schema_.erase(tableName);
  table_files_.erase(tableName);
  table_ids_.erase(tableName);
  return 0;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
  loadDbIfExist();
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  attrs = table_schema_.at(tableName).back();
  return 0;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
  loadDbIfExist();
  return insertTupleImpl(tableName, data, rid);
}

RC RelationManager::insertTupleImpl(const std::string &tableName, const void *data, RID &rid, bool is_system) {
  if (!is_system && system_tables_.count(tableName)) {
    DB_ERROR << "Not allowed to insert Tuple in system table " << tableName;
    return -1;
  }
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  const auto &recordDescriptor = table_schema_.at(tableName).back();
  directory_t cur_ver = table_schema_.at(tableName).size() - 1;
  FileHandle fh;
  rbfm_->openFile(table_file, fh);
  RC ret = rbfm_->insertRecordImpl(fh, recordDescriptor, data, rid, cur_ver);
  rbfm_->closeFile(fh);
  return ret;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
  loadDbIfExist();
  return deleteTupleImpl(tableName, rid);
}

RC RelationManager::deleteTupleImpl(const std::string &tableName, const RID &rid, bool is_system) {
  if (!is_system && system_tables_.count(tableName)) {
    DB_ERROR << "Not allowed to delete tuple in system table `" << tableName << "`";
    return -1;
  }
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  const auto &recordDescriptor = table_schema_.at(tableName).back(); // actually we don't need schema when deleting
  FileHandle fh;
  rbfm_->openFile(table_file, fh);
  RC ret = rbfm_->deleteRecord(fh, recordDescriptor, rid);
  rbfm_->closeFile(fh);
  return ret;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
  loadDbIfExist();
  return updateTupleImpl(tableName, data, rid);
}

RC RelationManager::updateTupleImpl(const std::string &tableName, const void *data, const RID &rid, bool is_system) {
  if (!is_system && system_tables_.count(tableName)) {
    DB_ERROR << "Not allowed to update Tuple in system table " << tableName;
    return -1;
  }
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  // when update, we don't need old schema, we only need to calculate the old size
  const auto &recordDescriptor = table_schema_.at(tableName).back();
  directory_t cur_ver = table_schema_.at(tableName).size() - 1;
  FileHandle fh;
  rbfm_->openFile(table_file, fh);
  RC ret = rbfm_->updateRecordImpl(fh, recordDescriptor, data, rid, cur_ver);
  rbfm_->closeFile(fh);
  return ret;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
  loadDbIfExist();
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  const auto &recordDescriptors = table_schema_.at(tableName);
  FileHandle fh;
  rbfm_->openFile(table_file, fh);
  RC ret = rbfm_->readRecordImpl(fh, recordDescriptors, rid, data, {});
  rbfm_->closeFile(fh);
  return ret;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
  return rbfm_->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
  loadDbIfExist();
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  const auto &recordDescriptors = table_schema_.at(tableName);
  FileHandle fh;
  rbfm_->openFile(table_file, fh);
  RC ret = rbfm_->readRecordImpl(fh, recordDescriptors, rid, data, {attributeName});
  rbfm_->closeFile(fh);
  return ret;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
  loadDbIfExist();
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  const auto &table_file = table_files_.at(tableName);
  const auto &schemas = table_schema_.at(tableName);
  const std::vector<Attribute> &cur_schema = schemas.back();
  if (compOp != CompOp::NO_OP
      && std::find_if(cur_schema.begin(), cur_schema.end(), [&](const Attribute &attr) {
        return attr.name == conditionAttribute;
      }) == cur_schema.end()) {
    DB_ERROR << "Condition attribute `" << conditionAttribute << "` not found in table `" << tableName << "`";
    return -1;
  }
  rbfm_->openFile(table_file, rm_ScanIterator.file_handle_);
  RC ret = rm_ScanIterator.rbfm_scan_iterator_.init(rm_ScanIterator.file_handle_,
                                                    rbfm_,
                                                    schemas,
                                                    conditionAttribute,
                                                    compOp,
                                                    value,
                                                    attributeNames);

  return ret;
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
  loadDbIfExist();
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  if (system_tables_.count(tableName)) return -1;
  std::vector<Attribute> cur_schema = table_schema_.at(tableName).back();
  auto it = std::find_if(cur_schema.begin(),
                         cur_schema.end(),
                         [&](const Attribute &attr) { return attr.name == attributeName; });
  if (it == cur_schema.end()) {
    DB_ERROR << "Drop attribute `" << attributeName << "` in table `" << tableName << "` failed, attribute not exist!";
    return -1;
  }
  cur_schema.erase(it);
  return createTableImpl(tableName, cur_schema);
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
  loadDbIfExist();
  if (!ifDBExists() || !ifTableExists(tableName)) return -1;
  if (system_tables_.count(tableName)) return -1;
  std::vector<Attribute> cur_schema = table_schema_.at(tableName).back();
  auto it = std::find_if(cur_schema.begin(),
                         cur_schema.end(),
                         [&](const Attribute &a) { return a.name == attr.name; });
  if (it != cur_schema.end()) {
    DB_ERROR << "Add attribute `" << attr.name << "` in table `" << tableName << "` failed, attribute already exist!";
    return -1;
  }
  cur_schema.push_back(attr);
  return createTableImpl(tableName, cur_schema);
}

RC RelationManager::createTableImpl(const std::string &tableName,
                                    const std::vector<Attribute> &attrs,
                                    bool is_system_table) {
  directory_t ver = 0; // by default, system table will always has one version
  if (!is_system_table) {
    // for system table, these must be created before hand
    ver = table_schema_[tableName].size();
    if (ver == MAX_SCHEMA_VER) {
      DB_ERROR << "exceed schema max version " << MAX_SCHEMA_VER;
      return -1;
    }
    table_schema_[tableName].push_back(attrs);
    if (!ver) {
      table_files_[tableName] = getTableFileName(tableName, is_system_table);
      rbfm_->createFile(table_files_[tableName]);
      table_ids_[tableName] = ++max_tid_;
      DB_DEBUG << "Create table `" << tableName << "` with tid " << max_tid_;
    }
  }
  if (!ver) {
    // only create table at version 0
    auto table_data = makeTableRecord(tableName, is_system_table);
    RID tbl_id;
    if (insertTupleImpl(TABLE_CATALOG_NAME_, table_data.data(), tbl_id, true) != 0) return -1;
  }

  for (int i = 0; i < attrs.size(); ++i) {
    auto column_data = makeColumnRecord(tableName, i, ver, attrs[i]);
    RID col_id;
    if (insertTupleImpl(COLUMN_CATALOG_NAME_, column_data.data(), col_id, true) != 0) return -1;
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

//  DB_DEBUG << "table record length of: " << table_name << " is " << table_record_length;
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
                                                    const int ver,
                                                    Attribute attr) {
  int null_indicator_length = int(ceil(double(6) / 8));
  unsigned column_record_length = null_indicator_length; // null indicator
  column_record_length += sizeof(int); // ver
  column_record_length += sizeof(int); // table id
  column_record_length += sizeof(int) + attr.name.size(); // column name
  column_record_length += 4 * sizeof(int); // column type, length, position, ver


//  DB_DEBUG << "column record length of: " << table_name << "," << attr.name << " is " << column_record_length;
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
  memcpy(column_record.data() + offset, attr.name.c_str(), attr.name.size());
  offset += attr.name.size();
  // col type
  int col_type = attr.type;
  memcpy(column_record.data() + offset, &col_type, sizeof(int));
  offset += sizeof(int);
  // col len
  memcpy(column_record.data() + offset, &(attr.length), sizeof(int));
  offset += sizeof(int);
  // col pos
  memcpy(column_record.data() + offset, &idx, sizeof(int));
  offset += sizeof(int);
  // ver
  memcpy(column_record.data() + offset, &ver, sizeof(int));
  return column_record;

}

void RelationManager::parseCatalog() {
  // parse Table.catalog
  std::unordered_map<int, std::string> id_tables_map;
  FileHandle fh_table;
  rbfm_->openFile(getTableFileName(TABLE_CATALOG_NAME_, true), fh_table);
  RBFM_ScanIterator table_scan_iterator;
  std::vector<std::string> table_projected_fields;
  for (auto &desc:TABLE_CATALOG_DESC_) table_projected_fields.push_back(desc.name);
  rbfm_->scan(fh_table,
              TABLE_CATALOG_DESC_, "", NO_OP, nullptr,
              table_projected_fields,
              table_scan_iterator);
  RID rid;
  char buffer[PAGE_SIZE];
  while (table_scan_iterator.getNextRecord(rid, buffer) != RBFM_EOF) {
    int offset = sizeof(char); // null indicator
    int table_id = *(buffer + offset);
    offset += sizeof(int);
    int tab_name_len = *(buffer + offset);
    offset += sizeof(int);
    std::string tab_name(buffer + offset, tab_name_len);
    offset += tab_name_len;
    int file_name_len = *(buffer + offset);
    offset += sizeof(int);
    std::string file_name(buffer + offset, file_name_len);
    offset += file_name_len;
    table_ids_[tab_name] = table_id;
    table_files_[tab_name] = file_name;
    if (id_tables_map.count(table_id)) {
      DB_ERROR << "conflict table id " << table_id;
      throw std::runtime_error("Parse schema error");
    }
    id_tables_map[table_id] = tab_name;
    max_tid_ = std::max(max_tid_, table_id);
    int isSys = *(buffer + offset);
    if (isSys == SYSTEM_FLAG)
      system_tables_.insert(tab_name);
  }
  table_scan_iterator.close();
  fh_table.closeFile();

  // parse Column.catalog

  // unordered_map<tid, map<ver, vector<<col_pos, attr>>>>
  std::unordered_map<int, std::map<int, std::vector<std::pair<int, Attribute>>>> cols_by_tid;

  FileHandle fh_col;
  rbfm_->openFile(getTableFileName(COLUMN_CATALOG_NAME_, true), fh_col);
  RBFM_ScanIterator col_scan_iterator;
  std::vector<std::string> col_projected_field;
  for (auto &desc: COLUMN_CATALOG_DESC_) col_projected_field.push_back(desc.name);
  rbfm_->scan(fh_col,
              COLUMN_CATALOG_DESC_, "", NO_OP, nullptr,
              col_projected_field,
              col_scan_iterator);
  while (col_scan_iterator.getNextRecord(rid, buffer) != RBFM_EOF) {
    Attribute col;
    int offset = sizeof(char); // skip null indicator
    int table_id = *(buffer + offset);
    offset += sizeof(int);
    int attr_name_len = *(buffer + offset);
    offset += sizeof(int);
    col.name = std::string(buffer + offset, attr_name_len);
    offset += attr_name_len;
    col.type = static_cast<AttrType> (*(buffer + offset));
    offset += sizeof(int);
    col.length = *(buffer + offset);
    offset += sizeof(int);
    int attr_pos = *(buffer + offset);
    offset += sizeof(int);
    int ver = *(buffer + offset);
    cols_by_tid[table_id][ver].emplace_back(attr_pos, col);
  }
  col_scan_iterator.close();
  fh_col.closeFile();

  // store parsed info to table_schema_
  std::unordered_set<int> visited;
  for (std::pair<const int, std::map<int, std::vector<std::pair<int, Attribute>>>> &table:cols_by_tid) {
    auto tid = table.first;
    if (!id_tables_map.count(tid)) {
      DB_ERROR << "tid " << tid << " not exist!";
      throw std::runtime_error("Parse schema error");
    }
    std::string table_name = id_tables_map.at(table.first);
    visited.insert(tid);
    if (table.second.rbegin()->first + 1 != table.second.size()) {
      DB_ERROR << "table `" << id_tables_map.at(tid) << "` version number not match, max_ver "
               << table.second.rbegin()->first << ", should be " << (table.second.size() - 1);
      throw std::runtime_error("table version number not match");
    }
    for (auto &ver:table.second) {
      auto &cols = ver.second;
      std::sort(cols.begin(),
                cols.end(),
                [](const std::pair<int, Attribute> &p1, const std::pair<int, Attribute> &p2) {
                  return p1.first < p2.first;
                });
      if (cols.back().first != cols.size() - 1) {
        DB_ERROR << "max col pos " << cols.back().first << " while cols.size() == " << cols.size();
        throw std::runtime_error("Parse schema error");
      }
      table_schema_[table_name].emplace_back();
      std::vector<Attribute> &schema = table_schema_[table_name].back();
      for (auto &col : cols)
        schema.push_back(col.second);
    }
  }
  for (auto &kv : id_tables_map)
    if (!visited.count(kv.first)) {
      DB_ERROR << "table id " << kv.first << " not found in cols";
      throw std::runtime_error("Parse schema error");
    }
}

void RelationManager::printTables() {
  loadDbIfExist();
  static const std::unordered_map<AttrType, std::string, EnumHash> TypeNameMap = {
      {AttrType::TypeInt, "TypeInt"},
      {AttrType::TypeReal, "TypeReal"},
      {AttrType::TypeVarChar, "TypeVarChar"}
  };
  std::map<int, std::string> id_to_tables;
  for (auto &kv :table_ids_) id_to_tables[kv.second] = kv.first;
  // print sorted by tid
  for (auto &t : id_to_tables) {
    DB_INFO << "Table: " << t.second;
    auto &vers = table_schema_.at(t.second);
    for (int v = 0; v < vers.size(); ++v) {
      DB_INFO << "version " << v;
      for (auto &attr : vers[v]) {
        DB_INFO << "    " << attr.name << "\t" << TypeNameMap.at(attr.type) << "\t" << attr.length;
      }
    }
  }
}

RC RM_ScanIterator::close() {
  int ret = file_handle_.closeFile();
  if (!ret) return ret;
  return rbfm_scan_iterator_.close();
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
  return rbfm_scan_iterator_.getNextRecord(rid, data);
}
