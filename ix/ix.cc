#include <sstream>
#include <set>

#include "ix.h"

IndexManager &IndexManager::instance() {
  static IndexManager _index_manager = IndexManager();
  return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {
  IXFileHandle handler;
  return handler.createFile(fileName);
}

RC IndexManager::destroyFile(const std::string &fileName) {
  if (!PagedFileManager::ifFileExists(fileName)) {
    DB_WARNING << "try to delete non-exist file " << fileName;
    return -1;
  }
  return remove(fileName.c_str());
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
  return ixFileHandle.openFile(fileName);
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
  return ixFileHandle.closeFile();
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
  auto tree = BPlusTree::createTreeOrLoadIfExist(ixFileHandle, attribute);
  if (!tree) {
    DB_WARNING << "fail to load tree!";
    return -1;
  }
  Key k(attribute.type, static_cast<const char *>(key), rid);
  return tree->insert(k, nullptr);
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
  auto tree = BPlusTree::createTreeOrLoadIfExist(ixFileHandle, attribute);
  if (!tree) {
    DB_WARNING << "fail to load tree!";
    return -1;
  }
  Key k(attribute.type, static_cast<const char *>(key), {0, 0});
  return (!tree->erase(k));
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
  return ix_ScanIterator.initIterator(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::initIterator(IXFileHandle &ixFileHandle,
                                 const Attribute &attribute,
                                 const void *lowKey,
                                 const void *highKey,
                                 bool lowKeyInclusive,
                                 bool highKeyInclusive) {
  auto tree = BPlusTree::createTreeOrLoadIfExist(ixFileHandle, attribute);
  if (!tree) return -1;
  btree = tree;
  // for low key, we use RID = {0,0} to workaround range scan (and carefully handle inclusive case)
  low_key = lowKey ? std::make_shared<Key>(attribute.type, static_cast<const char *>(lowKey), RID{0, 0}) : nullptr;
  // for height key, RID doesn't matter, just set to {0,0} here
  high_key = highKey ? std::make_shared<Key>(attribute.type, static_cast<const char *>(highKey), RID{0, 0}) : nullptr;
  low_inclusive = lowKeyInclusive;
  high_inclusive = highKeyInclusive;
  std::pair<Node *, int>
      start_position = lowKey ? tree->find(*low_key) : std::pair<Node *, int>{tree->getFirstLeaf(), 0};
  node = start_position.first;
  idx = start_position.second;
  if (!low_inclusive && low_key) {
    while (checkCurPos() && node->entriesConst().at(idx).first.cmpKeyVal(*low_key) == 0) {
      moveNext();
    }
  }
  init = true;
  return 0;
}

bool IX_ScanIterator::checkCurPos() {
  if (node && idx == node->entriesConst().size()) {
    // reach end of node
    node = node->getRight();
    idx = 0;
  }
  if (!node) {
    // reach EOF
    return false;
  }
  return true;
}

void IX_ScanIterator::moveNext() {
  ++idx;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
  if (!init) {
    DB_WARNING << "IX_ScanIterator already reach IX_EOF or not initialized";
    return IX_EOF;
  }
  if (!checkCurPos()) {
    init = false;
    return IX_EOF;
  }

  Key k = node->entriesConst().at(idx).first;
  if (high_key) {
    bool eof = false;
    if (high_inclusive) {
      if (high_key->cmpKeyVal(k) < 0) eof = true;
    } else {
      if (high_key->cmpKeyVal(k) <= 0) eof = true;
    }
    if (eof) {
      init = false;
      return IX_EOF;
    }
  }

  // write output
  rid.pageNum = k.page_num;
  rid.slotNum = k.slot_num;
  k.fetchKey(static_cast<char *>(key));

  moveNext();
  return 0;
}

RC IX_ScanIterator::close() {
  btree = nullptr;
  init = false;
  node = nullptr;
  idx = -1;
  low_key = nullptr;
  high_key = nullptr;
  return 0;
}

IXFileHandle::IXFileHandle() {
  readPageCounter = 0;
  writePageCounter = 0;
  appendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
}

BPlusTree *IXFileHandle::getTree(Attribute attr) {
  if (!tree) tree = BPlusTree::createTreeOrLoadIfExist(*this, attr);
  return tree.get();
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
  readPageCount = readPageCounter;
  writePageCount = writePageCounter;
  appendPageCount = appendPageCounter;
  return 0;
}

RC IXFileHandle::openFile(const std::string &fileName) {
  if (!PagedFileManager::ifFileExists(fileName)) {
//    DB_WARNING << "try to open non-exist file " << fileName;
    return -1;
  }

  if (_file.is_open()) {
    DB_WARNING << "file " << name << "already open!";
    return -1;
  }

  _file.open(fileName, std::ios::in | std::ios::out | std::ios::binary);
  if (!_file.good()) {
//    DB_WARNING << "failed to open file " << fileName;
    return -1;
  }
  _file.seekg(0, std::ios::end);
  name = fileName;
  meta_modified_ = false;
  free_pages.clear();
  pages.clear();

  // load counter from metadata
  _file.seekg(0);
  _file.read((char *) &readPageCounter, sizeof(unsigned));
  _file.read((char *) &writePageCounter, sizeof(unsigned));
  _file.read((char *) &appendPageCounter, sizeof(unsigned));

  // load free space for each page
  // meta pages store free space are always appended at the end
  int num_pages = getNumberOfPages();
  _file.seekg(getPos(num_pages));
  int free_page_nums;
  int free_page_id;
  _file.read((char *) (&free_page_nums), sizeof(int));
  for (int i = 0; i < free_page_nums; ++i) {
    _file.read((char *) (char *) (&free_page_id), sizeof(int));
    free_pages.insert(free_page_id);
  }

  return 0;
}

RC IXFileHandle::closeFile() {
  if (!_file.is_open()) {
//    DB_WARNING << "File not opened.";
    return -1;
  }
  pages.clear(); // then all pages will destruct and dump to disk

  // flush new counters to metadata
  _file.seekp(0);
  char meta_page[PAGE_SIZE];
  memcpy(meta_page, &readPageCounter, sizeof(unsigned));
  memcpy(meta_page + 1 * sizeof(unsigned), &writePageCounter, sizeof(unsigned));
  memcpy(meta_page + 2 * sizeof(unsigned), &appendPageCounter, sizeof(unsigned));
  _file.write(meta_page, PAGE_SIZE);

  // flush pages free space to metadata at tail
  int page_num = getNumberOfPages();
  if (meta_modified_ || page_num == 0) {

    _file.seekp(getPos(page_num));
    int free_page_nums = free_pages.size();
    _file.write((const char *) (&free_page_nums), sizeof(int));
    for (int pid : free_pages) {
      _file.write((const char *) (&pid), sizeof(int));
    }
  }

  _file.close();
  return 0;
}

RC IXFileHandle::createFile(const std::string &fileName) {

  if (PagedFileManager::ifFileExists(fileName)) {
//    DB_WARNING << "File " << fileName << " exist!";
    return -1;
  }

  if (_file.is_open())
    return -1;

  _file.open(fileName, std::ios::out | std::ios::binary);
  if (!_file.good()) {
//    DB_WARNING << "failed to create file " << fileName;
    return -1;
  }
  // write counters as metadata to head of file
  return closeFile();
}

RC IXFileHandle::readPage(PageNum pageNum, void *data) {
  // pageNum exceed total number of pages
  if (pageNum >= getNumberOfPages() || !_file.is_open())
    return -1;
  _file.seekg(getPos(pageNum));
  _file.read((char *) data, PAGE_SIZE);
  readPageCounter++;
  return 0;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
  if (pageNum >= getNumberOfPages() || !_file.is_open())
    return -1;
  meta_modified_ = true;
  _file.seekp(getPos(pageNum));
  _file.write((char *) data, PAGE_SIZE);
  writePageCounter++;
  return 0;
}

std::pair<RC, IXPage *> IXFileHandle::appendPage() {
  if (!_file.is_open()) {
//    DB_WARNING << "File is not opened!";
    return {-1, nullptr};
  }
  meta_modified_ = true;
  _file.seekp(getPos(appendPageCounter)); // this will overwrite the tailing meta pages
  char data[PAGE_SIZE];
  _file.write((char *) data, PAGE_SIZE);

  std::shared_ptr<IXPage> cur_page = std::make_shared<IXPage>(appendPageCounter++, this);
  pages[cur_page->pid] = cur_page;

  return {0, cur_page.get()};
}

unsigned IXFileHandle::getNumberOfPages() {
  return appendPageCounter;
}

std::pair<RC, IXPage *> IXFileHandle::getPage(int pid) {
  if (pid >= getNumberOfPages() || pid < 0) {
    DB_WARNING << "page id " << pid << " out of range";
    return {-1, nullptr};
  }
  if (!pages.count(pid)) {
    auto cur_page = std::make_shared<IXPage>(pid, this);
    readPage(pid, cur_page->data);
    pages[pid] = cur_page;
  }
  return {0, pages[pid].get()};
}

std::pair<RC, IXPage *> IXFileHandle::requestNewPage() {
  if (free_pages.empty()) {
    return appendPage();
  } else {
    int free_pid = *free_pages.begin();
    free_pages.erase(free_pid);
    return getPage(free_pid);
  }
}

RC IXFileHandle::releasePage(IXPage *page) {
  if (free_pages.count(page->pid)) {
    DB_WARNING << "page " << page->pid << " already release";
    return -1;
  }
  page->modify = false;
  free_pages.insert(page->pid);
  return 0;
}

const size_t IXPage::MAX_DATA_SIZE = PAGE_SIZE - sizeof(int);
const size_t IXPage::DEFAULT_DATA_BEGIN = sizeof(int);

IXPage::IXPage(PID page_id, IXFileHandle *handle) : pid(page_id), data(nullptr), modify(false), file_handle(handle) {
  data = static_cast<char *>(malloc(PAGE_SIZE));
}

const char *IXPage::dataConst() const {
  return data;
}

char *IXPage::dataNonConst() {
  modify = true;
  return data;
}

RC IXPage::dump() {
  if (modify) {
    DB_INFO << "dump IXPage " << pid;
    DB_DEBUG << print_bytes(data, 50);
    return file_handle->writePage(pid, data);
  }
  return 0;
}

IXPage::~IXPage() {
  dump();
  if (data) free(data);
}

/**********************************************************
 *  B+Tree
 **********************************************************/

Key::Key(AttrType key_type_, const char *key_val, RID rid)
    : key_type(key_type_), page_num(rid.pageNum), slot_num(rid.slotNum) {
  switch (key_type) {
    case AttrType::TypeInt:i = *((const int *) key_val);
      break;
    case AttrType::TypeReal:f = *((const float *) key_val);
      break;
    case AttrType::TypeVarChar:int str_len = *((const int *) key_val);
      s = std::string(key_val + sizeof(int), key_val + sizeof(int) + str_len);
      break;
  }
}

Key::Key(AttrType key_tpye_, const char *src) : key_type(key_tpye_) {
  switch (key_type) {
    case AttrType::TypeInt:i = *((const int *) src);
      src += sizeof(int);
      break;
    case AttrType::TypeReal:f = *((const float *) src);
      src += sizeof(float);
      break;
    case AttrType::TypeVarChar:int str_len = *((const int *) src);
      s = std::string(src + sizeof(int), src + sizeof(int) + str_len);
      src += str_len + sizeof(int);
      break;
  }
  page_num = *((const int *) src);
  src += sizeof(int);
  slot_num = *((const int *) src);
}

int Key::getSize() const {
  switch (key_type) {
    case AttrType::TypeInt:return sizeof(int) + 2 * sizeof(int);

    case AttrType::TypeReal:return sizeof(float) + 2 * sizeof(int);

    case AttrType::TypeVarChar:return sizeof(int) + s.size() + 2 * sizeof(int);
  }
  return -1;
}

void Key::dump(char *dst) const {
  switch (key_type) {
    case AttrType::TypeInt:memcpy(dst, &i, sizeof(int));
      dst += sizeof(int);
      break;
    case AttrType::TypeReal:memcpy(dst, &f, sizeof(float));
      dst += sizeof(float);
      break;
    case AttrType::TypeVarChar:int str_len = s.size();
      memcpy(dst, &str_len, sizeof(int));
      dst += sizeof(int);
      memcpy(dst, s.data(), str_len);
      dst += str_len;
      break;
  }
  memcpy(dst, &page_num, sizeof(int));
  dst += sizeof(int);
  memcpy(dst, &slot_num, sizeof(int));
}

void Key::fetchKey(char *dst) const {
  switch (key_type) {
    case AttrType::TypeInt:memcpy(dst, &i, sizeof(int));

      break;
    case AttrType::TypeReal:memcpy(dst, &f, sizeof(float));
      break;
    case AttrType::TypeVarChar:int str_len = s.size();
      memcpy(dst, &str_len, sizeof(int));
      dst += sizeof(int);
      memcpy(dst, s.data(), str_len);
      break;
  }
  return;
}

std::string Key::ToString(bool key_val_only) const {
  std::string val_str;
  switch (key_type) {
    case AttrType::TypeInt:val_str = std::to_string(i);
      break;
    case AttrType::TypeReal:val_str = std::to_string(f);
      break;
    case AttrType::TypeVarChar: val_str = s;
      break;
  }
  if (key_val_only) return val_str;
  else
    return "<" + val_str + "," + std::to_string(page_num) + "," + std::to_string(slot_num) + ">";
}

int Key::cmpKeyVal(const Key &rhs) const {
  switch (key_type) {
    case AttrType::TypeInt: return i - rhs.i;
      break;
    case AttrType::TypeReal: {
      auto tmp = f - rhs.f;
      if (tmp < 0) return -1;
      else if (tmp > 0) return 1;
      else return 0;
    }
      break;
    case AttrType::TypeVarChar: return s.compare(rhs.s);
      break;
  }
  return 0;
}

bool Key::operator<(const Key &rhs) const {
  if (rhs.key_type != key_type) throw std::runtime_error("compare different type of key!");
  int res = 0;
  switch (key_type) {
    case AttrType::TypeInt:res = i - rhs.i;
      break;
    case AttrType::TypeReal:res = f - rhs.f;
      break;
    case AttrType::TypeVarChar: res = s.compare(rhs.s);
      break;
  }
  if (res == 0) return std::tie(page_num, slot_num) < std::tie(rhs.page_num, slot_num);
  else return res < 0 ? true : false;
}

bool Key::operator==(const Key &rhs) const {
  if (rhs.key_type != key_type) throw std::runtime_error("compare different type of key!");
  switch (key_type) {
    case AttrType::TypeInt:return i == rhs.i;
      break;
    case AttrType::TypeReal:return f == rhs.f;
      break;
    case AttrType::TypeVarChar: return s == rhs.s;
      break;
  }
  return false;
}

const int Node::INVALID_PID = -1;

bool Node::isLeaf() const {
  return leaf;
}
int Node::getPid() const {
  return pid;
}

int Node::getRightPid() const {
  return right_pid;
}

Node *Node::getRight() {
  return right_pid == INVALID_PID ? nullptr : btree->getNode(right_pid).second;
}

Node *Node::getChild(int idx) {
  return btree->getNode(children_pids.at(idx)).second;
}

void Node::setRightPid(int r_pid) {
  modified = true;
  right_pid = r_pid;
}

std::deque<int> &Node::childrenPidsNonConst() {
  modified = true;
  return children_pids;
}

const std::deque<int> &Node::childrenPidsConst() const {
  return children_pids;
}

std::deque<Node::data_t> &Node::entriesNonConst() {
  modified = true;
  return entries;
}

const std::deque<Node::data_t> &Node::entriesConst() const {
  return entries;
}

Node::Node(BPlusTree *tree_ptr, IXPage *meta_p) : btree(tree_ptr), meta_page(meta_p), pid(meta_p->pid), modified(true) {

}

RC Node::loadFromPage() {
  /*******************************************************************
   * 1. `node meta page`
   * int page_type : 0 -> meta page, 1 -> data page
   * int is_leaf : 0 -> non-leaf, 1 -> leaf
   * int right_pid : -1 means null
   * int m : current entry number, in range [M, 2M]
   * 2 * M * (int + int + int) : position of keys <PID, offset, size>
   * (2 * M + 1) * int : children meta page
   *
   * ******************************************************************
   * 2. `data page`
   * int page_type : 0 -> meta page, 1 -> data page
   * following data...
   *
   */
  const int *pt = (int *) meta_page->dataConst();
  ++pt;
  leaf = (*pt++) == 1;
  right_pid = *pt++;
  int m = *pt++;
  entries.clear();
  // load data entries
  int page_id, offset, size;
  std::set<IXPage *> data_pages_set;
  for (int i = 0; i < m; ++i) {
    page_id = *(pt + i * 3 + 0);
    offset = *(pt + i * 3 + 1);
    size = *(pt + i * 3 + 2);
    auto ret = btree->file_handle_->getPage(page_id);
    if (ret.first) return -1;
    IXPage *page = ret.second;
    data_pages_set.insert(page);
    entries.emplace_back(Key(btree->key_attr.type, page->dataConst() + offset), std::make_shared<Data>(0));
  }
  data_pages = {data_pages_set.begin(), data_pages_set.end()};
  // load children pids
  pt += 2 * btree->M * 3;
  if (!leaf) {
    children_pids.clear();
    for (int i = 0; i < m + 1; ++i) {
      children_pids.emplace_back(*pt++);
    }
  }
  return 0;
}

std::shared_ptr<Node> Node::createNode(BPlusTree *tree_ptr, IXPage *meta_p, bool is_leaf) {
  auto node = std::make_shared<Node>(tree_ptr, meta_p);
  node->leaf = is_leaf;
  node->modified = true;
  return node;
}

std::shared_ptr<Node> Node::loadNodeFromPage(BPlusTree *tree_ptr, IXPage *meta_p) {
  auto node = std::make_shared<Node>(tree_ptr, meta_p);
  if (node->loadFromPage()) return nullptr;
  node->modified = false;
  return node;
}

RC Node::dumpToPage() {
  if (!modified) return 0;
  DB_INFO << "dump node " << pid << " with " << entries.size() << " entries " << toString();
  // write data page
  int cur_page_idx = -1;
  int free_space = 0;
  int offset = -1;
  char *data_pt = nullptr;

  std::vector<std::tuple<int, int, int>> entry_pos; // pid, offset, size

  for (auto &entry : entries) {
    int entry_size = entry.first.getSize();
    if (entry_size > free_space) {
      // next page
      ++cur_page_idx;
      if (cur_page_idx == data_pages.size()) {
        auto ret = btree->file_handle_->requestNewPage();
        if (ret.first) return -1;
        data_pages.emplace_back(ret.second);
      }
      IXPage *next_page = data_pages[cur_page_idx];
      data_pt = next_page->dataNonConst();
      *((int *) data_pt) = 1; // type1 `data page`
      data_pt += sizeof(int);
      offset = IXPage::DEFAULT_DATA_BEGIN;
      free_space = IXPage::MAX_DATA_SIZE;
    }
    entry.first.dump(data_pt);
    data_pt += entry_size;
    free_space -= entry_size;
    entry_pos.emplace_back(data_pages[cur_page_idx]->pid, offset, entry_size);
    offset += entry_size;
  }

  for (int i = cur_page_idx + 1; i < data_pages.size(); ++i) {
    btree->file_handle_->releasePage(data_pages[i]);
  }
  data_pages.resize(cur_page_idx + 1);
//  std::ostringstream oss;
//  oss << "[";
//  for (auto p : data_pages) oss << p->pid << ",";
//  oss << "]";
//  DB_DEBUG << "data pages:" << oss.str();
  // write meta page

  /*******************************************************************
   * 1. `node meta page`
   * int page_type : 0 -> meta page, 1 -> data page
   * int is_leaf : 1 -> non-leaf, 0 -> non-leaf
   * int right_pid : -1 means null
   * int m : current entry number, in range [M, 2M]
   * 2 * M * (int + int + int) : position of keys <PID, offset, size>
   * (2 * M + 1) * int : children meta page
   *
   * ******************************************************************
   * 2. `data page`
   * int page_type : 0 -> meta page, 1 -> data page
   * following data...
   *
   */
  int *pt = (int *) meta_page->dataNonConst();
  *pt++ = 0; // type0 `meta page`
  *pt++ = leaf ? 1 : 0;
  *pt++ = right_pid;
  *pt++ = entries.size();
  for (int i = 0; i < entries.size(); ++i) {
    *(pt + i * 3 + 0) = std::get<0>(entry_pos[i]);
    *(pt + i * 3 + 1) = std::get<1>(entry_pos[i]);
    *(pt + i * 3 + 2) = std::get<2>(entry_pos[i]);
  }
  pt += 2 * btree->M * 3;
  for (int children_pid : children_pids) {
    *pt++ = children_pid;
  }
  return 0;
}

std::string Node::toString() const {
  std::ostringstream oss;
  oss << "pid " << pid << ":{";
  for (auto c : children_pids) oss << c << ";";
  oss << "}\t";
  oss << "[";
  for (int j = 0; j < entries.size(); ++j) {
    oss << entries[j].first.ToString();
    if (j != entries.size() - 1) oss << ",";
  }
  oss << "]";
  return oss.str();
}

const int BPlusTree::TREE_META_PID = 0;
const int BPlusTree::DEFAULT_ORDER_M = 100;

std::unordered_map<std::string, std::shared_ptr<BPlusTree>> BPlusTree::global_index_map;

BPlusTree::BPlusTree(IXFileHandle &file_handle) : file_handle_(&file_handle), modified(true), root_(nullptr) {}

RC BPlusTree::initTree() {
  auto ret = file_handle_->requestNewPage();
  if (ret.first) {
    DB_WARNING << "Failed to init tree";
    return -1;
  }
  meta_page = ret.second;
  return 0;
}

RC BPlusTree::loadFromFile() {
  nodes_.clear();
  modified = false;
  /*******************************************************************
   * 0. `tree meta page` (which will always be the first page
   * int root_page : node meta page num for root, -1 if tree empty
   * int M : order of tree
   * int key_type : 0 -> int, 1 -> float, 2 -> varchar
   * varchar attr_name : attr used for index, format is varchar (int + string)
   * ******************************************************************
   */
  auto ret = file_handle_->getPage(TREE_META_PID);
  if (ret.first) {
    DB_WARNING << "failed to load B+tree from file";
    return -1;
  }
  meta_page = ret.second;
  const int *pt = (const int *) meta_page->dataConst();
  int root_pid = *pt++;
  M = *pt++;
  int key_type_val = *pt++;
  if (key_type_val > 2) {
    DB_WARNING << "unrecognized key type " << std::to_string(key_type_val);
    return -1;
  }
  key_attr.type = static_cast<AttrType>(key_type_val);
  int str_len = *pt++;
  const char *char_pt = (const char *) pt;
  key_attr.name = std::string(char_pt, char_pt + str_len);

  auto ret2 = getNode(root_pid);
  if (ret2.first) {
    DB_WARNING << "failed to load root node";
    return -1;
  }
  root_ = ret2.second;
  return 0;
}

std::shared_ptr<BPlusTree> BPlusTree::createTreeOrLoadIfExist(IXFileHandle &file_handle, Attribute attr) {
//  if (!global_index_map.count(file_handle.name)) {
//    std::shared_ptr<BPlusTree> tree;
//    if (file_handle.getNumberOfPages()) {
//      // exist
//      tree = loadTreeFromFile(file_handle, attr);
//    } else {
//      tree = createTree(file_handle, DEFAULT_ORDER_M, attr);
//    }
//    global_index_map[file_handle.name] = tree;
//  }
//  return global_index_map[file_handle.name];
  std::shared_ptr<BPlusTree> tree;
  if (file_handle.getNumberOfPages()) {
    // exist
    tree = loadTreeFromFile(file_handle, attr);
  } else {
    tree = createTree(file_handle, DEFAULT_ORDER_M, attr);
  }
  return tree;
}

std::shared_ptr<BPlusTree> BPlusTree::createTree(IXFileHandle &file_handle, int order, Attribute attr) {
  auto tree = std::make_shared<BPlusTree>(file_handle);
  if (tree->initTree()) return nullptr;
  tree->key_attr = attr;
  tree->M = order;
  tree->modified = true;
  return tree;
}

std::shared_ptr<BPlusTree> BPlusTree::loadTreeFromFile(IXFileHandle &file_handle, Attribute attr) {
  auto tree = std::make_shared<BPlusTree>(file_handle);
  if (tree->loadFromFile()) return nullptr;
  if (tree->key_attr.name != attr.name) {
    DB_WARNING << "Key attr name unmatched! given `" << attr.name << "` but got `" << tree->key_attr.name
               << "` from disk";
    return nullptr;
  }
  if (tree->key_attr.type != attr.type) {
    DB_WARNING << "Key attr type unmatched! given `" << attr.type << "` but got `" << tree->key_attr.type
               << "` from disk";
    return nullptr;
  }
  tree->modified = false;
  return tree;
}

RC BPlusTree::dumpToFile() {
  if (!modified) return 0;
  DB_INFO << "dump B+tree of key `" << key_attr.name << "` and root "
          << (root_ ? std::to_string(root_->getPid()) : "empty");
  // dump meta page
  /*******************************************************************
   * 0. `tree meta page` (which will always be the first page
   * int root_page : node meta page num for root, -1 if tree empty
   * int M : order of tree
   * int key_type : 0 -> int, 1 -> float, 2 -> varchar
   * varchar attr_name : attr used for index, format is varchar (int + string)
   * ******************************************************************
   */
  auto ret = file_handle_->getPage(TREE_META_PID);
  if (ret.first) {
    DB_WARNING << "failed to load B+tree from file";
    return -1;
  }
  IXPage *tree_meta_page = ret.second;
  int *pt = (int *) tree_meta_page->dataNonConst();
  *pt++ = root_ ? root_->getPid() : -1;
  *pt++ = M;
  int key_type_val = static_cast<int>(key_attr.type);
  *pt++ = key_type_val;
  *pt++ = key_attr.name.size();
  memcpy(pt, key_attr.name.data(), key_attr.name.size());
  // dump nodes
  for (auto &kv : nodes_) {
    if (kv.second->dumpToPage()) {
      DB_WARNING << "Failed to dump node with pid " << kv.first;
      return -1;
    }
  }
  return 0;
}

RC BPlusTree::insert(const Key &key, std::shared_ptr<Data> data) {
  modified = true;
  if (!root_) {
    auto ret = createNode(true);
    if (ret.first) return -1;
    root_ = ret.second;
    root_->entriesNonConst().emplace_back(key, data);
    return 0;
  }
  std::vector<std::pair<Node *, int>> path{{root_, 0}};
  std::pair<int, bool> res = search(key, path);
  Node *node = path.back().first;
  int idx_in_parent;
  int entry_idx = res.first;
  if (res.second) {
    // key exist, replace data
    node->entriesNonConst()[entry_idx].second = data;
  } else {
    // create key, insert to entry_idx
    auto &node_entries = node->entriesNonConst();
    node_entries.insert(node_entries.begin() + entry_idx, {key, data});
    if (node_entries.size() > MAX_ENTRY()) {
      // split: [0,M], [M+1, 2M+1], and copy_up/push_up M+1 key to parent, recursively split up
      Node *cur_node = node;
      while (cur_node->entriesConst().size() > MAX_ENTRY()) {
        idx_in_parent = path.back().second;
        path.pop_back();
        Node *parent = path.empty() ? nullptr : path.back().first;

        std::deque<Node::data_t> &entries = cur_node->entriesNonConst();
        auto ret = createNode(cur_node->isLeaf());
        Node *new_node = ret.second;
        // split entries
        Key mid_key = entries[M].first;
        auto &new_node_entries = new_node->entriesNonConst();
        if (cur_node->isLeaf()) {
          // copy_up key and split data
          new_node_entries.insert(new_node_entries.end(), entries.begin() + M, entries.end());
          entries.resize(M);
        } else {
          // push_up key and split data/children
          new_node_entries.insert(new_node_entries.end(), entries.begin() + M + 1, entries.end());
          entries.resize(M);
          std::deque<int> &cur_children = cur_node->childrenPidsNonConst();
          std::deque<int> &new_children = new_node->childrenPidsNonConst();
          new_children.insert(new_children.end(), cur_children.begin() + M + 1, cur_children.end());
          cur_children.resize(M + 1);
        }
        // modify right
        new_node->setRightPid(cur_node->getRightPid());
        cur_node->setRightPid(new_node->getPid());
        // pull mid_key up to parent
        if (!parent) {
          // split root
          auto ret = createNode(false);
          if (ret.first) {
            DB_WARNING << "failed to create new root";
            return -1;
          }
          Node *new_root = ret.second;
          new_root->childrenPidsNonConst().push_back(root_->getPid());
          root_ = new_root;
          parent = new_root;
        }
        auto &parent_children = parent->childrenPidsNonConst();
        auto &parent_entires = parent->entriesNonConst();
        parent_children.insert(parent_children.begin() + idx_in_parent + 1, new_node->getPid());
        parent_entires.insert(parent_entires.begin() + idx_in_parent, {mid_key, nullptr});
        cur_node = parent;
      }
    }
  }
  return 0;
}

bool BPlusTree::erase(const Key &key) {
  modified = true;
  if (!root_) return false;
  std::vector<std::pair<Node *, int>> path{{root_, 0}};
  auto res = search(key, path);
  if (!res.second) return false;
  Node *node = path.back().first;
  int idx = res.first;
  node->entriesNonConst().erase(node->entriesNonConst().begin() + idx);
  while (node != root_ && node->entriesConst().size() < M) {
    // borrow from sibling or merge
    int idx_in_parent = path.back().second;
    path.pop_back();
    Node *parent = path.back().first;
    Node *left, *right;
    bool borrow_from_right = true;
    int left_idx;
    if (idx_in_parent != parent->childrenPidsConst().size() - 1) {
      left_idx = idx_in_parent;
      left = node;
      right = node->getRight();
    } else {
      left_idx = idx_in_parent - 1;
      left = parent->getChild(left_idx);
      right = node;
      borrow_from_right = false;
    }
    bool merge = false;
    if (borrow_from_right) {
      if (right->entriesConst().size() > M) {
        if (node->isLeaf()) {
          left->entriesNonConst().push_back(right->entriesConst().front());
          right->entriesNonConst().pop_front();
          parent->entriesNonConst()[left_idx] = {right->entriesConst().front().first, nullptr};
        } else {
          left->childrenPidsNonConst().push_back(right->childrenPidsConst().front());
          right->childrenPidsNonConst().pop_front();
          left->entriesNonConst().push_back(parent->entriesConst().at(left_idx));
          parent->entriesNonConst()[left_idx] = right->entriesConst().front();
          right->entriesNonConst().pop_front();
        }
      } else
        merge = true;
    } else {
      if (left->entriesConst().size() > M) {
        if (node->isLeaf()) {
          right->entriesNonConst().push_front(left->entriesConst().back());
          left->entriesNonConst().pop_back();
          parent->entriesNonConst()[left_idx] = {right->entriesConst().front().first, nullptr};
        } else {
          right->childrenPidsNonConst().push_front(left->childrenPidsConst().back());
          left->childrenPidsNonConst().pop_back();
          right->entriesNonConst().push_front(parent->entriesConst().at(left_idx));
          parent->entriesNonConst()[left_idx] = left->entriesConst().back();
          left->entriesNonConst().pop_back();
        }
      } else
        merge = true;
    }
    if (merge) {
      //delete right
      // merge
      if (!node->isLeaf()) {
        left->entriesNonConst().push_back(parent->entriesConst().at(left_idx));
        auto &left_children_nc = left->childrenPidsNonConst();
        const auto &right_children_c = right->childrenPidsConst();
        left_children_nc.insert(left_children_nc.end(), right_children_c.begin(), right_children_c.end());
      }
      auto &left_entries_nc = left->entriesNonConst();
      const auto &right_entires_c = right->entriesConst();
      left_entries_nc.insert(left_entries_nc.end(), right_entires_c.begin(), right_entires_c.end());

      left->setRightPid(right->getRightPid());
      deleteNode(right->getPid());
      // delete from parent
      parent->entriesNonConst().erase(parent->entriesNonConst().begin() + left_idx);
      parent->childrenPidsNonConst().erase(parent->childrenPidsNonConst().begin() + left_idx + 1);
      if (parent->entriesConst().empty()) {
        // delete old root
        assert(parent->childrenPidsConst().front() == left->getPid());
        root_ = parent->getChild(0);
        deleteNode(parent->getPid());
        break;
      }
    }
    node = parent;
  }
  if (node == root_ && node->entriesConst().empty()) {
    // delete root
    deleteNode(root_->getPid());
    root_ = nullptr;
  }
  return true;
}

bool BPlusTree::contains(const Key &key) {
  if (!root_) return false;
  std::vector<std::pair<Node *, int>> path{{root_, 0}};
  return search(key, path).second;
}

std::pair<Node *, int> BPlusTree::find(const Key &key) {
  if (!root_) return {nullptr, -1};
  std::vector<std::pair<Node *, int>> path{{root_, 0}};
  auto ret = search(key, path);
  return {path.back().first, ret.first};
}

Node *BPlusTree::getFirstLeaf() const {
  if (!root_) {
    DB_WARNING << "tree empty!";
    return nullptr;
  }
  Node *node = root_;
  while (!node->isLeaf()) node = node->getChild(0);
  return node;
}

RC BPlusTree::bulkLoad(std::vector<Node::data_t> entries) {
  modified = true;
  if (root_) {
    DB_WARNING << "can only bulkLoad when tree is empty!";
    return -1;
  }
  if (entries.empty()) {
    DB_WARNING << "entries empty";
    return -1;
  }
  std::sort(entries.begin(), entries.end());
  std::vector<Node *> cur_layer, prev_layer;
  std::vector<std::pair<Key, Key>> cur_node_range, prev_node_range;
  // build leaf layer
  bool last = false;
  for (int i = 0; i < entries.size();) {
    int j = std::min(i + MAX_ENTRY(), int(entries.size()));
    int step = MAX_ENTRY();
    auto ret = createNode(true);
    if (ret.first) return -1;
    Node *new_node = ret.second;
    prev_layer.push_back(new_node);
    if (!last && i + 2 * MAX_ENTRY() >= entries.size() && i + MAX_ENTRY() < entries.size()) {
      // last two bunch, we need to split average to avoid last node element < M
      step = (entries.size() - i) / 2;
      j = i + step;
      last = true;
    }
    new_node->entriesNonConst().insert(new_node->entriesNonConst().end(), entries.begin() + i, entries.begin() + j);
    prev_node_range.emplace_back(new_node->entriesConst().front().first, new_node->entriesConst().back().first);
    i += step;
  }
  entries.resize(prev_layer.size());
  for (int i = 1; i < prev_layer.size(); ++i) {
    entries[i] = {prev_node_range[i].first, nullptr};
  }
  // build index layer

  while (prev_layer.size() > 1) {
    // connect layer from left to right
    for (int i = 1; i < prev_layer.size(); ++i) {
      prev_layer[i - 1]->setRightPid(prev_layer[i]->getPid());
    }
    // build cur layer using entries
    bool last = false;
    for (int i = 0; i < entries.size();) {
      int j = std::min(i + MAX_ENTRY() + 1, int(entries.size()));
      int step = 1 + MAX_ENTRY();
      auto ret = createNode(false);
      if (ret.first) return -1;
      Node *new_node = ret.second;
      // we will drop the first key, leaving MAX_ENTRY key and MAX_ENTRY + 1 children
      if (!last && i + 2 * (MAX_ENTRY() + 1) >= entries.size() && i + MAX_ENTRY() + 1 < entries.size()) {
        // last two bunch, we need to split average to avoid last node element < M
        step = (entries.size() - i) / 2;
        j = i + step;
        last = true;
      }
      new_node->entriesNonConst().insert(new_node->entriesNonConst().end(),
                                         entries.begin() + i + 1,
                                         entries.begin() + j);
      for (int k = i; k < j; ++k) {
        new_node->childrenPidsNonConst().push_back(prev_layer[k]->getPid());
      }
      cur_layer.push_back(new_node);
      cur_node_range.emplace_back(prev_node_range[i].first, prev_node_range[j].second);
      i += step;
    }

    entries.resize(cur_layer.size());
    for (int k = 0; k < cur_layer.size(); ++k) {
      entries[k] = {cur_node_range[k].first, nullptr};
    }
    prev_layer = std::move(cur_layer);
    cur_layer = {};
    prev_node_range = std::move(cur_node_range);
    cur_node_range = {};

  }
  root_ = prev_layer.front();
  return 0;
}

std::pair<int, bool> BPlusTree::search(Key key, std::vector<std::pair<Node *, int>> &path) {
  // binary search
  Node *node = path.back().first;
  int low = 0, high = node->entriesConst().size() - 1;
  while (low < high) {
    int mid = low + (high - low) / 2;
    if (key < node->entriesConst()[mid].first) {
      high = mid;
    } else if (node->entriesConst()[mid].first < key) {
      low = mid + 1;
    } else {
      low = mid;
      break;
    }
  }
  // if not found, low point to the first element which is greater than key
  // if key is greater than all elements, low = entries.size() - 1, should handle this corner case
  if (node->isLeaf()) {
    if (node->entriesConst().at(low).first == key) {
      return {low, true};
    } else {
      if (low == node->entriesConst().size() - 1 && node->entriesConst().back().first < key) {
        return {low + 1, false};
      } else {
        return {low, false};
      }
    }
  } else {
    if (node->entriesConst().at(low).first == key) {
      path.emplace_back(node->getChild(low + 1), low + 1);
    } else {
      if (low == node->entriesConst().size() - 1 && node->entriesConst().back().first < key) {
        path.emplace_back(node->getChild(low + 1), low + 1);
      } else {
        path.emplace_back(node->getChild(low), low);
      }
    }
    return search(key, path);
  }
}

std::pair<RC, Node *> BPlusTree::createNode(bool leaf) {
  auto ret = file_handle_->requestNewPage();
  if (ret.first) {
    DB_WARNING << "failed to request new page";
    return {-1, nullptr};
  }
  IXPage *new_page = ret.second;
  std::shared_ptr<Node> new_node = Node::createNode(this, new_page, leaf);
  nodes_[new_page->pid] = new_node;
  return {0, new_node.get()};
}

std::pair<RC, Node *> BPlusTree::getNode(int pid) {
  if (!nodes_.count(pid)) {
    auto ret = file_handle_->getPage(pid);
    if (ret.first) {
      DB_WARNING << "failed to get page " << pid;
      return {-1, nullptr};
    }
    nodes_[pid] = Node::loadNodeFromPage(this, ret.second);
  }
  return {0, nodes_[pid].get()};
}

RC BPlusTree::deleteNode(int pid) {
  if (!nodes_.count(pid)) {
    DB_WARNING << "deleteNode failed! pid " << pid << " not exist!";
    return -1;
  }
  return 0;
}

void BPlusTree::printEntries() const {
  if (!root_) {
    DB_WARNING << "empty tree";
    return;
  }
  std::vector<Key> keys;
  Node *node = getFirstLeaf();
  while (node) {
    for (auto &data : node->entriesConst()) keys.push_back(data.first);
    node = node->getRight();
  }
  for (auto &k : keys) std::cout << k.ToString() << ", ";
  std::cout << std::endl;
}

void BPlusTree::printTree() const {
  std::deque<Node *> q{root_};
  while (!q.empty()) {
    int sz = q.size();
    for (int i = 0; i < sz; ++i) {
      Node *node = q.front();
      q.pop_front();
      if (!node) {
        if (i != sz - 1) std::cout << "|\t";
        continue;
      }
      std::cout << node->toString() << "\t";
      for (int j = 0; j < node->childrenPidsConst().size(); ++j) {
        q.push_back(node->getChild(j));
      }
      if (!node->isLeaf()) q.push_back(nullptr);
    }
    std::cout << std::endl;
  }

}

BPlusTree::~BPlusTree() {
  dumpToFile();
}

