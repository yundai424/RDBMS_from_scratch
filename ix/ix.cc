#include <sstream>

#include "ix.h"

IndexManager &IndexManager::instance() {
  static IndexManager _index_manager = IndexManager();
  return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {
  return -1;
}

RC IndexManager::destroyFile(const std::string &fileName) {
  return -1;
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
  return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
  return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
  return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
  return -1;
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
  return -1;
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
  return -1;
}

RC IX_ScanIterator::close() {
  return -1;
}

IXFileHandle::IXFileHandle() {
  readPageCounter = 0;
  writePageCounter = 0;
  appendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
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

  // flush new counters to metadata
  _file.seekp(0);
  _file.write((char *) &readPageCounter, sizeof(unsigned));
  _file.write((char *) &writePageCounter, sizeof(unsigned));
  _file.write((char *) &appendPageCounter, sizeof(unsigned));

  // flush pages free space to metadata at tail
  if (meta_modified_) {
    int page_num = getNumberOfPages();
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

  std::shared_ptr<IXPage> cur_page = std::make_shared<IXPage>(appendPageCounter++);
  pages[cur_page->pid] = cur_page;

  return {0, cur_page.get()};
}

unsigned IXFileHandle::getNumberOfPages() {
  return appendPageCounter;
}

std::pair<RC, IXPage *> IXFileHandle::getPage(int pid) {
  if (pid >= getNumberOfPages()) {
    DB_WARNING << "page id " << pid << " out of range";
    return {-1, nullptr};
  }
  if (!pages.count(pid)) {
    auto cur_page = std::make_shared<IXPage>(pid);
    cur_page->data = static_cast<char *>(malloc(PAGE_SIZE));
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
const size_t IXPage::DEFAULT_DATA_END = sizeof(int);

IXPage::IXPage(PID page_id) : pid(page_id), data(nullptr), modify(false) {}

IXPage::~IXPage() {
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

void Key::serialize(char *dst) const {
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

std::string Key::ToString() const {
  std::string val_str;
  switch (key_type) {
    case AttrType::TypeInt:val_str = std::to_string(i);
      break;
    case AttrType::TypeReal:val_str = std::to_string(f);
      break;
    case AttrType::TypeVarChar: val_str = s;
      break;
  }
  return "<" + val_str + "," + std::to_string(page_num) + "," + std::to_string(slot_num) + ">";
}

bool Key::operator<(const Key &rhs) const {
  if (rhs.key_type != key_type) throw std::runtime_error("compare different type of key!");
  switch (key_type) {
    case AttrType::TypeInt:return i < rhs.i;
      break;
    case AttrType::TypeReal:return f < rhs.f;
      break;
    case AttrType::TypeVarChar: return s < rhs.s;
      break;
  }
  return false;
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

Node *Node::getRight() {
  return right_pid == INVALID_PID ? nullptr : btree->getNode(right_pid).second;
}

Node *Node::getChild(int idx) {
  return btree->getNode(children_pids[idx]).second;
}

void Node::setRight(int pid) {
  right_pid = pid;
}

Node::Node(BPlusTree *tree_ptr, IXPage *meta_p, bool is_leaf)
    : btree(tree_ptr), meta_page(meta_p), pid(meta_p->pid), leaf(is_leaf) {

}

Node::Node(BPlusTree *tree_ptr, IXPage *meta_p) : btree(tree_ptr), meta_page(meta_p), pid(meta_p->pid) {
  /*
   * ******************************************************************
   * meta page:
   * int page_type : 0 -> meta page, 1 -> data page
   * int key_type : 0 -> int, 1 -> float, 2 -> varchar
   * int is_leaf : 0 -> non-leaf, 1 -> leaf
   * int right_pid : -1 means null
   * int M : order of tree
   * int m : current entry number, in range [M, 2M], which means current children number will be m + 1
   * 2 * M * (int + int + int) : position of keys <PID, offset, size>
   * (2 * M + 1) * int : children meta page
   *
   * ******************************************************************
   */
  int *pt = (int *) meta_page->data;
  ++pt;
  int key_type_val = *pt++;
  if (key_type_val > 2) throw std::runtime_error("unrecognized key type " + std::to_string(key_type_val));
  btree->key_type = static_cast<AttrType>(key_type_val);
  leaf = (*pt++) == 1;
  right_pid = *pt++;
  btree->M = *pt++;
  int m = *pt++;
  entries.clear();
  int page_id, offset, size;
  for (int i = 0; i < m; ++i) {
    page_id = *(pt + i * 3 + 0);
    offset = *(pt + i * 3 + 1);
    size = *(pt + i * 3 + 2);
    IXPage *page = btree->file_handle_->getPage(page_id).second;
    entries.emplace_back(Key(btree->key_type, page->data + offset), std::make_shared<Data>(0));
  }
  pt += 2 * btree->M * 3;
  if (leaf) {
    for (int i = 0; i < m + 1; ++i) {
      children_pids.emplace_back(*pt++);
    }
  }
}

std::string Node::toString() const {
  std::ostringstream oss;
  oss << "[";
  for (int j = 0; j < entries.size(); ++j) {
    oss << entries[j].first.ToString();
    if (j != entries.size() - 1) oss << ",";
  }
  oss << "]";
  return oss.str();
}

const int BPlusTree::ROOT_PID = 0;

BPlusTree::BPlusTree(int order, IXFileHandle &file_handle)
    : file_handle_(&file_handle), M(order), MAX_ENTRY(order * 2) {}

BPlusTree::BPlusTree(IXFileHandle &file_handle) : file_handle_(&file_handle) {
  // load from file
  file_handle_->getPage(ROOT_PID);
}

RC BPlusTree::insert(const Key &key, std::shared_ptr<Data> data) {
  if (!root_) {
    auto ret = createNode(true);
    if (!ret.first) return -1;
    root_ = ret.second;
    root_->entries.emplace_back(key, data);
    return 0;
  }
  std::vector<std::pair<Node *, int>> path{{root_, 0}};
  std::pair<int, bool> res = search(key, path);
  Node *node = path.back().first;
  int idx_in_parent;
  int entry_idx = res.first;
  if (res.second) {
    // key exist, replace data
    node->entries[entry_idx].second = data;
  } else {
    // create key, insert to entry_idx
    node->entries.insert(node->entries.begin() + entry_idx, {key, data});
    if (node->entries.size() > MAX_ENTRY) {
      // split: [0,M], [M+1, 2M+1], and copy_up/push_up M+1 key to parent, recursively split up
      Node *cur_node = node;
      while (cur_node->entries.size() > MAX_ENTRY) {
        idx_in_parent = path.back().second;
        path.pop_back();
        Node *parent = path.empty() ? nullptr : path.back().first;

        std::deque<Node::data_t> &entries = cur_node->entries;
        auto ret = createNode(cur_node->leaf);
        Node *new_node = ret.second;
        // split entries
        Key mid_key = entries[M].first;
        if (cur_node->leaf) {
          // copy_up key and split data
          new_node->entries.insert(new_node->entries.end(), entries.begin() + M, entries.end());
          entries.resize(M);
        } else {
          // push_up key and split data/children
          new_node->entries.insert(new_node->entries.end(), entries.begin() + M + 1, entries.end());
          entries.resize(M);
          std::deque<int> &children = cur_node->children_pids;
          new_node->children_pids.insert(new_node->children_pids.end(), children.begin() + M + 1, children.end());
          children.resize(M + 1);
        }
        // modify right
        new_node->setRight(cur_node->right_pid);
        cur_node->setRight(new_node->pid);
        // pull mid_key up to parent
        if (!parent) {
          // split root
          auto ret = createNode(false);
          if (!ret.first) {
            DB_WARNING << "failed to create new root";
            return -1;
          }
          Node *new_root = ret.second;
          new_root->children_pids.push_back(root_->pid);
          root_ = new_root;
          parent = new_root;
        }
        parent->children_pids.insert(parent->children_pids.begin() + idx_in_parent + 1, new_node->pid);
        parent->entries.insert(parent->entries.begin() + idx_in_parent, {mid_key, nullptr});
        cur_node = parent;
      }
    }
  }
}

bool BPlusTree::erase(const Key &key) {
  if (!root_) return false;
  std::vector<std::pair<Node *, int>> path{{root_, 0}};
  auto res = search(key, path);
  if (!res.second) return false;
  Node *node = path.back().first;
  int idx = res.first;
  node->entries.erase(node->entries.begin() + idx);
  while (node != root_ && node->entries.size() < M) {
    // borrow from sibling or merge
    int idx_in_parent = path.back().second;
    path.pop_back();
    Node *parent = path.back().first;
    Node *left, *right;
    bool borrow_from_right = true;
    int left_idx;
    if (idx_in_parent != parent->children_pids.size() - 1) {
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
      if (right->entries.size() > M) {
        if (node->leaf) {
          left->entries.push_back(right->entries.front());
          right->entries.pop_front();
          parent->entries[left_idx] = {right->entries.front().first, nullptr};
        } else {
          left->children_pids.push_back(right->children_pids.front());
          right->children_pids.pop_front();
          left->entries.push_back(parent->entries[left_idx]);
          parent->entries[left_idx] = right->entries.front();
          right->entries.pop_front();
        }
      } else
        merge = true;
    } else {
      if (left->entries.size() > M) {
        if (node->leaf) {
          right->entries.push_front(left->entries.back());
          left->entries.pop_back();
          parent->entries[left_idx] = {right->entries.front().first, nullptr};
        } else {
          right->children_pids.push_front(left->children_pids.back());
          left->children_pids.pop_back();
          right->entries.push_front(parent->entries[left_idx]);
          parent->entries[left_idx] = left->entries.back();
          left->entries.pop_back();
        }
      } else
        merge = true;
    }
    if (merge) {
      //delete right
      // merge
      if (!node->leaf) {
        left->entries.push_back(parent->entries[left_idx]);
        left->children_pids.insert(left->children_pids.end(), right->children_pids.begin(), right->children_pids.end());
      }
      left->entries.insert(left->entries.end(), right->entries.begin(), right->entries.end());

      left->setRight(right->right_pid);
      // delete from parent
      parent->entries.erase(parent->entries.begin() + left_idx);
      parent->children_pids.erase(parent->children_pids.begin() + left_idx + 1);
      if (parent->entries.empty()) {
        // delete old root
        assert(parent->children_pids.front() == left->pid);
        root_ = parent->getChild(0);
        break;
      }
    }
    node = parent;
  }
  return true;
}

bool BPlusTree::find(const Key &key) {
  if (!root_) return false;
  std::vector<std::pair<Node *, int>> path{{root_, 0}};
  return search(key, path).second;
}

RC BPlusTree::bulkLoad(std::vector<Node::data_t> entries) {
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
  // build leaf layer
  bool last = false;
  for (int i = 0; i < entries.size();) {
    int j = std::min(i + MAX_ENTRY, int(entries.size()));
    int step = MAX_ENTRY;
    auto ret = createNode(true);
    if (ret.first) return -1;
    Node *new_node = ret.second;
    prev_layer.push_back(new_node);
    if (!last && i + 2 * MAX_ENTRY >= entries.size() && i + MAX_ENTRY < entries.size()) {
      // last two bunch, we need to split average to avoid last node element < M
      step = (entries.size() - i) / 2;
      j = i + step;
      last = true;
    }
    new_node->entries.insert(new_node->entries.end(), entries.begin() + i, entries.begin() + j);
    i += step;
  }
  entries.resize(prev_layer.size());
  for (int i = 1; i < prev_layer.size(); ++i) {
    entries[i] = {prev_layer[i]->entries.front().first, nullptr};
  }
  // build index layer

  while (prev_layer.size() > 1) {
    // connect layer from left to right
    for (int i = 1; i < prev_layer.size(); ++i) {
      prev_layer[i - 1]->setRight(prev_layer[i]->pid);
    }
    // build cur layer using entries
    bool last = false;
    for (int i = 0; i < entries.size();) {
      int j = std::min(i + MAX_ENTRY + 1, int(entries.size()));
      int step = 1 + MAX_ENTRY;
      auto ret = createNode(false);
      if (!ret.first) return -1;
      Node *new_node = ret.second;
      cur_layer.push_back(new_node);
      // we will drop the first key, leaving MAX_ENTRY key and MAX_ENTRY + 1 children
      if (!last && i + 2 * (MAX_ENTRY + 1) >= entries.size() && i + MAX_ENTRY + 1 < entries.size()) {
        // last two bunch, we need to split average to avoid last node element < M
        step = (entries.size() - i) / 2;
        j = i + step;
        last = true;
      }
      new_node->entries.insert(new_node->entries.end(),
                               entries.begin() + i + 1,
                               entries.begin() + j);
      for (int k = i; k < j; ++k) {
        new_node->children_pids.push_back(prev_layer[k]->pid);
      }
      i += step;
    }

    entries.resize(cur_layer.size());
    for (int k = 0; k < cur_layer.size(); ++k) {
      entries[k] = {cur_layer[k]->entries.front().first, nullptr};
    }
    prev_layer = std::move(cur_layer);
    cur_layer = {};

  }
  root_ = prev_layer.front();
}

std::pair<int, bool> BPlusTree::search(Key key, std::vector<std::pair<Node *, int>> &path) {
  // binary search
  Node *node = path.back().first;
  int low = 0, high = node->entries.size() - 1;
  while (low < high) {
    int mid = low + (high - low) / 2;
    if (key < node->entries[mid].first) {
      high = mid;
    } else if (node->entries[mid].first < key) {
      low = mid + 1;
    } else {
      low = mid;
      break;
    }
  }
  // if not found, low point to the first element which is greater than key
  // if key is greater than all elements, low = entries.size() - 1, should handle this corner case
  if (node->leaf) {
    if (node->entries[low].first == key) {
      return {low, true};
    } else {
      if (low == node->entries.size() - 1 && node->entries.back().first < key) {
        return {low + 1, false};
      } else {
        return {low, false};
      }
    }
  } else {
    if (node->entries[low].first == key) {
      path.emplace_back(node->getChild(low + 1), low + 1);
    } else {
      if (low == node->entries.size() - 1 && node->entries.back().first < key) {
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
  std::shared_ptr<Node> new_node = std::make_shared<Node>(this, new_page, leaf);
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
    nodes_[pid] = std::make_shared<Node>(this, ret.second);
  }
  return {0, nodes_[pid].get()};
}

void BPlusTree::printEntries() const {
  std::vector<Key> keys;
  Node *node = root_;
  while (!node->leaf) node = node->getChild(0);
  while (node) {
    for (auto &data : node->entries) keys.push_back(data.first);
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
      for (int j = 0; j < node->children_pids.size(); ++j) {
        q.push_back(node->getChild(j));
      }
      if (!node->leaf) q.push_back(nullptr);
    }
    std::cout << std::endl;
  }

}

