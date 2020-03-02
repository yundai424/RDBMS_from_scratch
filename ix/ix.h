#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <deque>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;

class IXFileHandle;

class IndexManager {

 public:
  static IndexManager &instance();

  // Create an index file.
  RC createFile(const std::string &fileName);

  // Delete an index file.
  RC destroyFile(const std::string &fileName);

  // Open an index and return an ixFileHandle.
  RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

  // Close an ixFileHandle for an index.
  RC closeFile(IXFileHandle &ixFileHandle);

  // Insert an entry into the given index that is indicated by the given ixFileHandle.
  RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Delete an entry from the given index that is indicated by the given ixFileHandle.
  RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Initialize and IX_ScanIterator to support a range search
  RC scan(IXFileHandle &ixFileHandle,
          const Attribute &attribute,
          const void *lowKey,
          const void *highKey,
          bool lowKeyInclusive,
          bool highKeyInclusive,
          IX_ScanIterator &ix_ScanIterator);

  // Print the B+ tree in pre-order (in a JSON record format)
  void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

 protected:
  IndexManager() = default;                                                   // Prevent construction
  ~IndexManager() = default;                                                  // Prevent unwanted destruction
  IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
  IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

};

class BPlusTree;
class Node;
class Key;
class Context;

class IX_ScanIterator {
 private:

  bool init_ = false;
  bool closed_ = true;

  std::shared_ptr<Context> ctx;

  std::pair<int, int> ptr; // node_pid, entry_idx

  std::shared_ptr<Key> low_key;
  std::shared_ptr<Key> high_key;

  bool low_inclusive;
  bool high_inclusive;

  bool checkCurPos();
  void moveNext();

 public:
  friend class IndexManager;

  // Constructor
  IX_ScanIterator();

  // Destructor
  ~IX_ScanIterator();

  RC init(IXFileHandle &ixFileHandle,
          const Attribute &attribute,
          const void *lowKey,
          const void *highKey,
          bool lowKeyInclusive,
          bool highKeyInclusive);

  // Get next matching entry
  RC getNextEntry(RID &rid, void *key);

  Node *cur_node();

  // Terminate index scan
  RC close();
};

/*
 * LFU cache
 */

struct FrequencyNode;

struct LFUNode {
  int key;
  LFUNode *prev;
  LFUNode *next;
  FrequencyNode *freq_node;
  explicit LFUNode(int k);
};

struct FrequencyNode {
  int freq;
  int size;
  LFUNode *head; // most recent
  LFUNode *tail;
  FrequencyNode *next;
  FrequencyNode *prev;
  explicit FrequencyNode(int f);
  ~FrequencyNode();

  void InsertNode(LFUNode *node);
  void RemoveNode(LFUNode *node);
  std::vector<LFUNode*> RemoveUnrecent(int max_num);
};

class LFUCache {
 private:
  std::unordered_map<int, LFUNode *> nodes;
  std::unordered_set<int> lazy_popout;
  FrequencyNode *head; // most frequent
  FrequencyNode *tail; // most in-frequent
  int cap;
  int size;

  static void RemoveFreqNode(FrequencyNode *node);

  static void InsertFreqNode(FrequencyNode *prev, FrequencyNode *cur);

  static void IncreaseFreq(LFUNode *node);

 public:
  explicit LFUCache(int capacity);

  ~LFUCache();

  bool get(int key);

  void erase(int key);

  std::pair<int, bool> put(int key, bool lazy_popout = false);

  std::vector<int> lazyPopout();

  void setCap(int c);

};

struct IXPage;

class BPlusTree;

class IXFileManager {
  friend class IXFileHandle;
  friend class BPlusTree;
  static const int LFU_CAP;

  // variables to keep counter for each operation
  unsigned readPageCounter;
  unsigned writePageCounter;
  unsigned appendPageCounter;

  LFUCache lfu;
  std::unordered_map<int, std::shared_ptr<IXPage>> pages;
  std::unordered_set<int> free_pages; // some pages might be freed after entry deletion
  std::fstream _file;

  static inline size_t getPos(PageNum page_num) {
    return (page_num + 1) * PAGE_SIZE;
  }

  void popOut(int id);
  RC dumpMeta();
  RC loadMeta();
 public:
  static std::unordered_map<std::string, std::shared_ptr<IXFileManager>> global_map;
  static IXFileManager *getMgr(const std::string &file);
  static void removeMgr(const std::string &file);

  bool meta_modified_;
  std::string name;

  IXFileManager(std::string file);
  ~IXFileManager();

  RC readPage(PageNum pageNum, void *data);                           // Get a specific page
  RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
  std::pair<RC, IXPage *> appendPage();                                    // Append a specific page
  unsigned getNumberOfPages();                                        // Get the number of pages in the file

  std::pair<RC, IXPage *> getPage(int pid);
  std::pair<RC, IXPage *> requestNewPage();
  RC releasePage(int);
  RC init();
  // serialization to disk, should be call after each operation
  RC dumpToFile();
  RC close();

  void updateFileHandle(IXFileHandle &handle) const;

};

/**
 * first page is meta page store three counters
 *
 * following the last data page are also some other meta pages:
 * first 4 bytes(int) indicate numbers of free pages, followed by page ids
 */
class IXFileHandle {

 public:

  // variables to keep counter for each operation
  unsigned readPageCounter;
  unsigned writePageCounter;
  unsigned appendPageCounter;

  bool open_ = false;
  std::string name;
  IXFileManager *mgr;

  // Constructor
  IXFileHandle();

  // Destructor
  ~IXFileHandle();

  // Put the current counter values of associated PF FileHandles into variables
  RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

  RC createFile(const std::string &fileName);
  RC openFile(const std::string &fileName);
  RC closeFile();

  RC updateCounter();
};

/*******************************************************************
 * we define three kind of IxPage, meta page and data page
 *
 * each tree node start with a meta page, followed by several data page
 * ******************************************************************
 * 0. `tree meta page` (which will always be the first page
 * int root_page : node meta page num for root, -1 if tree empty
 * int M : order of tree
 * int key_type : 0 -> int, 1 -> float, 2 -> varchar
 * varchar attr_name : attr used for index, format is varchar (int + string)
 * ******************************************************************
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

class IXPage {
 public:
  friend class IXFileManager;
  friend class IXFileHandle;
  /*
   * for `data page`
   */
  static const size_t MAX_DATA_SIZE;
  static const size_t DEFAULT_DATA_BEGIN;

  bool meta;
  PID pid;

  IXPage(PID page_id, IXFileManager *mgr);
  const char *dataConst() const;
  char *dataNonConst();

  ~IXPage();

 private:
  bool dirty;
  char *data;
  IXFileManager *file_mgr;

  RC dump();

};

struct Data {
  int i;
  inline std::string toString() const {
    return std::to_string(i);
  }
  Data(int val) : i(val) {};
};

struct Key {

  int i;
  float f;
  std::string s;
  AttrType key_type;

  unsigned page_num;
  unsigned slot_num;

  Key() = default;

  Key(AttrType key_type_, const char *key_val, RID rid);

  Key(AttrType key_tpye_, const char *src); // deserialize from binary

  int getSize() const;

  void dump(char *dst) const; // dump the whole key to binary

  void fetchKey(char *dst) const; // write the key val to dst (i, f or s)

  std::string toString(bool key_val_only = true) const;

  int cmpKeyVal(const Key &rhs) const;

  bool operator<(const Key &rhs) const;
  bool operator==(const Key &rhs) const;
};

class BPlusTree;

class Node {

 public:
  typedef std::pair<Key, std::shared_ptr<Data>> data_t; // for index node, Data will be nullptr
  static const int INVALID_PID;
 private:

  BPlusTree *btree;

  bool dirty;
  int pid;
  std::vector<int> data_pages_id;
  bool leaf;
  int right_pid = -1;
  std::deque<int> children_pids;
  std::deque<data_t> entries; // size = children.size() - 1

  IXPage *getMetaPage();
  IXPage *getDataPage(int i);

  RC loadFromPage();
 public:
  // const
  bool isLeaf() const;
  int getPid() const;
  int getRightPid() const;
  Node *getRight();
  Node *getChild(int idx);
  const std::vector<int> &dataPagesId() const;
  const std::deque<int> &childrenPidsConst() const;
  const std::deque<data_t> &entriesConst() const;
  // non const, set modified to true
  void setRightPid(int r_pid);
  std::deque<int> &childrenPidsNonConst();
  std::deque<data_t> &entriesNonConst();

  RC dumpToPage();

  std::string toString() const;

  /*
   * do not use ctor directly
   * construct node using static function instead to avoid confusion.
   * either construct an empty node in memory, and associate it with a page or construct from disk
   */
  Node(BPlusTree *tree_ptr, int meta_pid);

  ~Node();

  static std::shared_ptr<Node> createNode(BPlusTree *tree_ptr, int meta_pid, bool is_leaf);

  static std::shared_ptr<Node> loadNodeFromPage(BPlusTree *tree_ptr, int meta_pid);

};

class BPlusTree {
  friend class Node;
  friend class IX_ScanIterator;
 private:
  const static int TREE_META_PID;
  const static int DEFAULT_ORDER_M;
  const static int LFU_CAP;

  static std::unordered_map<std::string, std::shared_ptr<BPlusTree>> global_map;

  LFUCache lfu;

  IXPage *meta_page;
  std::unordered_map<int, std::shared_ptr<Node>> nodes_;
  int root_pid;
  IXFileManager *mgr;
  Attribute key_attr;
  bool modified;
  int M; // order, # of key should be in range [M, 2M], and # of children should be [M+1, 2M+1]
  std::unordered_set<std::pair<int, int> *> scan_sentry;

  /**
   * DFS helper function to search for key using binary search
   * @param key
   * @param path DFS path, node and its idx in its parent's children, the last node will always be a leaf node
   * @return <entry_index, found> if found, entry_index point to exactly the position where entry key == key, and found = true
   * else, entry_index point to the first entry larger than key, and found = false
   */
  std::pair<int, bool> search(Key key, std::vector<std::pair<Node *, int>> &path);

  RC initTree();
  RC loadFromFile();

  static std::shared_ptr<BPlusTree> createTree(IXFileManager *mgr, int order, Attribute attr);
  static std::shared_ptr<BPlusTree> loadTreeFromFile(IXFileManager *mgr, Attribute attr);

  void popOut(int pid);

  Node *root();

  std::pair<RC, Node *> createNode(bool leaf);
  std::pair<RC, Node *> getNode(int pid);
  RC deleteNode(int pid);

  void sentrySplitToRight(Node *src, Node *dst, int src_begin);
  void sentryMergeFromRight(Node *left, Node *right, int left_size);
  void sentryMergeFromLeft(Node *left, Node *right, int left_size);
  void sentryBorrowFromRight(Node *left, Node *right, int left_size);
  void sentryBorrowFromLeft(Node *left, Node *right, int left_size);
  void sentryDeleteEntry(Node *node, int idx);
  void sentryInsertEntry(Node *node, int idx);

  void printRecursive(Node *node) const;

 public:

  /*
   * do not use ctor directly
   * use static function to create from memory or load from file
   */
  BPlusTree(IXFileManager *mgr);

  static BPlusTree *createTreeOrLoadIfExist(IXFileManager *mgr, const Attribute &attr);
  static void destroyAllTrees();

  int inline MAX_ENTRY() const;

  RC insert(const Key &key, std::shared_ptr<Data> data = nullptr);
  bool erase(const Key &key);
  bool contains(const Key &key);
  void popoutFromCache();

  /**
   *
   * @param key
   * @return Node and idx point to key if exactly matched, otherwise idx point to the first element greater than key
   */
  std::pair<Node *, int> find(const Key &key);
  Node *getFirstLeaf();
  RC bulkLoad(std::vector<Node::data_t> entries);

  void registerScan(std::pair<int, int> *entry);
  void unregisterScan(std::pair<int, int> *entry);

  RC dumpToMgr();

  void printEntries();
  void printTree();

  ~BPlusTree();

};

int inline BPlusTree::MAX_ENTRY() const {
  return M * 2;
}

struct Context {
  /*
   * all members' life cycle must be valid during the life cycle of Context
   */

  static std::pair<RC, std::shared_ptr<Context>> enterCtx(IXFileHandle *file_handle, const Attribute &attr);

  IXFileHandle *file_handle;
  BPlusTree *btree;
  IXFileManager *mgr;

  Context(IXFileManager *mgr, IXFileHandle *file_handle, BPlusTree *btree);
  ~Context();
};

#endif
