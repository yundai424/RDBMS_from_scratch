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

class IX_ScanIterator {
 public:

  // Constructor
  IX_ScanIterator();

  // Destructor
  ~IX_ScanIterator();

  // Get next matching entry
  RC getNextEntry(RID &rid, void *key);

  // Terminate index scan
  RC close();
};

struct IXPage;

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

  std::unordered_set<int> free_pages; // some pages might be freed after entry deletion

  // Constructor
  IXFileHandle();

  // Destructor
  ~IXFileHandle();

  // Put the current counter values of associated PF FileHandles into variables
  RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

  std::string name;
  std::unordered_map<int, std::shared_ptr<IXPage>> pages;
  bool meta_modified_;

  RC readPage(PageNum pageNum, void *data);                           // Get a specific page
  RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
  std::pair<RC, IXPage *> appendPage();                                    // Append a specific page
  unsigned getNumberOfPages();                                        // Get the number of pages in the file

  std::pair<RC, IXPage *> getPage(int pid);
  std::pair<RC, IXPage *> requestNewPage();
  RC releasePage(IXPage *page);

  RC createFile(const std::string &fileName);
  RC openFile(const std::string &fileName);
  RC closeFile();

 private:
  static inline size_t getPos(PageNum page_num) {
    return (page_num + 1) * PAGE_SIZE;
  }

  std::fstream _file;
};

/*******************************************************************
 * we define three kind of IxPage, meta page and data page
 *
 * each tree node start with a meta page, followed by several data page
 * ******************************************************************
 * 0. `tree meta page` (which will always be the first page
 * int root_page : node meta page num for root, -1 if tree empty
 * ******************************************************************
 * 1. `node meta page`
 * int page_type : 0 -> meta page, 1 -> data page
 * int key_type : 0 -> int, 1 -> float, 2 -> varchar
 * int is_leaf : 0 -> leaf, 1 -> non-leaf
 * int right_pid : -1 means null
 * int M : order of tree
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

struct IXPage {

  /*
   * for `data page`
   */
  static const size_t MAX_DATA_SIZE;
  static const size_t DEFAULT_DATA_END;

  char *data;

  bool meta;

  bool modify;

  PID pid;

  explicit IXPage(PID page_id);

  ~IXPage();

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

  Key(AttrType key_type_, const char *key_val, RID rid);

  Key(AttrType key_tpye_, const char *src); // deserialize from binary

  int getSize() const;

  void serialize(char *dst) const; // serialize to binary

  std::string ToString() const;

  bool operator<(const Key &rhs) const;
  bool operator==(const Key &rhs) const;
};

class BPlusTree;

class Node {
  static const int INVALID_PID;

  BPlusTree *btree;

  IXPage *meta_page;
  std::vector<IXPage *> data_pages;
  bool modified;

  /*
   * set ctor to private
   * construct node using static function instead to avoid confusion.
   * either construct an empty node in memory, and associate it with a page or construct from disk
   */
  Node(BPlusTree *tree_ptr, IXPage *meta_p);

  RC loadFromPage();

 public:
  typedef std::pair<Key, std::shared_ptr<Data>> data_t; // for index node, Data will be nullptr
  int pid;
  bool leaf;
  int right_pid;
  std::deque<int> children_pids;
  std::deque<data_t> entries; // size = children.size() - 1


  Node *getRight();
  Node *getChild(int idx);
  void setRight(int pid);

  RC dumpToPage();

  std::string toString() const;

  static std::shared_ptr<Node> createNode(BPlusTree *tree_ptr, IXPage *meta_p, bool is_leaf);

  static std::shared_ptr<Node> loadNodeFromPage(BPlusTree *tree_ptr, IXPage *meta_p);

};

class BPlusTree {
  friend class Node;
 private:
  const static int TREE_META_PID;
//  std::vector<std::shared_ptr<Node>> nodes;
  std::unordered_map<int, std::shared_ptr<Node>> nodes_;
  Node *root_;
  IXFileHandle *file_handle_;
  AttrType key_type;
  bool modified;
  int M; // order, # of key should be in range [M, 2M], and # of children should be [M+1, 2M+1]

/**
   * DFS helper function to search for key using binary search
   * @param key
   * @param path DFS path, node and its idx in its parent's children, the last node will always be a leaf node
   * @return <entry_index, found> if found, entry_index point to exactly the position where entry key == key, and found = true
   * else, entry_index point to the first entry larger than key, and found = false
   */
  std::pair<int, bool> search(Key key, std::vector<std::pair<Node *, int>> &path);

  /*
   * set ctor to private
   * use static function to create from memory or load from file
   */
  BPlusTree(IXFileHandle &file_handle);

  RC loadFromFile();

 public:

  int inline MAX_ENTRY() const;

  RC insert(const Key &key, std::shared_ptr<Data> data);
  bool erase(const Key &key);
  bool find(const Key &key);
  RC bulkLoad(std::vector<Node::data_t> entries);

  std::pair<RC, Node *> createNode(bool leaf);
  std::pair<RC, Node *> getNode(int pid);

  RC dumpToFile();

  static std::shared_ptr<BPlusTree> createTree(IXFileHandle &file_handle, int order, AttrType key_type);
  static std::shared_ptr<BPlusTree> loadTreeFromFile(IXFileHandle &file_handle);



  void printEntries() const;
  void printTree() const;

};

int inline BPlusTree::MAX_ENTRY() const {
  return M * 2;
}

#endif
