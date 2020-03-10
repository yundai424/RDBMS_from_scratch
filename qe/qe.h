#ifndef _qe_h_
#define _qe_h_

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

typedef enum {
  MIN = 0, MAX, COUNT, SUM, AVG
} AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
  AttrType type;          // type of value
  void *data;             // value
};

struct Condition {
  std::string lhsAttr;        // left-hand side attribute
  CompOp op;                  // comparison operator
  bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
  std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
  Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
};

struct Utils {
  /**
   * concatenate two records into one, and write into output
   * @param left_data
   * @param right_data
   * @param left_attrs
   * @param right_attrs
   * @param out
   */
  static void concatRecords(const std::vector<Attribute> &left_attrs,
                            const std::vector<Attribute> &right_attrs,
                            const void *left_data,
                            const void *right_data,
                            void *output);

  /**
   *
   * @param is_left
   * @param data
   * @return <bool is_not_null, string value_as_string>
   */
  static std::pair<bool, Key> parseCondValue(const std::vector<Attribute> &attrs, int pos, const void *data);
};

class Iterator {
  // All the relational operators and access methods are iterators.
 public:
  virtual RC getNextTuple(void *data) = 0;

  virtual void getAttributes(std::vector<Attribute> &attrs) const = 0;

  virtual ~Iterator() = default;;
};

class TableScan : public Iterator {
  // A wrapper inheriting Iterator over RM_ScanIterator
 public:
  RelationManager &rm;
  RM_ScanIterator *iter;
  std::string tableName;
  std::vector<Attribute> attrs;
  std::vector<std::string> attrNames;
  RID rid{};

  TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
    //Set members
    this->tableName = tableName;

    // Get Attributes from RM
    rm.getAttributes(tableName, attrs);

    // Get Attribute Names from RM
    for (Attribute &attr : attrs) {
      // convert to char *
      attrNames.push_back(attr.name);
    }

    // Call RM scan to get an iterator
    iter = new RM_ScanIterator();
    rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

    // Set alias
    if (alias) this->tableName = alias;
  };

  // Start a new iterator given the new compOp and value
  void setIterator(const std::string &conditionAttribute,
                   const CompOp compOp,
                   const void *value,
                   const std::vector<Attribute> &attributes) {
    iter->close();
    delete iter;
    iter = new RM_ScanIterator();
    attrs = attributes;
    attrNames.clear();
    for (Attribute &attr : attrs)
      attrNames.push_back(attr.name);
    rm.scan(tableName, conditionAttribute, compOp, value, attrNames, *iter);
  };

  RC getNextTuple(void *data) override {
    return iter->getNextTuple(rid, data);
  };

  void getAttributes(std::vector<Attribute> &attributes) const override {
    attributes.clear();
    attributes = this->attrs;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    for (Attribute &attribute : attributes) {
      std::string tmp = tableName;
      tmp += ".";
      tmp += attribute.name;
      attribute.name = tmp;
    }
  };

  ~TableScan() override {
    iter->close();
  };
};

class IndexScan : public Iterator {
  // A wrapper inheriting Iterator over IX_IndexScan
 public:
  RelationManager &rm;
  RM_IndexScanIterator *iter;
  std::string tableName;
  std::string attrName;
  std::vector<Attribute> attrs;
  char key[PAGE_SIZE]{};
  RID rid{};

  IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName, const char *alias = NULL)
    : rm(rm) {
    // Set members
    this->tableName = tableName;
    this->attrName = attrName;


    // Get Attributes from RM
    rm.getAttributes(tableName, attrs);

    // Call rm indexScan to get iterator
    iter = new RM_IndexScanIterator();
    rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

    // Set alias
    if (alias) this->tableName = alias;
  };

  // Start a new iterator given the new key range
  void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
    iter->close();
    delete iter;
    iter = new RM_IndexScanIterator();
    rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, *iter);
  };

  RC getNextTuple(void *data) override {
    int rc = iter->getNextEntry(rid, key);
    if (rc == 0) {
      rc = rm.readTuple(tableName.c_str(), rid, data);
    }
    return rc;
  };

  void getAttributes(std::vector<Attribute> &attributes) const override {
    attributes.clear();
    attributes = this->attrs;


    // For attribute in std::vector<Attribute>, name it as rel.attr
    for (Attribute &attribute : attributes) {
      std::string tmp = tableName;
      tmp += ".";
      tmp += attribute.name;
      attribute.name = tmp;
    }
  };

  ~IndexScan() override {
    iter->close();
  };
};

class Filter : public Iterator {
  // Filter operator
 public:
  Filter(Iterator *input,               // Iterator of input R
         const Condition &condition     // Selection condition
  );

  ~Filter() override;

  RC getNextTuple(void *data) override;

  // For attribute in std::vector<Attribute>, name it as rel.attr
  void getAttributes(std::vector<Attribute> &attrs) const override;

 private:
  Iterator *input_;
  const Condition condition_;
  std::vector<Attribute> attrs_;
  int attr_idx_;
};

class Project : public Iterator {
  // Projection operator
 public:
  Project(Iterator *input,                    // Iterator of input R
          const std::vector<std::string> &attrNames);   // std::vector containing attribute names
  ~Project() override;

  RC getNextTuple(void *data) override;

  // For attribute in std::vector<Attribute>, name it as rel.attr
  void getAttributes(std::vector<Attribute> &attrs) const override;

 private:
  Iterator *input_;
  std::vector<Attribute> input_attrs_;
  std::unordered_set<std::string> input_attr_names_;  // in case the projected fields are not sorted
  std::vector<int> proj_idx_;
};

class BNLJoin : public Iterator {
  // Block nested-loop join operator
 public:
  BNLJoin(Iterator *leftIn,            // Iterator of input R
          TableScan *rightIn,           // TableScan Iterator of input S
          const Condition &condition,   // Join condition
          const unsigned numPages       // # of pages that can be loaded into memory,
    //   i.e., memory block size (decided by the optimizer)
  );

  ~BNLJoin() override;

  RC getNextTuple(void *data) override;

  // For attribute in std::vector<Attribute>, name it as rel.attr
  void getAttributes(std::vector<Attribute> &attrs) const override;

 private:
  std::vector<Attribute> l_attrs_;
  std::vector<Attribute> r_attrs_;
  Condition condition_;
  unsigned num_pages_;
  unsigned left_record_length_;
  Iterator *l_in_;
  Iterator *r_in_;
  int l_pos_;
  int r_pos_;
  char *l_buffer_; // in-memory buffer with size = num_pages * PAGE_SIZE, used for loading outer table into hash table
  char *r_buffer_;
  std::unordered_map<Key, std::vector<char *>, KeyHash> hash_map_;
  bool same_key_in_left_; // true when going to iter multiple records in left table that matches the same key with current right record
  std::pair<std::vector<char *>::iterator, std::vector<char *>::iterator> same_key_iter_; // curr and end
  RC loadLeftRecordBlocks();
};

class INLJoin : public Iterator {
  // Index nested-loop join operator
 public:
  INLJoin(Iterator *leftIn,           // Iterator of input R
          IndexScan *rightIn,          // IndexScan Iterator of input S
          const Condition &condition   // Join condition
  );

  ~INLJoin() override;

  RC getNextTuple(void *data) override;

  // For attribute in std::vector<Attribute>, name it as rel.attr
  void getAttributes(std::vector<Attribute> &attrs) const override;

 private:
  Iterator *l_in_;
  IndexScan *r_in_;
  Condition condition_;
  int l_pos_;
  std::vector<Attribute> l_attrs_;
  std::vector<Attribute> r_attrs_;
  char *l_buffer_;
  bool same_key_in_right_;
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
  // Grace hash join operator
 public:
  GHJoin(Iterator *leftIn,               // Iterator of input R
         Iterator *rightIn,               // Iterator of input S
         const Condition &condition,      // Join condition (CompOp is always EQ)
         const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
  );

  ~GHJoin() override;

  RC getNextTuple(void *data) override;

  // For attribute in std::vector<Attribute>, name it as rel.attr
  void getAttributes(std::vector<Attribute> &attrs) const;

 private:
  Iterator *l_in_;
  Iterator *r_in_;
  std::vector<Attribute> l_attrs_;
  std::vector<Attribute> r_attrs_;
  int l_pos_;
  int r_pos_;
  const Condition condition_;
  const unsigned num_partitions_;
  std::vector<std::shared_ptr<FileHandle>> l_fhs_;
  std::vector<std::shared_ptr<FileHandle>> r_fhs_;

  RecordBasedFileManager *rbfm_;
  int curr_partition_;
  std::unordered_map<Key, std::vector<RID>, KeyHash> hash_map_;
  RBFM_ScanIterator r_iter_;
  char *r_buffer_;

  bool same_key_in_left_;
  std::pair<std::vector<RID>::iterator, std::vector<RID>::iterator> same_key_iter_; // curr and end


  void dumpPartitions(bool is_left);

  RC loadLeftPartition(int num);

  int inline getHash(const Key &key) {
    return KeyHash()(key) % num_partitions_;
  }

  std::string inline getPartitionFileName(int num, bool is_left) {
    if (is_left)
      return "left_" + l_attrs_[l_pos_].name + "_" + std::to_string(num);
    else
      return "right_" + r_attrs_[r_pos_].name + "_" + std::to_string(num);
  }
};

class Aggregate : public Iterator {
  // Aggregation operator
 public:
  // Mandatory
  // Basic aggregation
  Aggregate(Iterator *input,          // Iterator of input R
            const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
            AggregateOp op            // Aggregate operation
  );

  // Optional for everyone: 5 extra-credit points
  // Group-based hash aggregation
  Aggregate(Iterator *input,             // Iterator of input R
            const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
            const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
            AggregateOp op              // Aggregate operation
  );

  ~Aggregate() = default;

  RC getNextTuple(void *data) override;

  // Please name the output attribute as aggregateOp(aggAttr)
  // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
  // output attrname = "MAX(rel.attr)"
  void getAttributes(std::vector<Attribute> &attrs) const override {};

 private:
  Iterator *input_;
  Attribute agg_attr_;
  Attribute group_attr_;
  AggregateOp op_;
  float cnt_;
  float val_;
  bool is_group_by_;
  bool is_first_return_;  // indicate whether getNextTuple has been called
  std::unordered_map<Key, std::pair<float, float>, KeyHash> group_map_;  // map<key, <cnt, val>>
  std::unordered_map<Key, std::pair<float, float>, KeyHash>::iterator group_map_iter_;

  void updateValue(float &cnt, float &val, const void *data);

  float returnValue(float cnt, float val);

  RC getNextNotGroupBy(void *data);

  RC getNextGroupBy(void *data);
};

#endif
