
#include "qe.h"
#include <unordered_map>

/******************************
 *         Utilities
 *****************************/

std::pair<bool, Key> Utils::parseCondValue(const std::vector<Attribute> & attrs, int pos, const void * data) {
  std::vector<bool> is_null = RecordBasedFileManager::parseNullIndicator((unsigned char *) data, attrs.size());
  if (is_null[pos]) return {false, {}};
  char *pt = (char *) data + RecordBasedFileManager::nullIndicatorLength(attrs);
  for (int i = 0; i < pos; ++i) {
    if (attrs[i].type == TypeVarChar) {
      int char_len = *pt;
      pt += sizeof(int) + char_len;
    } else {
      pt += attrs[i].length;
    }
  }
  return {true, Key(attrs[pos].type, pt, {0, 0})};
}

void Utils::concatRecords(const std::vector<Attribute> &left_attrs,
                          const std::vector<Attribute> &right_attrs,
                          const void *left_data,
                          const void *right_data,
                          void *output) {
  char *pt = (char *)output;
  std::vector<bool> l_null = RecordBasedFileManager::parseNullIndicator((unsigned char *)left_data, left_attrs.size());
  std::vector<bool> r_null = RecordBasedFileManager::parseNullIndicator((unsigned char *)right_data, right_attrs.size());
  l_null.insert(l_null.end(), r_null.begin(), r_null.end());
  std::vector<char> null_bytes = RecordBasedFileManager::makeNullIndicator(l_null);
  memcpy(pt, null_bytes.data(), null_bytes.size());
  pt += null_bytes.size();

  int l_record_len = RecordBasedFileManager::getRecordLength(left_attrs, left_data);
  int r_record_len = RecordBasedFileManager::getRecordLength(right_attrs, right_data);
  memcpy(pt, (char *)left_data + RecordBasedFileManager::nullIndicatorLength(left_attrs), l_record_len);
  pt += l_record_len;
  memcpy(pt, (char *)right_data + RecordBasedFileManager::nullIndicatorLength(right_attrs), r_record_len);
  std::vector<Attribute> attrs;
  attrs.insert(attrs.end(), left_attrs.begin(), left_attrs.end());
  attrs.insert(attrs.end(), right_attrs.begin(), right_attrs.end());
}
/******************************
 *          Filter
 *****************************/

Filter::Filter(Iterator *input, const Condition &condition) : input_(input), condition_(condition) {
  if (condition.bRhsIsAttr) {
    DB_WARNING << "right hand side must be value but not attr in Filter operation!";
    throw std::runtime_error("Invalid condition");
  }
  input->getAttributes(attrs_);
  auto idx = std::find_if(attrs_.begin(),
               attrs_.end(),
               [&](const Attribute &attr) { return condition.lhsAttr == attr.name; });
  if (idx == attrs_.end()) {
    DB_ERROR << "left hand side attr not found in input iterator's attributes!";
    throw std::runtime_error("Attr not found");
  }
  attr_idx_ = idx - attrs_.begin();
  if (attrs_[attr_idx_].type != condition.rhsValue.type) {
    DB_ERROR << "types for left hand side and right hand side don't match!";
    throw std::runtime_error("Invalid condition");
  }

}

Filter::~Filter() = default;

RC Filter::getNextTuple(void *data) {
  while (input_->getNextTuple(data) != QE_EOF) {
    if (condition_.op == NO_OP) {
      return 0;
    }
    int indicator_bytes_num = int(ceil(double(attrs_.size()) / 8));
    std::vector<bool>
      null_indicators = RecordBasedFileManager::parseNullIndicator((const unsigned char *) data, attrs_.size());
    // corresponding field is NULL: cmp always result in false
    if (null_indicators[attr_idx_]) {
      continue;
    }
    const char *real_data = ((char *) data) + indicator_bytes_num;
    for (int i = 0; i < attr_idx_; i++) {
      if (!null_indicators[i]) {
        if (attrs_[i].type == TypeVarChar) {
          int char_len = *((int *) real_data);
          real_data += sizeof(int) + char_len;
        } else {
          real_data += attrs_[i].length;
        }
      }
    }
    RC cmp = RecordBasedFileManager::cmpAttr(condition_.op,
                                             condition_.rhsValue.type,
                                             real_data,
                                             condition_.rhsValue.data);
    if (cmp) return 0;
  }
  return QE_EOF;
}

  void Filter::getAttributes(std::vector<Attribute> & attrs) const {
  input_->getAttributes(attrs);
}

/******************************
 *          Project
 *****************************/

Project::Project(Iterator *input, const std::vector<std::string> &attrNames) : input_(input) {
  input->getAttributes(input_attrs_);
  for (Attribute &attr : input_attrs_) input_attr_names_.insert(attr.name);
  for (auto &field : attrNames) {
    auto it = std::find_if(input_attrs_.begin(),
                           input_attrs_.end(),
                           [&](const Attribute &attr) { return attr.name == field; });
    if (it == input_attrs_.end()) {
      DB_ERROR << "projected field not found: " << field;
      throw std::runtime_error("Attribute not found error");
    }
    proj_idx_.push_back(it - input_attrs_.begin());
  }
}

Project::~Project() = default;

RC Project::getNextTuple(void * data) {
  char buffer[PAGE_SIZE];
  if (input_->getNextTuple(buffer) != QE_EOF) {
    std::vector<bool> is_null = RecordBasedFileManager::parseNullIndicator(reinterpret_cast<const unsigned char *>(buffer),
                                                                                   input_attrs_.size());

    // since we cannot insure the projected idx is sorted, we must find pointers to all fields in input fields,
    //   and only until then we could fetch data

    // 1. locate pointer pointing to the beginning of each field in input data
    std::vector<int> data_field_begin;
    char *in_pt = buffer + RecordBasedFileManager::nullIndicatorLength(input_attrs_);
    int offset = 0;
    for (int i = 0; i < input_attrs_.size(); ++i) {
      if (is_null[i]) {
        data_field_begin.push_back(-1);
      } else {
        data_field_begin.push_back(offset);
        if (input_attrs_[i].type == TypeVarChar) {
          int char_len = *(in_pt + offset);
          offset += sizeof(int) + char_len;
        } else {
          offset += input_attrs_[i].length;
        }
      }
    }
    // 2. write data
    // write null indicator
    int indicator_bytes_num = int(ceil(double(proj_idx_.size()) / 8));
    memset(data, 0, indicator_bytes_num);
    char *out_pt = (char *)data;
    for (int i = 0; i < proj_idx_.size(); ++i) {
      if (is_null[proj_idx_[i]]) {
        unsigned char mask = 1 << (7 - (i % 8));
        *out_pt = *out_pt | mask;
      }
      if (i % 8 == 7) out_pt++;
    }
    // write field data
    out_pt = (char *)data + indicator_bytes_num;
    for (int idx : proj_idx_) {
      int field_len;
      if (input_attrs_[idx].type == TypeVarChar) {
        int char_len = *(in_pt + data_field_begin[idx]);
        field_len = sizeof(int) + char_len;
      } else {
        field_len = input_attrs_[idx].length;
      }
      memcpy(out_pt, in_pt + data_field_begin[idx], field_len);
      out_pt += field_len;
    }
    return 0;
  }
  return QE_EOF;
}

void Project::getAttributes(std::vector<Attribute> & attrs) const {
  attrs.clear();
  for (int idx : proj_idx_) attrs.push_back(input_attrs_.at(idx));
}

/******************************
 *
 *         BNL Join
 *
 *****************************/

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) : l_in_(
  leftIn), r_in_(rightIn), condition_(condition), num_pages_(numPages), same_key_in_left_(false), l_buffer_(nullptr),
  r_buffer_(nullptr) {
  leftIn->getAttributes(l_attrs_);
  rightIn->getAttributes(r_attrs_);
  left_record_length_ = RecordBasedFileManager::nullIndicatorLength(l_attrs_);
  for (Attribute &attr : l_attrs_)
    left_record_length_ += attr.length;

  auto it = std::find_if(l_attrs_.begin(), l_attrs_.end(), [&] (const Attribute &attr) {return attr.name == condition.lhsAttr;});
  if (it == l_attrs_.end()) {
    DB_ERROR << "join attribute " << condition.lhsAttr << "not found in left table!";
    throw std::runtime_error("attribute not found");
  }
  l_pos_ = it - l_attrs_.begin();

  it = std::find_if(r_attrs_.begin(), r_attrs_.end(), [&] (const Attribute &attr) {return attr.name == condition.rhsAttr;});
  if (it == r_attrs_.end()) {
    DB_ERROR << "join attribute " << condition.rhsAttr << "not found in right table!";
    throw std::runtime_error("attribute not found");
  }
  r_pos_ = it - r_attrs_.begin();
  r_buffer_ = (char *) malloc(PAGE_SIZE);
  l_buffer_ = (char *) malloc(numPages * PAGE_SIZE);
  loadLeftRecordBlocks();
}

BNLJoin::~BNLJoin() {
  if (l_buffer_) free(l_buffer_);
  if (r_buffer_) free(r_buffer_);
};

/**
 * load numPages of records and store into hash_map_
 * @return <RC, map<val_string, pointer>>
 */
RC BNLJoin::loadLeftRecordBlocks() {
  hash_map_.clear();
  memset(l_buffer_, 0, num_pages_ * PAGE_SIZE);
  char *output = l_buffer_;
  unsigned max_num_records = num_pages_ * PAGE_SIZE / left_record_length_;
  for (int i = 0; i < max_num_records; ++i) {
    if (l_in_->getNextTuple(output) != QE_EOF) {
      auto res = Utils::parseCondValue(l_attrs_, l_pos_, output);
      if (!res.first) continue; // null field, just skip
      hash_map_[res.second].push_back(output);
      output += left_record_length_;
    } else {
      break;
    }
  }
  if (hash_map_.empty()) return QE_EOF;
  return 0;
}

RC BNLJoin::getNextTuple(void * data) {
  // multiple records in left table mapped to the same value
  if (same_key_in_left_ && same_key_iter_.first != same_key_iter_.second) {
    Utils::concatRecords(l_attrs_, r_attrs_, *same_key_iter_.first, r_buffer_, data);
    ++same_key_iter_.first;
    return 0;
  } else {
    same_key_in_left_ = false;
  }
  // finish output the same left records, fetch next right record
  while (r_in_->getNextTuple(r_buffer_) != QE_EOF) {
    auto key = Utils::parseCondValue(r_attrs_, r_pos_, r_buffer_);
    if (!key.first || !hash_map_.count(key.second) || hash_map_.at(key.second).empty()) continue;
    std::vector<char *> &left_records = hash_map_.at(key.second);
    Utils::concatRecords(l_attrs_, r_attrs_, left_records.at(0), r_buffer_, data);
    if (left_records.size() > 1) {
      same_key_iter_.first = left_records.begin() + 1;
      same_key_iter_.second = left_records.end();
      same_key_in_left_ = true;
    }
    return 0;
  }
  if (loadLeftRecordBlocks()) return QE_EOF;
  return getNextTuple(data);
}

void BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
  attrs.clear();
  attrs.insert(attrs.end(), l_attrs_.begin(), l_attrs_.end());
  attrs.insert(attrs.end(), r_attrs_.begin(), r_attrs_.end());
}

/******************************
 *
 *         INL Join
 *
 *****************************/
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition)
  : l_in_(leftIn), r_in_(rightIn), condition_(condition), same_key_in_right_(false), l_buffer_(
  nullptr) {
  leftIn->getAttributes(l_attrs_);
  rightIn->getAttributes(r_attrs_);

  auto it = std::find_if(l_attrs_.begin(),
                         l_attrs_.end(),
                         [&](const Attribute &attr) { return attr.name == condition.lhsAttr; });
  if (it == l_attrs_.end()) {
    DB_ERROR << "join attribute " << condition.lhsAttr << "not found in left table!";
    throw std::runtime_error("attribute not found");
  }
  l_pos_ = it - l_attrs_.begin();
  l_buffer_ = (char *) malloc(PAGE_SIZE);
 }

 INLJoin::~INLJoin() {
  if (l_buffer_) free(l_buffer_);
};

 void INLJoin::getAttributes(std::vector<Attribute> & attrs) const {
   attrs.clear();
   attrs.insert(attrs.end(), l_attrs_.begin(), l_attrs_.end());
   attrs.insert(attrs.end(), r_attrs_.begin(), r_attrs_.end());
 }

RC INLJoin::getNextTuple(void *data) {
  char buffer[PAGE_SIZE];
  // current left key probes to multiple records in right B+ tree
  if (same_key_in_right_ && r_in_->getNextTuple(buffer) != QE_EOF) {
    Utils::concatRecords(l_attrs_, r_attrs_, l_buffer_, buffer, data);
    return 0;
  } else {
    same_key_in_right_ = false;
  }

  while (l_in_->getNextTuple(l_buffer_) != QE_EOF) {
    char *l_key = l_buffer_ + RecordBasedFileManager::getFieldOffset(l_attrs_, l_buffer_, l_pos_);
    r_in_->setIterator(l_key, l_key, true, true);
    if (r_in_->getNextTuple(buffer) != QE_EOF) {
      same_key_in_right_ = true;
      Utils::concatRecords(l_attrs_, r_attrs_, l_buffer_, buffer, data);
      return 0;
    }
  }
  return QE_EOF;
}

/******************************
*
*         Aggregate

*****************************/
Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op)
  : input_(input), agg_attr_(aggAttr), op_(op), is_group_by_(false), cnt_(0), val_(0), is_first_return_(true) {
  if (op_ == AggregateOp::MIN) val_ = std::numeric_limits<float>::max();
  // find position of aggAttr
  std::vector<Attribute> input_attrs;
  input->getAttributes(input_attrs);
  auto it = std::find_if(input_attrs.begin(),
                         input_attrs.end(),
                         [&](const Attribute &attr) { return attr.name == aggAttr.name; });
  if (it == input_attrs.end())
    throw std::runtime_error("aggAttr not found!");
  int pos = it - input_attrs.begin();

  // read data
  char buffer[PAGE_SIZE];
  while (input->getNextTuple(buffer) != QE_EOF) {
    auto is_null = RecordBasedFileManager::parseNullIndicator((unsigned char *)buffer, input_attrs.size());
    if (is_null[pos]) continue;
    int offset = RecordBasedFileManager::getFieldOffset(input_attrs, buffer, pos);
    updateValue(cnt_, val_, buffer + offset);
  }
}

Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op)
  : input_(input), agg_attr_(aggAttr), group_attr_(groupAttr), op_(op), is_group_by_(true), is_first_return_(true) {
  std::vector<Attribute> input_attrs;
  input->getAttributes(input_attrs);
  auto it = std::find_if(input_attrs.begin(),
                         input_attrs.end(),
                         [&](const Attribute &attr) { return attr.name == aggAttr.name; });
  if (it == input_attrs.end())
    throw std::runtime_error("aggAttr not found!");
  int agg_pos = it - input_attrs.begin();

  it = std::find_if(input_attrs.begin(),
                    input_attrs.end(),
                    [&](const Attribute &attr) { return attr.name == groupAttr.name; });
  if (it == input_attrs.end())
    throw std::runtime_error("groutAttr not found!");
  int group_pos = it - input_attrs.begin();

  char buffer[PAGE_SIZE];
  while (input->getNextTuple(buffer) != QE_EOF) {
    auto is_null = RecordBasedFileManager::parseNullIndicator((unsigned char *)buffer, input_attrs.size());
    if (is_null.at(group_pos) || is_null.at(agg_pos)) continue;
    auto key = Utils::parseCondValue(input_attrs, group_pos, buffer).second;
    // init val according to aggrOp
    if (!group_map_.count(key)) {
      if (op == AggregateOp::MIN) {
        group_map_[key] = {0, std::numeric_limits<float>::max()};
      } else {
        group_map_[key] = {0, 0};
      }
    }
    int offset = RecordBasedFileManager::getFieldOffset(input_attrs, buffer, agg_pos);
    updateValue(group_map_.at(key).first, group_map_.at(key).second, buffer + offset);
  }
}

RC Aggregate::getNextTuple(void * data) {
  if (is_group_by_) return getNextGroupBy(data);
  else return getNextNotGroupBy(data);
}

RC Aggregate::getNextGroupBy(void *data) {
  if (is_first_return_) {
    is_first_return_ = false;
    group_map_iter_ = group_map_.begin();
  }
  if (group_map_iter_ == group_map_.end()) return QE_EOF;

  char *pt = (char *) data;
  // null indicator
  memset(pt, 0, sizeof(char));
  ++pt;
  // groupby attr
  Key k = group_map_iter_->first;
  switch (k.key_type) {
    case AttrType::TypeInt:memcpy(pt, &k.i, sizeof(int));
      pt += sizeof(int);
      break;
    case AttrType::TypeReal:memcpy(pt, &k.f, sizeof(float));
      pt += sizeof(float);
      break;
    case AttrType::TypeVarChar:int str_len = k.s.size();
      memcpy(pt, &str_len, sizeof(int));
      pt += sizeof(int);
      memcpy(pt, k.s.data(), str_len);
      pt += str_len;
      break;
  }
  // aggr value
  float res = returnValue(group_map_iter_->second.first, group_map_iter_->second.second);
  memcpy(pt, &res, sizeof(float));
  ++group_map_iter_;
  return 0;
}

RC Aggregate::getNextNotGroupBy(void *data) {
  if (!is_first_return_) return QE_EOF;
  char *pt = (char *)data;
  memset(pt, 0, sizeof(char));
  ++pt;
  float res = returnValue(cnt_, val_);
  memcpy(pt, &res, agg_attr_.length);
  is_first_return_ = false;
  return 0;
}

void Aggregate::updateValue(float &cnt, float &val, const void *data) {
  float res = 0;
  if (agg_attr_.type == TypeInt) res = *((int *) data);
  else res = *((float *) data);
  switch (op_) {
    case AggregateOp::COUNT:
      ++cnt;
      break;
    case AggregateOp::SUM:
      val += res;
      break;
    case AggregateOp::MAX:
      val = std::max(val, res);
      break;
    case AggregateOp::MIN:
      val = std::min(val, res);
      break;
    case AggregateOp::AVG:
      ++cnt;
      val += res;
      break;
  }
}

float Aggregate::returnValue(float cnt, float val) {
  switch (op_) {
    case AggregateOp::COUNT:
      return cnt;
    case AggregateOp::AVG:
      return val / cnt;
    default:
      return val;
  }
}

void Aggregate::getAttributes(std::vector<Attribute> & attrs) const {
  std::string name = "";
  switch (op_) {
    case COUNT:
      name += "COUNT";
      break;
    case SUM:
      name += "SUM";
      break;
    case MAX:
      name += "MAX";
      break;
    case MIN:
      name += "MIN";
      break;
    case AVG:
      name += "AVG";
      break;
  }
  name += "(" + agg_attr_.name + ")";
  attrs.clear();
  if (is_group_by_) {
    attrs.push_back(group_attr_);
  }
  attrs.push_back(Attribute(agg_attr_));
  attrs.back().name = name;
}

/******************************
*
*         GHJoin

*****************************/

GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPartitions)
  : l_in_(leftIn), r_in_(rightIn), condition_(condition), num_partitions_(numPartitions), curr_partition_(-1),
  same_key_in_left_(false), r_buffer_(nullptr) {

  leftIn->getAttributes(l_attrs_);
  rightIn->getAttributes(r_attrs_);
  // get index of condition attr on left and right table
  auto it = std::find_if(l_attrs_.begin(), l_attrs_.end(), [&] (const Attribute &attr) {return attr.name == condition.lhsAttr;});
  if (it == l_attrs_.end()) {
    DB_ERROR << "join attribute " << condition.lhsAttr << "not found in left table!";
    throw std::runtime_error("attribute not found");
  }
  l_pos_ = it - l_attrs_.begin();

  it = std::find_if(r_attrs_.begin(), r_attrs_.end(), [&] (const Attribute &attr) {return attr.name == condition.rhsAttr;});
  if (it == r_attrs_.end()) {
    DB_ERROR << "join attribute " << condition.rhsAttr << "not found in right table!";
    throw std::runtime_error("attribute not found");
  }
  r_pos_ = it - r_attrs_.begin();

  // initialize all partition files
  rbfm_ = &RecordBasedFileManager::instance();
  for (int i = 0; i < numPartitions; ++i) {
//    FileHandle l_fh;
    std::string l_fname = getPartitionFileName(i, true);
    rbfm_->createFile(l_fname);
    l_fhs_.push_back(std::make_shared<FileHandle>());
    rbfm_->openFile(l_fname, *l_fhs_.back());

//    FileHandle r_fh;
    std::string r_fname = getPartitionFileName(i, false);
    rbfm_->createFile(r_fname);
    r_fhs_.push_back(std::make_shared<FileHandle>());
    rbfm_->openFile(r_fname, *r_fhs_.back());

  }
  // hash and dump each table to disk partition
  dumpPartitions(true);
  dumpPartitions(false);
  r_buffer_ = (char *) malloc(PAGE_SIZE);
}

GHJoin::~GHJoin() {
  if (r_buffer_) free(r_buffer_);
}

void GHJoin::dumpPartitions(bool is_left) {
  Iterator *in = is_left ? l_in_ : r_in_;
  auto &attrs = is_left ? l_attrs_ : r_attrs_;
  int pos = is_left ? l_pos_ : r_pos_;
  auto &fhs = is_left ? l_fhs_ : r_fhs_;

  char buffer[PAGE_SIZE];
  RID rid;
  while (in->getNextTuple(buffer) != QE_EOF) {
    auto key = Utils::parseCondValue(attrs, pos, buffer);
    if (!key.first) continue; // NULL field
    int hash = getHash(key.second);
    if (rbfm_->insertRecord(*fhs.at(hash), attrs, buffer, rid) != 0)
      throw std::runtime_error("dump partition failed");
  }
}

RC GHJoin::loadLeftPartition(int num) {
  hash_map_.clear();
  RBFM_ScanIterator rmsi;
  rbfm_->scan(*l_fhs_.at(num), l_attrs_, "", CompOp::NO_OP, nullptr, {}, rmsi);
  char buffer[PAGE_SIZE];
  RID rid;
  while (rmsi.getNextRecord(rid, buffer) != QE_EOF) {
    auto res = Utils::parseCondValue(l_attrs_, l_pos_, buffer);
    if (!res.first) continue; // null field, just skip
    hash_map_[res.second].push_back({rid.pageNum, rid.slotNum});
  }
  if (hash_map_.empty()) return QE_EOF;
  return 0;
}

RC GHJoin::getNextTuple(void * data) {
  // multiple records in left partition mapped to the same record in right buffer
  if (same_key_in_left_ && same_key_iter_.first != same_key_iter_.second) {
    char buffer[PAGE_SIZE];
    rbfm_->readRecord(*l_fhs_.at(curr_partition_), l_attrs_, *same_key_iter_.first, buffer);
    Utils::concatRecords(l_attrs_, r_attrs_, buffer, r_buffer_, data);
    ++same_key_iter_.first;
    return 0;
  } else {
    same_key_in_left_ = false;
  }

  RID rid;
  while (r_iter_.getNextRecord(rid, r_buffer_) != QE_EOF) {
    // probe into left partition
    auto key = Utils::parseCondValue(r_attrs_, r_pos_, r_buffer_);
    if (!key.first || !hash_map_.count(key.second) || hash_map_.at(key.second).empty()) continue;
    std::vector<RID> &left_rids = hash_map_.at(key.second);
    char buffer[PAGE_SIZE];
    rbfm_->readRecord(*l_fhs_.at(curr_partition_), l_attrs_, left_rids.at(0), buffer);
    Utils::concatRecords(l_attrs_, r_attrs_, buffer, r_buffer_, data);
    if (left_rids.size() > 1) {
      same_key_iter_.first = left_rids.begin() + 1;
      same_key_iter_.second = left_rids.end();
      same_key_in_left_ = true;
    }
    return 0;
  }

  // need to load next partition
  r_iter_.close();
  ++curr_partition_;
  if (curr_partition_ >= num_partitions_) {
    // reached end
    for (auto &fh : l_fhs_) {
      fh->closeFile();
      rbfm_->destroyFile(fh->name);
    }
    for (auto &fh : r_fhs_) {
      fh->closeFile();
      rbfm_->destroyFile(fh->name);
    }
    return QE_EOF;
  }
  loadLeftPartition(curr_partition_);
  rbfm_->scan(*r_fhs_.at(curr_partition_), r_attrs_, "", CompOp::NO_OP, nullptr, {}, r_iter_);
  return getNextTuple(data);
}


void GHJoin::getAttributes(std::vector<Attribute> & attrs) const {
  attrs.clear();
  attrs.insert(attrs.end(), l_attrs_.begin(), l_attrs_.end());
  attrs.insert(attrs.end(), r_attrs_.begin(), r_attrs_.end());
}