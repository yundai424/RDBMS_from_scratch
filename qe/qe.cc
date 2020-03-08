
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
//  std::string key;
//  switch (attrs[pos].type) {
//    case TypeVarChar: {
//      int char_len = *pt;
//      pt += sizeof(int);
//      for (int j = 0; j < char_len; ++j) key += *(pt + j);
//      break;
//    }
//    case TypeInt:key = std::to_string(*(int *) pt);
//      break;
//    case TypeReal:key = std::to_string(*(float *) pt);
//      break;
//  }
//  return {true, key};
  return {true, Key(attrs[pos].type, pt, {0, 0})};
}

void Utils::concatRecords(const std::vector<Attribute> &left_attrs,
                          const std::vector<Attribute> &right_attrs,
                          const void *left_data,
                          const void *right_data,
                          void *output) {
  char *pt = (char *)output;
  int l_null_indicator_len = RecordBasedFileManager::nullIndicatorLength(left_attrs);
  int r_null_indicator_len = RecordBasedFileManager::nullIndicatorLength(right_attrs);
  int l_record_len = RecordBasedFileManager::getRecordLength(left_attrs, left_data);
  int r_record_len = RecordBasedFileManager::getRecordLength(right_attrs, right_data);
  memcpy(pt, left_data, l_null_indicator_len);
  pt += l_null_indicator_len;
  memcpy(pt, right_data, r_null_indicator_len);
  pt += r_null_indicator_len;
  memcpy(pt, left_data, l_record_len);
  pt += l_record_len;
  memcpy(pt, right_data, r_record_len);
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
  input_->getAttributes(attrs);
}

/******************************
 *
 *         BNL Join
 *
 *****************************/

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) : l_in_(
  leftIn), r_in_(rightIn), condition_(condition), num_pages_(numPages), same_key_in_left_(false) {
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