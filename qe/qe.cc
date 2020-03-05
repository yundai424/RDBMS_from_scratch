
#include "qe.h"

Filter::Filter(Iterator *input, const Condition &condition) : input_(input), condition_(condition) {
  if (condition_.bRhsIsAttr) {
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
    char *in_pt = buffer + int(ceil(double(input_attrs_.size()) / 8));
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
