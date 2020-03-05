
#include "qe.h"

Filter::Filter(Iterator *input, const Condition &condition) : input_(input), condition_(condition) {
  if (condition_.bRhsIsAttr) {
    DB_WARNING << "right hand side must be value but not attr in Filter operation!";
    return;
  }
  input->getAttributes(attrs_);
  auto idx = std::find_if(attrs_.begin(),
               attrs_.end(),
               [&](const Attribute &attr) { return condition.lhsAttr == attr.name; });
  if (idx == attrs_.end()) {
    DB_ERROR << "left hand side attr not found in input iterator's attributes!";
    return;
  }
  attr_idx_ = idx - attrs_.begin();
  if (attrs_[attr_idx_].type != condition.rhsValue.type) {
    DB_ERROR << "types for left hand side and right hand side don't match!";
    return;
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

// ... the rest of your implementations go here
