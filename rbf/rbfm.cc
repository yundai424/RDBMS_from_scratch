#include <math.h>

#include "rbfm.h"

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;

RecordBasedFileManager &RecordBasedFileManager::instance() {
  static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
  return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() : pfm_(&PagedFileManager::instance()) {}

RecordBasedFileManager::~RecordBasedFileManager() { delete _rbf_manager; }

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {
  return PagedFileManager::instance().createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
  return PagedFileManager::instance().destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
  return PagedFileManager::instance().openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
  return PagedFileManager::instance().closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
  // use the array of field offsets method for variable length record introduced in class as the format of record
  // each record has a leading series of bytes indicating the pointers to each field


  //


  auto res = decodeRecord(recordDescriptor, data);
  std::vector<directory_entry> &directories = std::get<0>(res);
  const char *real_data = std::get<1>(res);
  size_t real_data_size = std::get<2>(res);
  size_t total_size = std::get<3>(res);

  // find target position to insert according to total_size
  std::fstream fs;
  size_t pos;
  fs.seekp(pos);
  fs.write((char *) directories.data(), sizeof(directory_entry) * directories.size());
  fs.write(real_data, real_data_size);

  return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
  return -1;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
  return -1;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
  return -1;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
  return -1;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {
  return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
  return -1;
}

/**
 * ========= Utility functions ==========
 */

void RecordBasedFileManager::appendNewPage(FileHandle &f) {
  char empty[PAGE_SIZE];
  f.appendPage(empty);
  // TODO

}

std::tuple<std::vector<directory_entry>, const char *, size_t, size_t>
RecordBasedFileManager::decodeRecord(const std::vector<Attribute> &recordDescriptor,
                                     const void *data) {
  // parse null indicators
  std::vector<bool> null_indicators;
  int fields_num = recordDescriptor.size();
  int indicator_bytes_num = int(ceil(double(fields_num) / 8));
  unsigned char indicator_bytes[indicator_bytes_num];
  unsigned char *pt = indicator_bytes;
  memcpy(indicator_bytes, data, indicator_bytes_num);

  for (auto i = 0; i < fields_num; ++i) {
    unsigned char mask = 1;
    mask = mask << i;
    null_indicators.push_back(mask & (*pt));
    if (i % 8 == 7) pt++;
  }

  // the real data position
  const char *real_data = ((char *) data) + indicator_bytes_num;

  // create directories
  std::vector<directory_entry> directories;
  unsigned offset = sizeof(directory_entry) * fields_num;
  for (int i = 0; i < fields_num; ++i) {
    if (null_indicators[i]) {
      directories.push_back(offset);
      offset += recordDescriptor[i].length;
    } else {
      directories.push_back(-1); // -1 to indicate null
    }
  }
//  directories.push_back(offset); // I want to use one additional size to mark the end of record
  size_t real_data_size = offset - sizeof(directory_entry) * fields_num;

  return {directories, real_data, real_data_size, offset};

}

short RecordBasedFileManager::firstAvailableSlot(const void *data) {

}



