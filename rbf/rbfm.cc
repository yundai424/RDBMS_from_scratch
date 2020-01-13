#include "rbfm.h"
#include <math.h>

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

void RecordBasedFileManager::AppendNewPage(FileHandle & f) {
  char empty[PAGE_SIZE];
  f.appendPage(empty);

}

void *RecordBasedFileManager::decodeRecord(const std::vector<Attribute> &recordDescriptor,
                                           const void *data,
                                           unsigned &recordLength) {
  int numFields = recordDescriptor.size();
  int ptrsLength = numFields * sizeof(short);
  int nullIndicatorLength = ceil((double) numFields / 8);

  void *ptrs = (char *) malloc(ptrsLength);
  int ptr = nullIndicatorLength;

}

short RecordBasedFileManager::firstAvailableSlot(const void *data) {

}


