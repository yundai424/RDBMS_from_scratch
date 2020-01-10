#include "pfm.h"
#include <iostream>

bool ifFileExists(const std::string &fileName) {
    std::ifstream file(fileName);
    if (file) {
        file.close();
        return true;
    }
    return false;
}

/**
 * ======= PagedFileManager =======
 */
PagedFileManager *PagedFileManager::_pf_manager = nullptr;

PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() {delete _pf_manager;}

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

RC PagedFileManager::createFile(const std::string &fileName) {
    FileHandle handler;
    return handler.createFile(fileName);
}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    if (!ifFileExists(fileName))
        return -1;
    if (remove(fileName.c_str()) != 0)
        return -1;
    return 0;
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return fileHandle.openFile(fileName);
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    return fileHandle.closeFile();
}

/**
 * ======= FileHandle ==========
 */

FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::~FileHandle() = default;

std::fstream & FileHandle::getFile() {return _file;}

RC FileHandle::openFile(const std::string & fileName) {
    if (!ifFileExists(fileName))
        return -1;
    if (_file.is_open())
        return -1;
    _file.open(fileName, std::ios::in | std::ios::out | std::ios::binary);

    // update counter using metadata
    _file.seekg(0);
    _file.read((char*)&readPageCounter, sizeof(unsigned));
    _file.read((char*)&writePageCounter, sizeof(unsigned));
    _file.read((char*)&appendPageCounter, sizeof(unsigned));
    return 0;
}

RC FileHandle::closeFile() {
    if (!_file.is_open())
        return -1;
    // flush new counters to metadata
    updateCounterToFile();
    _file.close();
    return 0;
}

RC FileHandle::updateCounterToFile() {
    if (!_file.is_open())
        return -1;
    _file.seekp(0);
    _file.write((char*)&readPageCounter, sizeof(unsigned));
    _file.write((char*)&writePageCounter, sizeof(unsigned));
    _file.write((char*)&appendPageCounter, sizeof(unsigned));
    return 0;
}

RC FileHandle::createFile(const std::string & fileName) {
    if (ifFileExists(fileName))
        return -1;
    if (_file.is_open())
        return -1;
    _file.open(fileName, std::ios::out | std::ios::binary);
    // write counters as metadata to head of file
    if (updateCounterToFile() != 0)
        return -1;
    return closeFile();
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
    // pageNum exceed total number of pages
    if (pageNum >= getNumberOfPages() || !_file.is_open())
        return -1;
    _file.seekg(OFFSET + PAGE_SIZE * pageNum);
    _file.read((char*)data, PAGE_SIZE);
    readPageCounter++;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if (pageNum >= getNumberOfPages() || !_file.is_open())
        return -1;
    _file.seekp(OFFSET + PAGE_SIZE * pageNum);
    _file.write((char*)data, PAGE_SIZE);
    writePageCounter++;
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    if (!_file.is_open())
        return -1;
    _file.seekp(OFFSET + PAGE_SIZE * appendPageCounter);
    _file.write((char*)data, PAGE_SIZE);
    appendPageCounter++;
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    return appendPageCounter;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}