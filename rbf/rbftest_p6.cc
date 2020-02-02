#include "pfm.h"
#include "test_util.h"

int RBFTest_private_6(PagedFileManager &pfm) {
    // Functions Tested:
    // 1. Open File
    // 2. Append Page
    // 3. Close File
    // 4. Open File again
    // 5. Get Number Of Pages
    // 6. Get Counter Values
    // 7. Close File
    std::cout << std::endl << "***** In RBF Test Case Private 6 *****" << std::endl;

    RC rc;
    std::string fileName = "test_private_6";

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCount1 = 0;
    unsigned writePageCount1 = 0;
    unsigned appendPageCount1 = 0;

    rc = pfm.createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "test_private_6"
    FileHandle fileHandle;
    rc = pfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    // Collect before counters
    rc = fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    if (rc != success) {
        std::cout << "[FAIL] collectCounterValues() failed. Test Case p5 failed." << std::endl;
        pfm.closeFile(fileHandle);
        return -1;
    }

    // Append the first page read the first page write the first page append the second page
    void *data = malloc(PAGE_SIZE);
    void *read_buffer = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < PAGE_SIZE; i++) {
        *((char *) data + i) = i % 96 + 30;
    }
    rc = fileHandle.appendPage(data);
    assert(rc == success && "Appending a page should not fail.");
    rc = fileHandle.readPage(0, read_buffer);
    assert(rc == success && "Reading a page should not fail.");
    for (unsigned i = 0; i < PAGE_SIZE; i++) {
        *((char *) data + i) = i % 96 + 30;
    }
    rc = fileHandle.writePage(0, data);
    assert(rc == success && "Writing a page should not fail.");
    for (unsigned i = 0; i < PAGE_SIZE; i++) {
        *((char *) data + i) = i % 96 + 30;
    }
    rc = fileHandle.appendPage(data);
    assert(rc == success && "Appending a page should not fail.");

    // collect after counters
    rc = fileHandle.collectCounterValues(readPageCount1, writePageCount1, appendPageCount1);
    if (rc != success) {
        std::cout << "[FAIL] collectCounterValues() failed. Test Case 13 failed." << std::endl;
        pfm.closeFile(fileHandle);
        return -1;
    }
    std::cout << "before:R W A - " << readPageCount << " " << writePageCount << " " << appendPageCount
              << " after:R W A - " << readPageCount1 << " " << writePageCount1 << " " << appendPageCount1 << std::endl;
    assert((readPageCount1 - readPageCount == 1 || readPageCount1 - readPageCount == 2) &&
           "Read counter should be correct.");
    assert(writePageCount1 - writePageCount == 1 && "Write counter should be correct.");
    assert((appendPageCount1 - appendPageCount == 2 || appendPageCount1 - appendPageCount == 3) &&
           "Append counter should be correct.");


    // Get the number of pages
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned) 2 && "The count should be one at this moment.");

    // Close the file "test_private_6"
    rc = pfm.closeFile(fileHandle);

    assert(rc == success && "Closing the file should not fail.");

    std::cout << "REOPEN FILE" << std::endl;
    // Open the file "test_private_6"
    FileHandle fileHandle2;
    rc = pfm.openFile(fileName, fileHandle2);

    assert(rc == success && "Open the file should not fail.");

    // collect after counters
    rc = fileHandle2.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    if (rc != success) {
        std::cout << "[FAIL] collectCounterValues() failed. Test Case p5 failed." << std::endl;
        pfm.closeFile(fileHandle);
        return -1;
    }
    std::cout << "after reopen: R W A - " << readPageCount << " " << writePageCount << " " << appendPageCount
              << std::endl;
    assert((readPageCount - readPageCount1 <= 2) && "Read counter should be correct.");
    assert((writePageCount - writePageCount1 <= 2) && "Write counter should be correct.");
    assert((appendPageCount - appendPageCount1 == 0) && "Append counter should be correct.");

    rc = pfm.closeFile(fileHandle2);

    assert(rc == success && "Closing the file should not fail.");

    int fsize = getFileSize(fileName);
    if (fsize <= 0) {
        std::cout << "File Size should not be zero at this moment." << std::endl;
        std::cout << "***** [FAIL] RBF Test Case Private 6 Failed! *****" << std::endl << std::endl;
        return -1;
    }

    rc = pfm.destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    free(data);
    free(read_buffer);

    std::cout << "RBF Test Case Private 6 Finished! The result will be examined." << std::endl << std::endl;

    return 0;
}

int main() {
    // To test the functionality of the paged file manager
    PagedFileManager &pfm = PagedFileManager::instance();
    remove("test_private_6");
    return RBFTest_private_6(pfm);
}