#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int RBFTest_private_3(RecordBasedFileManager &rbfm) {
    // Functions Tested:
    // 1. Create File - RBFM
    // 2. Open File
    // 3. insertRecord() - a big sized record so that two records cannot fit in a page.
    // 4. Close File
    // 5. Destroy File
    std::cout << std::endl << "***** In RBF Test Case Private 3 *****" << std::endl;

    RC rc;
    std::string fileName = "test_private_3";

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCount1 = 0;
    unsigned writePageCount1 = 0;
    unsigned appendPageCount1 = 0;

    // Create a file named "test_private_3"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating a file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating a file failed.");

    // Open the file "test_private_3"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid;
    int recordSize = 0;
    void *record = malloc(3000);
    void *returnedData = malloc(3000);

    std::vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor4(recordDescriptor);

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    int numRecords = 15;

    // collect before counters
    rc = fileHandle.collectCounterValues(readPageCount, writePageCount,
                                         appendPageCount);
    if (rc != success) {
        std::cout << "[FAIL] collectCounterValues() failed. Test Case Private 3 failed." << std::endl;
        rbfm.closeFile(fileHandle);
        return -1;
    }

    for (int i = 0; i < numRecords; i++) {
        // Insert a record into the file
        prepareLargeRecord4(recordDescriptor.size(), nullsIndicator, 2061,
                            record, &recordSize);

        rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success && "Inserting a record should not fail.");

        // collect after counters - 1
        rc = fileHandle.collectCounterValues(readPageCount1, writePageCount1,
                                             appendPageCount1);
        if (rc != success) {
            std::cout
                    << "[FAIL] collectCounterValues() failed. Test Case Private 3 failed."
                    << std::endl;
            rbfm.closeFile(fileHandle);
            return -1;
        }

        std::cout << "before:R W A - " << readPageCount << " " << writePageCount << " " << appendPageCount
                  << " after:R W A - "
                  << readPageCount1 << " " << writePageCount1 << " " << appendPageCount1 << std::endl;

        if (appendPageCount1 <= appendPageCount) {
            std::cout << "The implementation regarding appendPage() is not correct."
                      << std::endl;
            std::cout << "***** [FAIL] Test Case Private 3 Failed! *****" << std::endl;
            rbfm.closeFile(fileHandle);
            return -1;
        }

        // Given the rid, read the record from file
        rc = rbfm.readRecord(fileHandle, recordDescriptor, rid, returnedData);
        assert(rc == success && "Reading a record should not fail.");

        // Compare whether the two memory blocks are the same
        if (memcmp(record, returnedData, recordSize) != 0) {
            std::cout << "***** [FAIL] Test Case Private 3 Failed! *****" << std::endl << std::endl;
            free(record);
            free(returnedData);
            return -1;
        }

        readPageCount = readPageCount1;
        writePageCount = writePageCount1;
        appendPageCount = appendPageCount1;

    }

    // Close the file "test_private_3"
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    int fsize = getFileSize(fileName);
    if (fsize <= 0) {
        std::cout << "File Size should not be zero at this moment." << std::endl;
        std::cout << "***** [FAIL] Test Case Private 3 Failed! *****" << std::endl
                  << std::endl;
        return -1;
    }

    // Destroy File
    rc = rbfm.destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    free(record);
    free(returnedData);

    std::cout << "***** RBF Test Case Private 3 Finished. The result will be examined. *****" << std::endl << std::endl;

    return 0;
}

int main() {

    remove("test_private_3");

    return RBFTest_private_3(RecordBasedFileManager::instance());
}