#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int RBFTest_private_3b(RecordBasedFileManager &rbfm) {
    // Functions Tested:
    // 1. Create File - RBFM
    // 2. Open File
    // 3. insertRecord() - checks if we can't find an enough space in the last page,
    //                     the system checks from the beginning of the file.
    // 4. Close File
    // 5. Destroy File
    std::cout << "***** In RBF Test Case Private 3b *****" << std::endl;

    RC rc;
    std::string fileName = "test_private_3b";

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCount1 = 0;
    unsigned writePageCount1 = 0;
    unsigned appendPageCount1 = 0;
    unsigned readPageCountDiff = 0;
    unsigned writePageCountDiff = 0;
    unsigned appendPageCountDiff = 0;

    unsigned numberOfHeaderPages = 0;


    // Create a file named "test_private_3b"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating a file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating a file failed.");

    // Open the file "test_private_3b"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    // Get the initial number of pages in the file.
    // If it's greater than zero, we assume there is a directory.
    numberOfHeaderPages = fileHandle.getNumberOfPages();

    bool headerPageExists = numberOfHeaderPages > 0;

    if (headerPageExists) {
        std::cout << std::endl << "A header page exists." << std::endl;
    }

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

    int numRecords = 30;

    // Insert 30 records into the file
    for (int i = 0; i < numRecords; i++) {
        memset(record, 0, 3000);
        prepareLargeRecord4(recordDescriptor.size(), nullsIndicator, 2060 + i, record, &recordSize);

        rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success && "Inserting a record should not fail.");
    }

    // Collect before counters before doing one more insert
    rc = fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    if (rc != success) {
        std::cout << "[FAIL] collectCounterValues() failed. Test Case Private 3b failed." << std::endl;
        rbfm.closeFile(fileHandle);
        rbfm.destroyFile(fileName);
        free(record);
        free(returnedData);
        return -1;
    }

    // One more insertion
    memset(record, 0, 3000);
    prepareLargeRecord4(recordDescriptor.size(), nullsIndicator, 2160, record, &recordSize);

    rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");

    // Collect after counters
    rc = fileHandle.collectCounterValues(readPageCount1, writePageCount1, appendPageCount1);
    if (rc != success) {
        std::cout << "[FAIL] collectCounterValues() failed. Test Case Private 3b failed." << std::endl;
        rbfm.closeFile(fileHandle);
        rbfm.destroyFile(fileName);
        free(record);
        free(returnedData);
        return -1;
    }

    // Calculate the counter differences
    readPageCountDiff = readPageCount1 - readPageCount;
    appendPageCountDiff = appendPageCount1 - appendPageCount;
    writePageCountDiff = writePageCount1 - writePageCount;

    std::cout << "before:R W A - " << readPageCount << " " << writePageCount << " " << appendPageCount
              << " after:R W A - " << readPageCount1 << " " << writePageCount1 << " " << appendPageCount1 << std::endl;

    // If a directory exists, then we need to read at least one page and append one page.
    // Also, we need to update the directory structure. So, we need to have one write.
    if (headerPageExists) {
        if (readPageCountDiff < 1 || appendPageCountDiff < 1 || writePageCountDiff < 1) {
            std::cout << "The implementation regarding insertRecord() is not correct." << std::endl;
            std::cout << "***** [FAIL] Test Case Private 3b Failed! *****" << std::endl;
            rbfm.closeFile(fileHandle);
            rbfm.destroyFile(fileName);
            free(record);
            free(returnedData);
            return -1;
        }
    } else {
        // Each page can only contain one record. So, readPageCountDiff should be greater than 30
        // since the system needs to go through all pages from the beginning.
        if (readPageCountDiff < numRecords) {
            std::cout << "The implementation regarding insertRecord() is not correct." << std::endl;
            std::cout << "***** [FAIL] Test Case Private 3b Failed! *****" << std::endl;
            rbfm.closeFile(fileHandle);
            rbfm.destroyFile(fileName);
            free(record);
            free(returnedData);
            return -1;
        }
    }


    // Close the file "test_private_3b"
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    int fsize = getFileSize(fileName);
    if (fsize <= 0) {
        std::cout << "File Size should not be zero at this moment." << std::endl;
        std::cout << "***** [FAIL] Test Case Private 3b Failed! *****" << std::endl << std::endl;
        rbfm.destroyFile(fileName);
        free(record);
        free(returnedData);
        return -1;
    }

    // Destroy File
    rc = rbfm.destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    free(record);
    free(returnedData);

    std::cout << "***** RBF Test Case Private 3b Finished. The result will be examined. *****" << std::endl
              << std::endl;

    return 0;
}

int main() {

    remove("test_private_3b");

    return RBFTest_private_3b(RecordBasedFileManager::instance());
}