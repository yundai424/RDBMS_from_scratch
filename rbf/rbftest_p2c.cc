#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int RBFTest_private_2c(RecordBasedFileManager &rbfm) {
    // Functions Tested:
    // 1. Create File - RBFM
    // 2. Open File
    // 3. insertRecord() - with multiple NULLs
    // 4. Close File
    // 5. Destroy File
    std::cout << "***** In RBF Test Case Private 2c *****" << std::endl;

    RC rc;
    std::string fileName = "test_private_2c";

    // Create a file named "test_private_2c"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "test_private_2c"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid;
    int recordSize = 0;
    void *record = malloc(2000);
    void *returnedData = malloc(2000);

    std::vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor3(recordDescriptor);

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memset(record, 0, 2000);

    // Setting the following bytes as NULL
    // The entire byte representation is: 100011011000001111001000
    //                                    123456789012345678901234
    nullsIndicator[0] = 157; // 10011101
    nullsIndicator[1] = 130; // 10000010
    nullsIndicator[2] = 75;  // 01001011

    // Insert a record into a file
    prepareLargeRecord3(recordDescriptor.size(), nullsIndicator, 8, record, &recordSize);

    // Values of attr0, attr13, and attr20 should be NULL.
    std::cout << std::endl << "Data to be inserted:" << std::endl;
    rbfm.printRecord(recordDescriptor, record);

    rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm.readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");

    // Values of multiple fields should be NULL.
    std::cout << std::endl << "Returned Data:" << std::endl;
    rbfm.printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if (memcmp(record, returnedData, recordSize) != 0) {
        std::cout << "***** [FAIL] Test Case Private 2c Failed! *****" << std::endl << std::endl;
        free(record);
        free(returnedData);
        rbfm.closeFile(fileHandle);
        rbfm.destroyFile(fileName);
        return -1;
    }

    // Close the file "test_private_2c"
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    int fsize = getFileSize(fileName);
    if (fsize <= 0) {
        std::cout << "File Size should not be zero at this moment." << std::endl;
        return -1;
    }

    // Destroy File
    rc = rbfm.destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    free(record);
    free(returnedData);

    std::cout << "***** RBF Test Case Private 2c Finished. The result will be examined! *****" << std::endl << std::endl;

    return 0;
}

int main() {
                    
    remove("test_private_2c");

    return RBFTest_private_2c(RecordBasedFileManager::instance());

}