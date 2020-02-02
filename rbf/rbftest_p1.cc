#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int RBFTest_private_1(RecordBasedFileManager &rbfm) {
    // Checks whether VarChar is implemented correctly or not.
    //
    // Functions tested
    // 1. Create Two Record-Based File
    // 2. Open Two Record-Based File
    // 3. Insert Multiple Records Into Two files
    // 4. Close Two Record-Based File
    // 5. Compare The File Sizes
    // 6. Destroy Two Record-Based File
    std::cout << std::endl << "***** In RBF Test Case Private 1 *****" << std::endl;

    RC rc;
    std::string fileName1 = "test_private_1a";
    std::string fileName2 = "test_private_1b";

    // Create a file named "test_private_1a"
    rc = rbfm.createFile(fileName1);
    assert(rc == success && "Creating a file should not fail.");

    rc = createFileShouldSucceed(fileName1);
    assert(rc == success && "Creating a file failed.");

    // Create a file named "test_private_1b"
    rc = rbfm.createFile(fileName2);
    assert(rc == success && "Creating a file should not fail.");

    rc = createFileShouldSucceed(fileName2);
    assert(rc == success && "Creating a file failed.");

    // Open the file "test_private_1a"
    FileHandle fileHandle1;
    rc = rbfm.openFile(fileName1, fileHandle1);
    assert(rc == success && "Opening a file should not fail.");

    // Open the file "test_private_1b"
    FileHandle fileHandle2;
    rc = rbfm.openFile(fileName2, fileHandle2);
    assert(rc == success && "Opening a file should not fail.");

    RID rid;
    void *record = malloc(PAGE_SIZE);
    int numRecords = 16000;

    // Each varchar field length - 200
    std::vector<Attribute> recordDescriptor1;
    createRecordDescriptorForTwitterUser(recordDescriptor1);

    // NULL field indicator
    int nullFieldsIndicatorActualSize1 = getActualByteForNullsIndicator(recordDescriptor1.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize1);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize1);

    // Each varchar field length - 800
    std::vector<Attribute> recordDescriptor2;
    createRecordDescriptorForTwitterUser2(recordDescriptor2);

    bool equalSizes;

    // Insert records into file
    for (int i = 0; i < numRecords; i++) {
        // Test insert Record
        int size = 0;
        memset(record, 0, 3000);
        prepareLargeRecordForTwitterUser(recordDescriptor1.size(), nullsIndicator, i, record, &size);

        rc = rbfm.insertRecord(fileHandle1, recordDescriptor1, record, rid);
        assert(rc == success && "Inserting a record should not fail.");

        rc = rbfm.insertRecord(fileHandle2, recordDescriptor2, record, rid);
        assert(rc == success && "Inserting a record should not fail.");

        if (i % 1000 == 0 && i != 0) {
            std::cout << i << "/" << numRecords << " records are inserted." << std::endl;
            assert(compareFileSizes(fileName1, fileName2) && "Files should be the same size") ;
        }
    }
    // Close the file "test_private_1a"
    rc = rbfm.closeFile(fileHandle1);
    assert(rc == success && "Closing a file should not fail.");

    // Close the file "test_private_1b"
    rc = rbfm.closeFile(fileHandle2);
    assert(rc == success && "Closing a file should not fail.");

    free(record);

    std::cout << std::endl;
    equalSizes = compareFileSizes(fileName1, fileName2);

    rc = rbfm.destroyFile(fileName1);
    assert(rc == success && "Destroying a file should not fail.");

    rc = destroyFileShouldSucceed(fileName1);
    assert(rc == success && "Destroying the file should not fail.");

    rc = rbfm.destroyFile(fileName2);
    assert(rc == success && "Destroying a file should not fail.");

    rc = destroyFileShouldSucceed(fileName1);
    assert(rc == success && "Destroying the file should not fail.");
    
    free(nullsIndicator);

    if (!equalSizes) {
        std::cout << "Variable length Record is not properly implemented." << std::endl;
        std::cout << "**** [FAIL] RBF Test Private 1 failed. Two files are of different sizes. *****" << std::endl;

        return -1;
    }

    std::cout << "***** RBF Test Case Private 1 Finished. The result will be examined! *****" << std::endl << std::endl;

    return 0;
}

int main() {

    remove("test_private_1a");
    remove("test_private_1b");

    return RBFTest_private_1(RecordBasedFileManager::instance());
}