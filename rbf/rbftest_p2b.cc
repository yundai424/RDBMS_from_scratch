#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_private_2b(RecordBasedFileManager &rbfm) {
    // Functions Tested:
    // 1. Create File - RBFM
    // 2. Open File
    // 3. insertRecord() - with all NULLs
    // 4. Close File
    // 5. Destroy File
    std::cout << "***** In RBF Test Case private 2b *****" << std::endl;

    RC rc;
    string fileName = "test_private_2b";

    // Create a file named "test_private_2b"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating a file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating a file should not fail.");

    // Open the file "test_private_2b"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening a file should not fail.");

    RID rid;
    unsigned recordSize = 0;
    void *record = malloc(2000);
    void *returnedData = malloc(2000);

    vector<Attribute> recordDescriptorForTweetMessage;
    createRecordDescriptorForTweetMessage(recordDescriptorForTweetMessage);

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptorForTweetMessage.size());
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // set all fields as NULL
    nullsIndicator[0] = 248; // 11111000

    // Insert a record into a file
    prepareRecordForTweetMessage(recordDescriptorForTweetMessage.size(), nullsIndicator, 1234, 9, "wildfires", 43,
                                 "Curious ... did the amazon wildfires stop ?", 999, 3, "wow", record, &recordSize);

    rc = rbfm.insertRecord(fileHandle, recordDescriptorForTweetMessage, record, rid);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm.readRecord(fileHandle, recordDescriptorForTweetMessage, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");

    // An empty string should be printed for the referred_topics field.
    std::cout << std::endl << "Should print NULLs:" << std::endl;
    rbfm.printRecord(recordDescriptorForTweetMessage, returnedData);

    // Compare whether the two memory blocks are the same
    if (memcmp(record, returnedData, recordSize) != 0) {
        std::cout << "***** [FAIL] Test Case private 2b Failed! *****" << std::endl << std::endl;
        free(record);
        free(returnedData);
        return -1;
    }

    // Close the file "test_private_2b"
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    std::cout << std::endl;
    int fsize = getFileSize(fileName);
    if (fsize <= 0) {
        std::cout << "File Size should not be zero at this moment." << std::endl;
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

    std::cout << "***** RBF Test Case private 2b Finished. The result will be examined! *****" << std::endl
              << std::endl;

    return 0;
}

int main() {

    remove("test_private_2b");

    return RBFTest_private_2b(RecordBasedFileManager::instance());

}