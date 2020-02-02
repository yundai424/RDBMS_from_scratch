#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int RBFTest_private_2(RecordBasedFileManager &rbfm) {
    // Functions Tested:
    // 1. Create File - RBFM
    // 2. Open File
    // 3. insertRecord() - with an empty string field (not NULL)
    // 4. insertRecord() - with a NULL string field
    // 5. Close File
    // 6. Destroy File
    std::cout << "***** In RBF Test Case Private 2 *****" << std::endl;

    RC rc;
    std::string fileName = "test_private_2";

    // Create a file named "test_private_2"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating a file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating a file should not fail.");

    // Open the file "test_private_2"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening a file should not fail.");

    RID rid;
    unsigned recordSize = 0;
    void *record = malloc(2000);
    void *returnedData = malloc(2000);

    std::vector<Attribute> recordDescriptorForTweetMessage;
    createRecordDescriptorForTweetMessage(recordDescriptorForTweetMessage);

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptorForTweetMessage.size());
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file - referred_topics is an empty string - "", not null value.
    prepareRecordForTweetMessage(recordDescriptorForTweetMessage.size(), nullsIndicator, 1234, 0, "", 0, "", 999, 0, "",
                                 record, &recordSize);
    rc = rbfm.insertRecord(fileHandle, recordDescriptorForTweetMessage, record, rid);

    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm.readRecord(fileHandle, recordDescriptorForTweetMessage, rid,
                         returnedData);
    assert(rc == success && "Reading a record should not fail.");

    // An empty string should be printed for the referred_topics field.
    std::cout << std::endl << "Should print empty strings:" << std::endl;
    rbfm.printRecord(recordDescriptorForTweetMessage, returnedData);
    std::cout << std::endl;


    // Compare whether the two memory blocks are the same
    if (memcmp(record, returnedData, recordSize) != 0) {
        std::cout << "***** [FAIL] Test Case Private 2 Failed on Reading Empty String Record! *****" << std::endl
                  << std::endl;
        free(record);
        free(returnedData);
        return -1;
    }

    memset(record, 0, 2000);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    setAttrNull(nullsIndicator, 1, true);
    setAttrNull(nullsIndicator, 4, true);
    // Insert a record
    prepareRecordForTweetMessage(recordDescriptorForTweetMessage.size(), nullsIndicator, 1234, 0, "", 0, "", 999, 0, "",
                                 record, &recordSize);

    rc = rbfm.insertRecord(fileHandle, recordDescriptorForTweetMessage, record,
                           rid);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm.readRecord(fileHandle, recordDescriptorForTweetMessage, rid,
                         returnedData);
    assert(rc == success && "Reading a record should not fail.");

    // An NULL should be printed for the referred_topics field.
    std::cout << "Should print NULL:" << std::endl;
    rbfm.printRecord(recordDescriptorForTweetMessage, returnedData);
    std::cout << std::endl;

    // Compare whether the two memory blocks are the same
    if (memcmp(record, returnedData, recordSize) != 0) {
        std::cout << "***** [FAIL] Test Case Private 2 Failed on Reading Nullable Record! *****" << std::endl
                  << std::endl;
        free(record);
        free(returnedData);
        return -1;
    }

    // Close the file "test_private_2"
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    int fsize = getFileSize(fileName);
    if (fsize <= 0) {
        std::cout << "File Size should not be zero at this moment." << std::endl;
        rbfm.destroyFile(fileName);
        free(record);
        free(returnedData);
        free(nullsIndicator);
        return -1;
    }

    // Destroy File
    rc = rbfm.destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    free(record);
    free(returnedData);

    std::cout << "***** RBF Test Case Private 2 Finished. The result will be examined! *****" << std::endl << std::endl;

    return 0;
}

int main() {

    remove("test_private_2");

    return RBFTest_private_2(RecordBasedFileManager::instance());
}