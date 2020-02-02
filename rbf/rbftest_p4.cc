#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int RBFTest_private_4(RecordBasedFileManager &rbfm) {
    // Functions Tested:
    // 1. Create a File - test_private_4a
    // 2. Create a File - test_private_4b
    // 3. Open test_private_4a
    // 4. Open test_private_4b
    // 5. Insert 50000 records into test_private_4a
    // 6. Insert 50000 records into test_private_4b
    // 7. Close test_private_4a
    // 8. Close test_private_4b
    std::cout << std::endl << "***** In RBF Test Case Private 4 ****" << std::endl;

    RC rc;
    std::string fileName = "test_private_4a";
    std::string fileName2 = "test_private_4b";

    // Create a file named "test_private_4a"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating a file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating a file should not fail.");

    // Create a file named "test_private_4b"
    rc = rbfm.createFile(fileName2);
    assert(rc == success && "Creating a file should not fail.");

    rc = createFileShouldSucceed(fileName2);
    assert(rc == success && "Creating a file should not fail.");

    // Open the file "test_private_4a"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    // Open the file "test_private_4b"
    FileHandle fileHandle2;
    rc = rbfm.openFile(fileName2, fileHandle2);
    assert(rc == success && "Opening the file should not fail.");

    RID rid, rid2;
    void *record = malloc(1000);
    void *record2 = malloc(1000);
    void *returnedData = malloc(1000);
    void *returnedData2 = malloc(1000);
    int numRecords = 50000;
    int batchSize = 1000;

    std::vector<Attribute> recordDescriptorForTwitterUser, recordDescriptorForTweetMessage;

    createRecordDescriptorForTwitterUser(recordDescriptorForTwitterUser);
    createRecordDescriptorForTweetMessage(recordDescriptorForTweetMessage);

    // NULL field indicator

    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptorForTwitterUser.size());
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    int nullFieldsIndicatorActualSize2 = getActualByteForNullsIndicator(recordDescriptorForTweetMessage.size());
    unsigned char nullsIndicator2[nullFieldsIndicatorActualSize2];
    memset(nullsIndicator2, 0, nullFieldsIndicatorActualSize2);

    std::vector<RID> rids, rids2;
    // Insert 50,000 records into the file - test_private_4a and test_private_4b
    for (int i = 0; i < numRecords / batchSize; i++) {
        for (int j = 0; j < batchSize; j++) {
            memset(record, 0, 1000);
            int size = 0;

            prepareLargeRecordForTwitterUser(recordDescriptorForTwitterUser.size(), nullsIndicator, i * batchSize + j,
                                             record, &size);
            rc = rbfm.insertRecord(fileHandle, recordDescriptorForTwitterUser, record, rid);
            assert(rc == success && "Inserting a record for the file #1 should not fail.");
            rids.push_back(rid);
        }
        for (int j = 0; j < batchSize; j++) {
            memset(record2, 0, 1000);
            int size2 = 0;

            prepareLargeRecordForTweetMessage(recordDescriptorForTweetMessage.size(), nullsIndicator2,
                                              i * batchSize + j, record2, &size2);

            rc = rbfm.insertRecord(fileHandle2, recordDescriptorForTweetMessage, record2, rid2);
            assert(rc == success && "Inserting a record  for the file #2 should not fail.");

            rids2.push_back(rid2);
        }

        if (i % 5 == 0 && i != 0) {
            std::cout << i << " / " << numRecords / batchSize << " batches (" << numRecords
                      << " records) inserted so far for both files." << std::endl;
        }
    }

    std::cout << "Inserting " << numRecords << " records done for the both files." << std::endl << std::endl;

    // Close the file - test_private_4a
    rc = rbfm.closeFile(fileHandle);
    if (rc != success) {
        return -1;
    }
    assert(rc == success);

    free(record);
    free(returnedData);

    if (rids.size() != numRecords) {
        return -1;
    }

    // Close the file - test_private_4b
    rc = rbfm.closeFile(fileHandle2);
    if (rc != success) {
        return -1;
    }
    assert(rc == success);

    free(record2);
    free(returnedData2);

    if (rids2.size() != numRecords) {
        return -1;
    }

    int fsize = getFileSize(fileName);
    if (fsize <= 0) {
        std::cout << "File Size should not be zero at this moment." << std::endl;
        std::cout << "***** [FAIL] Test Case Private 4 Failed! *****" << std::endl << std::endl;
        return -1;
    }

    fsize = getFileSize(fileName2);
    if (fsize <= 0) {
        std::cout << "File Size should not be zero at this moment." << std::endl;
        std::cout << "***** [FAIL] Test Case Private 4 Failed! *****" << std::endl << std::endl;
        return -1;
    }

    // Write RIDs of test_private_4a to a disk - do not use this code. This is not a page-based operation. For test purpose only.
    ofstream ridsFile("test_private_4a_rids", ios::out | ios::trunc | ios::binary);

    if (ridsFile.is_open()) {
        ridsFile.seekp(0, ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFile.write(reinterpret_cast<const char *>(&rids.at(i).pageNum), sizeof(unsigned));
            ridsFile.write(reinterpret_cast<const char *>(&rids.at(i).slotNum), sizeof(unsigned));
        }
        ridsFile.close();
        std::cout << std::endl << std::endl;
    }

    // Write RIDs of test_private_4b to a disk - do not use this code. This is not a page-based operation. For test purpose only.
    ofstream ridsFile2("test_private_4b_rids", ios::out | ios::trunc | ios::binary);

    if (ridsFile2.is_open()) {
        ridsFile2.seekp(0, ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFile2.write(reinterpret_cast<const char *>(&rids2.at(i).pageNum), sizeof(unsigned));
            ridsFile2.write(reinterpret_cast<const char *>(&rids2.at(i).slotNum), sizeof(unsigned));
        }
        ridsFile2.close();
    }

    std::cout << "***** RBF Test Case Private 4 Finished. The result will be examined. *****" << std::endl << std::endl;

    return 0;
}

int main() {

    remove("test_private_4a");
    remove("test_private_4b");
    remove("test_private_4a_rids");
    remove("test_private_4b_rids");

    return RBFTest_private_4(RecordBasedFileManager::instance());

}