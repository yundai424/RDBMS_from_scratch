#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int RBFTest_private_5(RecordBasedFileManager &rbfm) {
    // Functions Tested:
    // 1. Open the File created by test_private_4 - test_private_4a
    // 2. Read entire records - test_private_4a
    // 3. Check correctness
    // 4. Close the File - test_private_4a
    // 5. Destroy the File - test_private_4a
    // 6. Open the File created by test_private_4 - test_private_4b
    // 7. Read entire records - test_private_4b
    // 8. Check correctness
    // 9. Close the File - test_private_4b
    // 10. Destroy the File - test_private_4b

    std::cout << std::endl << "***** In RBF Test Case Private 5 *****" << std::endl;

    RC rc;
    std::string fileName = "test_private_4a";
    std::string fileName2 = "test_private_4b";

    // Open the file "test_private_4a"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening a file should not fail.");

    void *record = malloc(1000);
    void *returnedData = malloc(1000);
    unsigned numRecords = 5000;

    std::vector<Attribute> recordDescriptorForTwitterUser, recordDescriptorForTweetMessage;
    createRecordDescriptorForTwitterUser(recordDescriptorForTwitterUser);

    std::vector<RID> rids, rids2;
    RID tempRID, tempRID2;

    // Read rids from the disk - do not use this code. This is not a page-based operation. For test purpose only.
    ifstream ridsFileRead("test_private_4a_rids", ios::in | ios::binary);

    unsigned pageNum;
    unsigned slotNum;

    if (ridsFileRead.is_open()) {
        ridsFileRead.seekg(0, ios::beg);
        for (unsigned i = 0; i < numRecords; i++) {
            ridsFileRead.read(reinterpret_cast<char *>(&pageNum), sizeof(unsigned));
            ridsFileRead.read(reinterpret_cast<char *>(&slotNum), sizeof(unsigned));
            tempRID.pageNum = pageNum;
            tempRID.slotNum = slotNum;
            rids.push_back(tempRID);
        }
        ridsFileRead.close();
    }

    if (rids.size() != numRecords) {
        return -1;
    }

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptorForTwitterUser.size());
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Compare records from the disk read with the record created from the method
    for (unsigned i = 0; i < numRecords; i++) {
        memset(record, 0, 1000);
        memset(returnedData, 0, 1000);
        rc = rbfm.readRecord(fileHandle, recordDescriptorForTwitterUser, rids[i], returnedData);
        if (rc != success) {
            return -1;
        }
        assert(rc == success);

        int size = 0;
        prepareLargeRecordForTwitterUser(recordDescriptorForTwitterUser.size(), nullsIndicator, (int) i, record, &size);
        if (memcmp(returnedData, record, size) != 0) {
            std::cout << "***** [FAIL] Comparison failed - RBF Test Case Private 5 Failed! *****" << i << std::endl
                      << std::endl;
            free(record);
            free(returnedData);
            return -1;
        }
    }

    // Close the file
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing a file should not fail.");

    int fsize = getFileSize(fileName);
    if (fsize <= 0) {
        std::cout << "File Size should not be zero at this moment." << std::endl;
        std::cout << "***** [FAIL] RBF Test Case Private 5 Failed! *****" << std::endl << std::endl;
        return -1;
    }

    // Destroy File
    rc = rbfm.destroyFile(fileName);
    assert(rc == success && "Destroying a file should not fail.");


    // Open the file "test_private_4b"
    FileHandle fileHandle2;
    rc = rbfm.openFile(fileName2, fileHandle2);
    assert(rc == success && "Opening a file should not fail.");

    createRecordDescriptorForTweetMessage(recordDescriptorForTweetMessage);

    std::cout << std::endl;

    // NULL field indicator
    int nullFieldsIndicatorActualSize2 = getActualByteForNullsIndicator(recordDescriptorForTweetMessage.size());
    unsigned char nullsIndicator2[nullFieldsIndicatorActualSize2];
    memset(nullsIndicator2, 0, nullFieldsIndicatorActualSize2);

    // Read rids from the disk - do not use this code. This is not a page-based operation. For test purpose only.
    ifstream ridsFileRead2("test_private_4b_rids", ios::in | ios::binary);

    if (ridsFileRead2.is_open()) {
        ridsFileRead2.seekg(0, ios::beg);
        for (unsigned i = 0; i < numRecords; i++) {
            ridsFileRead2.read(reinterpret_cast<char *>(&pageNum), sizeof(unsigned));
            ridsFileRead2.read(reinterpret_cast<char *>(&slotNum), sizeof(unsigned));
            tempRID2.pageNum = pageNum;
            tempRID2.slotNum = slotNum;
            rids2.push_back(tempRID2);
        }
        ridsFileRead2.close();
    }

    if (rids2.size() != numRecords) {
        return -1;
    }

    // Compare records from the disk read with the record created from the method
    for (unsigned i = 0; i < numRecords; i++) {
        memset(record, 0, 1000);
        memset(returnedData, 0, 1000);
        rc = rbfm.readRecord(fileHandle2, recordDescriptorForTweetMessage, rids2[i], returnedData);
        if (rc != success) {
            return -1;
        }
        assert(rc == success);

        int size = 0;
        prepareLargeRecordForTweetMessage(recordDescriptorForTweetMessage.size(), nullsIndicator2, (int) i, record,
                                          &size);

        if (memcmp(returnedData, record, size) != 0) {
            std::cout << memcmp(returnedData, record, size)
                      << "***** [FAIL] Comparison failed - RBF Test Case Private 5 Failed! *****" << i << std::endl
                      << std::endl;
            free(record);
            free(returnedData);
            return -1;
        }
    }

    // Close the file
    rc = rbfm.closeFile(fileHandle2);
    assert(rc == success && "Closing a file should not fail.");

    fsize = getFileSize(fileName2);
    if (fsize <= 0) {
        std::cout << "File Size should not be zero at this moment." << std::endl;
        std::cout << "***** [FAIL] RBF Test Case Private 5 Failed! *****" << std::endl << std::endl;
        return -1;
    }

    // Destroy File
    rc = rbfm.destroyFile(fileName2);
    assert(rc == success && "Destroying a file should not fail.");

    free(record);
    free(returnedData);

    remove("test_private_4a_rids");
    remove("test_private_4b_rids");

    std::cout << "***** RBF Test Case Private 5 Finished. The result will be examined. *****" << std::endl << std::endl;

    return 0;
}

int main() {

    return RBFTest_private_5(RecordBasedFileManager::instance());
}