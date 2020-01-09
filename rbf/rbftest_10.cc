#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int RBFTest_10(RecordBasedFileManager &rbfm) {
    // Functions tested
    // 1. Open Record-Based File
    // 2. Read Multiple Records
    // 3. Close Record-Based File
    // 4. Destroy Record-Based File
    std::cout << std::endl << "***** In RBF Test Case 10 *****" << std::endl;

    RC rc;
    std::string fileName = "test9";

    // Open the file "test9"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    int numRecords = 2000;
    void *record = malloc(1000);
    void *returnedData = malloc(1000);

    std::vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    std::vector<RID> rids;
    std::vector<int> sizes;
    RID tempRID;

    // Read rids from the disk - do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    std::ifstream ridsFileRead("test9rids", std::ios::in | std::ios::binary);

    unsigned pageNum;
    unsigned slotNum;

    if (ridsFileRead.is_open()) {
        ridsFileRead.seekg(0, std::ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFileRead.read(reinterpret_cast<char *>(&pageNum), sizeof(unsigned));
            ridsFileRead.read(reinterpret_cast<char *>(&slotNum), sizeof(unsigned));
            if (i % 1000 == 0) {
                std::cout << "loaded RID #" << i << ": " << pageNum << ", " << slotNum << std::endl;
            }
            tempRID.pageNum = pageNum;
            tempRID.slotNum = slotNum;
            rids.push_back(tempRID);
        }
        ridsFileRead.close();
    }

    assert(rids.size() == (unsigned) numRecords && "Reading records should not fail.");

    // Read sizes vector from the disk - do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    std::ifstream sizesFileRead("test9sizes", std::ios::in | std::ios::binary);

    int tempSize;

    if (sizesFileRead.is_open()) {
        sizesFileRead.seekg(0, std::ios::beg);
        for (int i = 0; i < numRecords; i++) {
            sizesFileRead.read(reinterpret_cast<char *>(&tempSize), sizeof(int));
            if (i % 1000 == 0) {
                std::cout << "loaded Sizes #" << i << ": " << tempSize << std::endl;
            }
            sizes.push_back(tempSize);
        }
        sizesFileRead.close();
    }

    assert(sizes.size() == (unsigned) numRecords && "Reading records should not fail.");

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    for (int i = 0; i < numRecords; i++) {
        memset(record, 0, 1000);
        memset(returnedData, 0, 1000);
        rc = rbfm.readRecord(fileHandle, recordDescriptor, rids[i], returnedData);
        assert(rc == success && "Reading a record should not fail.");

        if (i % 1000 == 0) {
            std::cout << std::endl << "Returned Data:" << std::endl;
            rbfm.printRecord(recordDescriptor, returnedData);
        }

        int size = 0;
        prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, record, &size);
        if (memcmp(returnedData, record, sizes[i]) != 0) {
            std::cout << "[FAIL] Test Case 10 Failed!" << std::endl << std::endl;
            free(nullsIndicator);
            free(record);
            free(returnedData);
            return -1;
        }
    }

    std::cout << std::endl;

    // Close the file "test9"
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    rc = rbfm.destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    free(nullsIndicator);
    free(record);
    free(returnedData);

    std::cout << "RBF Test Case 10 Finished! The result will be examined." << std::endl << std::endl;

    remove("test9sizes");
    remove("test9rids");

    return 0;
}

int main() {
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    return RBFTest_10(rbfm);
}
