#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int RBFTest_9(RecordBasedFileManager &rbfm, std::vector<RID> &rids, std::vector<int> &sizes) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Multiple Records
    // 4. Close Record-Based File
    std::cout << std::endl << "***** In RBF Test Case 09 *****" << std::endl;

    RC rc;
    std::string fileName = "test9";

    // Create a file named "test9"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file "test9"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid;
    void *record = malloc(1000);
    int numRecords = 2000;

    std::vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    for (Attribute &i : recordDescriptor) {
        std::cout << "Attr Name: " << i.name << " Attr Type: " << (AttrType) i.type
                  << " Attr Len: " << i.length << std::endl;
    }
    std::cout << std::endl;

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert 2000 records into file
    for (int i = 0; i < numRecords; i++) {
        // Test insert Record
        int size = 0;
        memset(record, 0, 1000);
        prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, record, &size);

        rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success && "Inserting a record should not fail.");

        rids.push_back(rid);
        sizes.push_back(size);
    }
    // Close the file "test9"
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    free(record);


    // Write RIDs to the disk. Do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    std::ofstream ridsFile("test9rids", std::ios::out | std::ios::trunc | std::ios::binary);

    if (ridsFile.is_open()) {
        ridsFile.seekp(0, std::ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFile.write(reinterpret_cast<const char *>(&rids[i].pageNum),
                           sizeof(unsigned));
            ridsFile.write(reinterpret_cast<const char *>(&rids[i].slotNum),
                           sizeof(unsigned));
            if (i % 1000 == 0) {
                std::cout << "RID #" << i << ": " << rids[i].pageNum << ", "
                          << rids[i].slotNum << std::endl;
            }
        }
        ridsFile.close();
    }

    // Write sizes vector to the disk. Do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    std::ofstream sizesFile("test9sizes", std::ios::out | std::ios::trunc | std::ios::binary);

    if (sizesFile.is_open()) {
        sizesFile.seekp(0, std::ios::beg);
        for (int i = 0; i < numRecords; i++) {
            sizesFile.write(reinterpret_cast<const char *>(&sizes[i]), sizeof(int));
            if (i % 1000 == 0) {
                std::cout << "Sizes #" << i << ": " << sizes[i] << std::endl;
            }
        }
        sizesFile.close();
    }

    std::cout << "RBF Test Case 09 Finished! The result will be examined." << std::endl << std::endl;
    free(nullsIndicator);
    return 0;
}

int main() {
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    remove("test9");
    remove("test9rids");
    remove("test9sizes");

    std::vector<RID> rids;
    std::vector<int> sizes;
    return RBFTest_9(rbfm, rids, sizes);
}
