#include "pfm.h"
#include "test_util.h"

int RBFTest_1(PagedFileManager &pfm) {
    // Functions Tested:
    // 1. Create File
    std::cout << std::endl << "***** In RBF Test Case 01 *****" << std::endl;

    RC rc;
    std::string fileName = "test1";

    // Create a file named "test"
    rc = pfm.createFile(fileName);
    assert(rc == success && "Creating the file failed.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Create "test" again, should fail
    rc = pfm.createFile(fileName);
    assert(rc != success && "Creating the same file should fail.");

    std::cout << "RBF Test Case 01 Finished! The result will be examined." << std::endl << std::endl;
    return 0;
}

int main() {
    // To test the functionality of the paged file manager
    PagedFileManager &pfm = PagedFileManager::instance();

    // Remove files that might be created by previous test run
    remove("test1");

    return RBFTest_1(pfm);
}
