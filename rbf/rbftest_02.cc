#include "pfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_2(PagedFileManager &pfm) {
    // Functions Tested:
    // 1. Destroy File
    std::cout << std::endl << "***** In RBF Test Case 02 *****" << std::endl;

    RC rc;
    string fileName = "test1";

    rc = pfm.destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    // Destroy "test1" again, should fail
    rc = pfm.destroyFile(fileName);
    assert(rc != success && "Destroy the same file should fail.");

    std::cout << "RBF Test Case 02 Finished! The result will be examined." << std::endl << std::endl;
    return 0;
}

int main() {
    // To test the functionality of the paged file manager
    PagedFileManager &pfm = PagedFileManager::instance();

    return RBFTest_2(pfm);
}
