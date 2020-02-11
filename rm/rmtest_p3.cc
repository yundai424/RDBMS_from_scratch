#include "rm_test_util.h"

RC TEST_RM_PRIVATE_3(const std::string &tableName) {
    // Functions tested
    // 1. Simple Scan
    std::cout << std::endl << "***** In RM Test Case Private 3 *****" << std::endl;

    RID rid;
    int numTuples = 100000;
    void *returnedData = malloc(300);
    std::set<int> user_ids;

    // Read UserIds that was created in the private test 1
    readUserIdsFromDisk(user_ids, numTuples);

    // Set up the iterator
    RM_ScanIterator rmsi;
    std::string attr = "userid";
    std::vector<std::string> attributes;
    attributes.push_back(attr);
    RC rc = rm.scan(tableName, "", NO_OP, NULL, attributes, rmsi);
    if (rc != success) {
        std::cout << "***** RM Test Case Private 3 failed. *****" << std::endl << std::endl;
        return -1;
    }

    int userid = 0;
    while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
        userid = *(int *) ((char *) returnedData + 1);

        if (user_ids.find(userid) == user_ids.end()) {
            std::cout << "***** RM Test Case Private 3 failed. *****" << std::endl << std::endl;
            rmsi.close();
            free(returnedData);
            return -1;
        }
    }
    rmsi.close();
    free(returnedData);

    std::cout << "***** RM Test Case Private 3 finished. The result will be examined. *****" << std::endl << std::endl;
    return 0;
}

int main() {
    return TEST_RM_PRIVATE_3("tbl_private_1");
}
