#include "rm_test_util.h"

RC TEST_RM_PRIVATE_8(const std::string &tableName) {
    // Functions tested
    // 1. Delete Tuples
    // 2. Scan Empty table
    // 3. Delete Table 
    std::cout << std::endl << "***** In RM Test Case Private 8 *****" << std::endl;

    RC rc;
    RID rid;
    int numTuples = 100000;
    void *returnedData = malloc(300);
    std::vector<RID> rids;
    std::vector<std::string> attributes;

    attributes.emplace_back("tweetid");
    attributes.emplace_back("userid");
    attributes.emplace_back("sender_location");
    attributes.emplace_back("send_time");
    attributes.emplace_back("referred_topics");
    attributes.emplace_back("message_text");

    readRIDsFromDisk(rids, numTuples);

    for (int i = 0; i < numTuples; i++) {
        rc = rm.deleteTuple(tableName, rids[i]);
        if (rc != success) {
            free(returnedData);
            std::cout << "***** RelationManager::deleteTuple() failed. RM Test Case Private 8 failed *****" << std::endl
                      << std::endl;
            return -1;
        }

        rc = rm.readTuple(tableName, rids[i], returnedData);
        if (rc == success) {
            free(returnedData);
            std::cout
                    << "***** RelationManager::readTuple() should fail at this point. RM Test Case Private 8 failed *****"
                    << std::endl << std::endl;
            return -1;
        }

        if (i % 10000 == 0) {
            std::cout << (i + 1) << " / " << numTuples << " have been processed." << std::endl;
        }
    }
    std::cout << "All records have been processed." << std::endl;

    // Set up the iterator
    RM_ScanIterator rmsi3;
    rc = rm.scan(tableName, "", NO_OP, NULL, attributes, rmsi3);
    if (rc != success) {
        free(returnedData);
        std::cout << "***** RelationManager::scan() failed. RM Test Case Private 8 failed. *****" << std::endl
                  << std::endl;
        return -1;
    }

    if (rmsi3.getNextTuple(rid, returnedData) != RM_EOF) {
        std::cout
                << "***** RM_ScanIterator::getNextTuple() should fail at this point. RM Test Case Private 8 failed. *****"
                << std::endl << std::endl;
        rmsi3.close();
        free(returnedData);
        return -1;
    }
    rmsi3.close();
    free(returnedData);

    // Delete a Table
    rc = rm.deleteTable(tableName);
    if (rc != success) {
        std::cout << "***** RelationManager::deleteTable() failed. RM Test Case Private 8 failed. *****" << std::endl
                  << std::endl;
        return -1;
    }

    std::cout << "***** RM Test Case Private 8 finished. The result will be examined. *****" << std::endl << std::endl;
    return 0;
}

int main() {
    return TEST_RM_PRIVATE_8("tbl_private_1");

}
