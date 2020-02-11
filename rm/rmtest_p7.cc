#include "rm_test_util.h"

int TEST_RM_PRIVATE_7(const std::string &tableName) {
    // Functions tested
    // 1. Scan Table
    std::cout << std::endl << "***** In RM Test Case Private 7 *****" << std::endl;

    RID rid;
    std::vector<std::string> attributes;
    void *returnedData = malloc(300);

    void *value = malloc(20);
    std::string msg = "UpdatedMsg00100";
    int msgLength = 15;

    memcpy((char *) value, &msgLength, 4);
    memcpy((char *) value + 4, msg.c_str(), msgLength);

    std::string attr = "message_text";
    attributes.emplace_back("sender_location");
    attributes.emplace_back("send_time");

    RM_ScanIterator rmsi2;
    RC rc = rm.scan(tableName, attr, EQ_OP, value, attributes, rmsi2);
    if (rc != success) {
        free(returnedData);
        std::cout << "***** RM Test Case Private 7 failed. *****" << std::endl << std::endl;
        return -1;
    }

    float sender_location;
    float send_time;
    int counter = 0;

    while (rmsi2.getNextTuple(rid, returnedData) != RM_EOF) {
        counter++;
        if (counter > 1) {
            std::cout << "***** A wrong entry was returned. RM Test Case Private 7 failed *****" << std::endl
                      << std::endl;
            rmsi2.close();
            free(returnedData);
            free(value);
            return -1;
        }

        sender_location = *(float *) ((char *) returnedData + 1);
        send_time = *(float *) ((char *) returnedData + 5);

        if (!(sender_location == 2200.0 || send_time == 5000.0)) {
            std::cout << "***** A wrong entry was returned. RM Test Case Private 7 failed *****" << std::endl
                      << std::endl;
            rmsi2.close();
            free(returnedData);
            free(value);
            return -1;
        }
    }

    rmsi2.close();
    free(returnedData);
    free(value);

    std::cout << "***** RM Test Case Private 7 finished. The result will be examined. *****" << std::endl << std::endl;

    return 0;
}

int main() {
    return TEST_RM_PRIVATE_7("tbl_private_1");

}
