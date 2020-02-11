#include "rm_test_util.h"

int TEST_RM_PRIVATE_5(const std::string &tableName) {
    // Functions tested
    // 1. Scan Table (VarChar with Nulls)
    std::cout << std::endl << "***** In RM Test Case Private 5 *****" << std::endl;

    RID rid;
    std::vector<std::string> attributes;
    void *returnedData = malloc(300);

    void *value = malloc(16);
    std::string msg = "Msg00250";
    int msgLength = 8;

    memcpy((char *) value, &msgLength, 4);
    memcpy((char *) value + 4, msg.c_str(), msgLength);

    std::string attr = "message_text";
    attributes.emplace_back("sender_location");
    attributes.emplace_back("send_time");

    RM_ScanIterator rmsi2;
    RC rc = rm.scan(tableName, attr, LT_OP, value, attributes, rmsi2);
    if (rc != success) {
        free(returnedData);
        std::cout << "***** RM Test Case Private 5 failed. *****" << std::endl << std::endl;
        return -1;
    }

    float sender_location;
    float send_time;
    bool nullBit;
    int counter = 0;

    while (rmsi2.getNextTuple(rid, returnedData) != RM_EOF) {

        // There are 2 tuples whose message_text value is NULL
        // For these tuples, send_time is also NULL. These NULLs should not be returned.
        nullBit = *(unsigned char *) ((char *) returnedData) & ((unsigned) 1 << (unsigned) 6);

        if (nullBit) {
            std::cout << "***** A wrong entry was returned. RM Test Case Private 5 failed *****" << std::endl
                      << std::endl;
            rmsi2.close();
            free(returnedData);
            free(value);
            return -1;
        }
        counter++;

        sender_location = *(float *) ((char *) returnedData + 1);
        send_time = *(float *) ((char *) returnedData + 5);

        if (!(sender_location >= 0.0 || sender_location <= 249.0 || send_time >= 2000.0 || send_time <= 2249.0)) {
            std::cout << "***** A wrong entry was returned. RM Test Case Private 5 failed *****" << std::endl
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

    if (counter != 242) {
        std::cout << "***** The number of returned tuple: " << counter
                  << " is not correct. RM Test Case Private 5 failed *****" << std::endl << std::endl;
    } else {
        std::cout << "***** RM Test Case Private 5 finished. The result will be examined. *****" << std::endl
                  << std::endl;
    }
    return 0;
}

int main() {
    // Using a table with Null values
    return TEST_RM_PRIVATE_5("tbl_private_2");

}
