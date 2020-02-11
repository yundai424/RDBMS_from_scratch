#include "rm_test_util.h"
#include <random>

RC TEST_RM_PRIVATE_6(const std::string &tableName) {
    // Functions tested
    // 1. Update tuples
    // 2. Read Attribute
    std::cout << std::endl << "***** In RM Test Case Private 6 *****" << std::endl;

    unsigned tupleSize = 0;
    int numTuples = 100000;
    void *tuple;
    void *returnedData = malloc(300);

    std::vector<RID> rids;
    std::vector<char *> tuples;
    std::set<int> user_ids;
    RC rc = 0;

    readRIDsFromDisk(rids, numTuples);

    // GetAttributes
    std::vector<Attribute> attrs;
    rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    int updateCount = 0;

    for (int i = 0; i < numTuples; i = i + 50) {
        tuple = malloc(300);

        // Update Tuple
        float sender_location = (float) i + 2100;
        float send_time = (float) i + 4900;
        int tweetid = i;
        int userid = i + i % 222;
        std::stringstream ss;
        ss << std::setw(5) << std::setfill('0') << i;
        std::string msg = "UpdatedMsg" + ss.str();
        std::string referred_topics = "UpdatedRto" + ss.str();

        prepareTweetTuple(attrs.size(), nullsIndicator, tweetid, userid, sender_location, send_time,
                          referred_topics.size(), referred_topics, msg.size(), msg, tuple, &tupleSize);

        // Update tuples
        rc = rm.updateTuple(tableName, tuple, rids[i]);
        assert(rc == success && "RelationManager::updateTuple() should not fail.");

        if (i % 10000 == 0) {
            std::cout << (i + 1) << "/" << numTuples << " records have been processed so far." << std::endl;
        }
        updateCount++;

        tuples.push_back((char *) tuple);
    }
    std::cout << "All records have been processed - update count: " << updateCount << std::endl;

    bool testFail = false;
    std::string attributeName;

    int readCount = 0;
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_real_distribution<double> dist(0, 6);
    // Integrity check
    for (int i = 0; i < numTuples; i = i + 50) {
        int attrID = dist(mt);
        if (attrID == 0) {
            attributeName = "tweetid";
        } else if (attrID == 1) {
            attributeName = "userid";
        } else if (attrID == 2) {
            attributeName = "sender_location";
        } else if (attrID == 3) {
            attributeName = "send_time";
        } else if (attrID == 4) {
            attributeName = "referred_topics";
        } else if (attrID == 5) {
            attributeName = "message_text";
        }
        rc = rm.readAttribute(tableName, rids[i], attributeName, returnedData);
        assert(rc == success && "RelationManager::readAttribute() should not fail.");

        int value = 0;
        float fvalue = 0;
        std::stringstream ss;
        ss << std::setw(5) << std::setfill('0') << i;
        std::string msgToCheck = "UpdatedMsg" + ss.str();
        std::string referred_topicsToCheck = "UpdatedRto" + ss.str();

        // tweetid
        if (attrID == 0) {
            if (memcmp(((char *) returnedData + 1), ((char *) tuples.at(readCount) + 1), 4) != 0) {
                testFail = true;
            } else {
                value = *(int *) ((char *) returnedData + 1);
                if (value != i) {
                    testFail = true;
                }
            }
            // userid
        } else if (attrID == 1) {
            if (memcmp(((char *) returnedData + 1), ((char *) tuples.at(readCount) + 5), 4) != 0) {
                testFail = true;
            } else {
                value = *(int *) ((char *) returnedData + 1);
                if (value != (i + i % 222)) {
                    testFail = true;
                }
            }
            // sender_location
        } else if (attrID == 2) {
            if (memcmp(((char *) returnedData + 1), ((char *) tuples.at(readCount) + 9), 4) != 0) {
                testFail = true;
            } else {
                fvalue = *(float *) ((char *) returnedData + 1);
                if (fvalue != ((float) i + 2100)) {
                    testFail = true;
                }
            }
            // send_time
        } else if (attrID == 3) {
            if (memcmp(((char *) returnedData + 1), ((char *) tuples.at(readCount) + 13), 4) != 0) {
                testFail = true;
            } else {
                fvalue = *(float *) ((char *) returnedData + 1);
                if (fvalue != ((float) i + 4900)) {
                    testFail = true;
                }
            }

            // referred_topics
        } else if (attrID == 4) {
            if (memcmp(((char *) returnedData + 5), ((char *) tuples.at(readCount) + 21), 15) != 0) {
                testFail = true;
            } else {
                std::string strToCheck(((char *) returnedData + 5), 15);
                if (strToCheck != referred_topicsToCheck) {
                    testFail = true;
                }
            }
            // message_text
        } else if (attrID == 5) {
            if (memcmp(((char *) returnedData + 5), ((char *) tuples.at(readCount) + 40), 15) != 0) {
                testFail = true;
            } else {
                std::string strToCheck(((char *) returnedData + 5), 15);
                if (strToCheck != msgToCheck) {
                    testFail = true;
                }
            }
        }

        if (testFail) {
            std::cout << "***** RM Test Case Private 6 failed on " << i << "th tuple - attr: " << attrID << "*****"
                      << std::endl << std::endl;
            free(returnedData);
            for (int j = 0; j < numTuples; j++) {
                free(tuples[j]);
            }
            rm.deleteTable(tableName);
            remove("rids_file");
            remove("user_ids_file");

            return -1;
        }
        readCount++;
    }

    free(returnedData);
    for (int i = 0; i < updateCount; i++) {
        free(tuples[i]);
    }

    std::cout << "***** RM Test Case Private 6 finished. The result will be examined. *****" << std::endl << std::endl;

    return 0;
}

int main() {
    return TEST_RM_PRIVATE_6("tbl_private_1");

}
