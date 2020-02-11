#include "rm_test_util.h"

RC TEST_RM_PRIVATE_9(const std::string &tableName) {
    // Functions tested
    // An attempt to modify System Catalogs tables - should fail
    std::cout << std::endl << "***** In RM Test Case Private 9 *****" << std::endl;

    RID rid;
    std::vector<Attribute> attrs;
    void *returnedData = malloc(200);

    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttribute() should not fail.");

    // print attribute name
    std::cout << "Tables table: (";
    for (size_t i = 0; i < attrs.size(); i++) {
        if (i < attrs.size() - 1)
            std::cout << attrs[i].name << ", ";
        else
            std::cout << attrs[i].name << ")" << std::endl << std::endl;
    }

    // Try to insert a row - should fail
    int offset = 0;
    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    void *buffer = malloc(1000);

    // Nulls-indicator
    memcpy((char *) buffer + offset, nullsIndicator, nullAttributesIndicatorActualSize);
    offset += nullAttributesIndicatorActualSize;

    int intValue = 0;
    int varcharLength = 7;
    std::string varcharStr = "Testing";
    float floatValue;

    for (auto &attr : attrs) {
        // Generating INT value
        if (attr.type == TypeInt) {
            intValue = 9999;
            memcpy((char *) buffer + offset, &intValue, sizeof(int));
            offset += sizeof(int);
        } else if (attr.type == TypeReal) {
            // Generating FLOAT value
            floatValue = 9999.9;
            memcpy((char *) buffer + offset, &floatValue, sizeof(float));
            offset += sizeof(float);
        } else if (attr.type == TypeVarChar) {
            // Generating VarChar value
            memcpy((char *) buffer + offset, &varcharLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, varcharStr.c_str(), varcharLength);
            offset += varcharLength;
        }
    }

    rc = rm.insertTuple(tableName, buffer, rid);
    if (rc == success) {
        std::cout
                << "***** [FAIL] The system catalog should not be altered by a user's insertion call. RM Test Case Private 9 failed. *****"
                << std::endl;
        free(returnedData);
        free(buffer);
        return -1;
    }

    // Try to delete the system catalog
    rc = rm.deleteTable(tableName);
    if (rc == success) {
        std::cout
                << "***** [FAIL] The system catalog should not be deleted by a user call. RM Test Case Private 9 failed. *****"
                << std::endl;
        free(returnedData);
        free(buffer);
        return -1;
    }

    // Set up the iterator
    RM_ScanIterator rmsi;
    std::vector<std::string> projected_attrs;
    if (attrs[1].name == "table-name") {
        projected_attrs.push_back(attrs[1].name);
    } else {
        std::cout
                << "***** [FAIL] The system catalog implementation is not correct. RM Test Case Private 9 failed. *****"
                << std::endl;
        free(returnedData);
        free(buffer);
        return -1;
    }

    rc = rm.scan(tableName, "", NO_OP, NULL, projected_attrs, rmsi);
    assert(rc == success && "RelationManager::scan() should not fail.");

    int counter = 0;
    while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
        counter++;
    }
    rmsi.close();

    // At least two system catalog tables exist (Tables and Columns)
    if (counter < 3) {
        std::cout
                << "***** [FAIL] The system catalog implementation is not correct. RM Test Case Private 9 failed. *****"
                << std::endl;
        free(returnedData);
        free(buffer);
        return -1;
    }

    std::cout << "***** RM Test Case Private 9 finished. The result will be examined. *****" << std::endl << std::endl;
    free(returnedData);
    free(buffer);
    return 0;
}

int main() {
    return TEST_RM_PRIVATE_9("Tables");

}
