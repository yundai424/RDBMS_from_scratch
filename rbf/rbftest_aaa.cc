#include "pfm.h"
#include "test_util.h"
#include "logger.h"

void test() {
  DB_DEBUG << "test";
  DB_INFO << "test";
  DB_WARNING << "test";
  DB_ERROR << "test";
}
int main() {
    // To test the functionality of the paged file manager
    test();

    return 0;
}
