#ifndef CS222_FALL19_TYPES_H
#define CS222_FALL19_TYPES_H

#include <string>
#include <vector>
#include <fstream>
/**
 * ========= typedefs and shared structs ===========
 */
typedef short directory_t; // directories before real data, to indicate offset for each field
static directory_t MAX_FIELD_NUM = INT16_MAX;

typedef unsigned PageNum;
typedef int RC;

// Record ID
typedef struct {
  unsigned pageNum;    // page number
  unsigned short slotNum;    // slot number in the page
} RID;

// Attribute
typedef enum {
  TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef enum {
  Normal = 0, Empty, Forwarding
} DirectoryType;

typedef unsigned AttrLength;

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
  EQ_OP = 0, // no condition// =
  LT_OP,      // <
  LE_OP,      // <=
  GT_OP,      // >
  GE_OP,      // >=
  NE_OP,      // !=
  NO_OP       // no condition
} CompOp;

struct Attribute {
  std::string name;  // attribute name
  AttrType type;     // attribute type
  AttrLength length; // attribute length
};

/**
 * ========= consts ===========
 */

#define PAGE_SIZE 4096


#endif //CS222_FALL19_TYPES_H
