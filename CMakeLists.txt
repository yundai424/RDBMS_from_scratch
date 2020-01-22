cmake_minimum_required(VERSION 2.8)
project(cs222p_winter20)

set(CMAKE_CXX_STANDARD 11)

add_custom_target(clean-all
        COMMAND rm Index* Indices* left* right* large* group* *out Tables Columns tbl_* *_file *idx)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -O1 -g  -fno-omit-frame-pointer")
if (CMAKE_BUILD_TYPE MATCHES Debug)
    add_definitions(-DDEBUG=1)
endif()

add_library(PFM ./rbf/pfm.cc)
add_library(RBFM ./rbf/rbfm.cc)
add_library(RM ./rm/rm.cc ${RBFM})


file(GLOB files rbf/rbftest*.cc)
foreach(file ${files})
    get_filename_component(name ${file} NAME_WE)
    add_executable(${name} ${file})
    target_link_libraries(${name} RBFM PFM)
endforeach()

file(GLOB files rm/rmtest_*.cc)
foreach(file ${files})
    get_filename_component(name ${file} NAME_WE)
    add_executable(${name} ${file})
    target_link_libraries(${name} RM RBFM PFM)
endforeach()