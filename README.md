By default you should not change those functions of pre-defined in the given .h files.
If you think some changes are really necessary, please contact us first.

If you are not using CLion and want to use command line make tool:

 - Modify the "CODEROOT" variable in makefile.inc to point to the root
  of your code base if you can't compile the code.
 
 - Finish the Record-based Files (RBF) Component:
   
   Go to folder "rbf" and test in the following order:
   ```
   make clean
   make
   ./rbftest_update
   ./rbftest_delete
   ```
  
 - and then implement the Relation Manager (RM):

   Go to folder "rm" and test in the following order:

   ```
   make clean
   make      
   ./rmtest_create_tables           
   ./rmtest_delete_tables
   ./rmtest_create_tables
   ./rmtest_00   
   ./rmtest_01  
   ./rmtest_02
   ./rmtest_03
   ./rmtest_04
   ./rmtest_05
   ./rmtest_06
   ./rmtest_07
   ./rmtest_08
   ./rmtest_09
   ./rmtest_10
   ./rmtest_11
   ./rmtest_12
   ./rmtest_13
   ./rmtest_13b
   ./rmtest_14
   ./rmtest_15
   ./rmtest_extra_1
   ./rmtest_extra_2
   
   ```

   The program should run. But initially it will generates an error. You are supposed to
   implement the API of the rest of the methods in rbfm.h and methods in rm.h as explained 
   in the project description.