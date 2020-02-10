By default you should not change those functions of pre-defined in the given .h files.
If you think some changes are really necessary, please contact us first.

If you are not using CLion and want to use command line make tool:

 - Modify the "CODEROOT" variable in makefile.inc to point to the root
  of your code base if you can't compile the code.
 
 - Implement the Index Manager (IX):

   Go to folder "ix" and test in the following order:

   ```
   make clean
   make      
   ./ixtest_01  
   ./ixtest_02
   ./ixtest_03
   ./ixtest_04
   ./ixtest_05
   ./ixtest_06
   ./ixtest_07
   ./ixtest_08
   ./ixtest_09
   ./ixtest_10
   ./ixtest_11
   ./ixtest_12
   ./ixtest_13
   ./ixtest_14
   ./ixtest_15
   ./ixtest_extra_01
   ./ixtest_extra_02   
   ./ixtest_p1  
   ./ixtest_p2
   ./ixtest_p3
   ./ixtest_p4
   ./ixtest_p5
   ./ixtest_p6    
   ./ixtest_pe_01
   ./ixtest_pe_02
   
   ```

   The program should run. But initially it will generates an error. You are supposed to
   implement the API of the rest of the methods in ix.h as explained in the project description.