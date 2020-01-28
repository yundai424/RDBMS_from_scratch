import subprocess

test_list = ["./rbftest_01",
             "./rbftest_02",
             "./rbftest_03",
             "./rbftest_04",
             "./rbftest_05",
             "./rbftest_06",
             "./rbftest_07",
             "./rbftest_08",
             "./rbftest_08b",
             "./rbftest_09",
             "./rbftest_10",
             "./rbftest_11",
             "./rbftest_12",
             "./rbftest_update",
             "./rbftest_delete",
             ]
subprocess.check_output('make clean', shell=True)
subprocess.check_output('make -j', shell=True)

try:
    for task in test_list:
        subprocess.run(task).check_returncode()
finally:
    subprocess.check_output('make clean', shell=True)
