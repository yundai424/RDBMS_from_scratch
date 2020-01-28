import subprocess

test_list = ['./rmtest_create_tables',
             './rmtest_delete_tables',
             './rmtest_create_tables',
             './rmtest_00',
             './rmtest_01',
             './rmtest_02',
             './rmtest_03',
             './rmtest_04',
             './rmtest_05',
             './rmtest_06',
             './rmtest_07',
             './rmtest_08',
             './rmtest_09',
             './rmtest_10',
             './rmtest_11',
             './rmtest_12',
             './rmtest_13',
             './rmtest_13b',
             './rmtest_14',
             './rmtest_15']
subprocess.check_output('make clean', shell=True)
subprocess.check_output('make -j', shell=True)

try:
    for task in test_list:
        subprocess.run(task).check_returncode()
finally:
    subprocess.check_output('make clean', shell=True)
