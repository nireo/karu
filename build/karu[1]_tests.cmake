add_test( EncoderTest.RawHeader /home/eemil/dev/karu/build/karu [==[--gtest_filter=EncoderTest.RawHeader]==] --gtest_also_run_disabled_tests)
set_tests_properties( EncoderTest.RawHeader PROPERTIES WORKING_DIRECTORY /home/eemil/dev/karu/build SKIP_REGULAR_EXPRESSION [==[\[  SKIPPED \]]==])
add_test( MemtableTest.LogFileCreated /home/eemil/dev/karu/build/karu [==[--gtest_filter=MemtableTest.LogFileCreated]==] --gtest_also_run_disabled_tests)
set_tests_properties( MemtableTest.LogFileCreated PROPERTIES WORKING_DIRECTORY /home/eemil/dev/karu/build SKIP_REGULAR_EXPRESSION [==[\[  SKIPPED \]]==])
add_test( MemtableTest.BasicOperations /home/eemil/dev/karu/build/karu [==[--gtest_filter=MemtableTest.BasicOperations]==] --gtest_also_run_disabled_tests)
set_tests_properties( MemtableTest.BasicOperations PROPERTIES WORKING_DIRECTORY /home/eemil/dev/karu/build SKIP_REGULAR_EXPRESSION [==[\[  SKIPPED \]]==])
add_test( SSTableTest.TestBuildFromBTree /home/eemil/dev/karu/build/karu [==[--gtest_filter=SSTableTest.TestBuildFromBTree]==] --gtest_also_run_disabled_tests)
set_tests_properties( SSTableTest.TestBuildFromBTree PROPERTIES WORKING_DIRECTORY /home/eemil/dev/karu/build SKIP_REGULAR_EXPRESSION [==[\[  SKIPPED \]]==])
set( karu_TESTS EncoderTest.RawHeader MemtableTest.LogFileCreated MemtableTest.BasicOperations SSTableTest.TestBuildFromBTree)