cmake_minimum_required(VERSION 3.16)

project(CURA_TESTS)

add_library(curatestutil STATIC
    "${CMAKE_CURRENT_SOURCE_DIR}/utilities/data_helper.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/utilities/rel_helper.cpp"
    )

set_property(TARGET curatestutil PROPERTY POSITION_INDEPENDENT_CODE ON)

target_link_libraries(curatestutil gmock gtest pthread "${CURA_LIBRARY_NAME}")

set(CURA_TEST_LIST CACHE INTERNAL "CURA_TEST_LIST")

function(ConfigureTest CMAKE_TEST_NAME CMAKE_TEST_SRC)
  add_executable(${CMAKE_TEST_NAME}
      ${CMAKE_TEST_SRC} test_common/main.cpp)
  target_link_libraries(${CMAKE_TEST_NAME} curatestutil)
  set_target_properties(${CMAKE_TEST_NAME} PROPERTIES
      RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests")
  add_test(NAME ${CMAKE_TEST_NAME} COMMAND ${CMAKE_TEST_NAME})
  set(CURA_TEST_LIST ${CURA_TEST_LIST} ${CMAKE_TEST_NAME} CACHE INTERNAL "CURA_TEST_LIST")
endfunction(ConfigureTest)

include_directories("${CMAKE_SOURCE_DIR}/src"
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${GTEST_INCLUDE_DIR}")

link_directories("${GTEST_LIBRARY_DIR}")

set(EXECUTION_TEST_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/execution/execution_tests.cpp")
ConfigureTest(EXECUTION_TEST "${EXECUTION_TEST_SRC}")

set(EXPLAIN_TEST_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/planning/explain_tests.cpp")
ConfigureTest(EXPLAIN_TEST "${EXPLAIN_TEST_SRC}")

set(EXPRESSION_TEST_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/expression/expression_tests.cpp")
ConfigureTest(EXPRESSION_TEST "${EXPRESSION_TEST_SRC}")

set(KERNEL_TEST_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/kernel/aggregate_tests.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/kernel/filter_tests.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/kernel/join_tests.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/kernel/limit_tests.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/kernel/project_tests.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/kernel/sort_tests.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/kernel/union_tests.cpp")
ConfigureTest(KERNEL_TEST "${KERNEL_TEST_SRC}")

set(PARSER_TEST_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/relational/parser_test.cpp")
ConfigureTest(PARSER_TEST "${PARSER_TEST_SRC}")
