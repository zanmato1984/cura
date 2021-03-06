cmake_minimum_required(VERSION 3.16)

project(CURA VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)

if(CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wno-error=deprecated-declarations")

  option(CMAKE_CXX11_ABI "Enable the GLIBCXX11 ABI" ON)

  if(CMAKE_CXX11_ABI)
    message(STATUS "Enabling the GLIBCXX11 ABI")
  else()
    message(STATUS "Disabling the GLIBCXX11 ABI")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
  endif(CMAKE_CXX11_ABI)
endif(CMAKE_COMPILER_IS_GNUCXX)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/" ${CMAKE_MODULE_PATH})

option(DISABLE_DEPRECATION_WARNING "Disable warnings generated from deprecated declarations." ON)
if(DISABLE_DEPRECATION_WARNING)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
endif(DISABLE_DEPRECATION_WARNING)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)
option(ENABLE_ASAN "Enable build with ASAN")
option(ENABLE_CUDF "Enable CUDF library which guides CURA to target GPU")
option(BUILD_TESTS "Configure CMake to build tests" ON)

if(ENABLE_ASAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
endif(ENABLE_ASAN)

if(ENABLE_CUDF)
  message(STATUS "Building CURA targeting GPU")
else()
  message(WARNING
    "Building CURA targeting CPU, "
    "this mode is only for purposes of demo and study on non-CUDA devices, "
    "thus some features may be unimplemented or definitely suboptimal")
endif(ENABLE_CUDF)

include(cmake/find/rapidjson.cmake)
include(cmake/find/arrow.cmake)

if(ENABLE_CUDF)
  include(cmake/find/cudf.cmake)
  add_definitions(-DUSE_CUDF)
  set(TARGET_DEPENDENT_SOURCE_SUFFIX "cu")
else()
  set(CURA_LIBRARY_FLAGS "${CURA_PACKAGE_FLAGS}_cpu")
  set(TARGET_DEPENDENT_SOURCE_SUFFIX "cpp")
endif(ENABLE_CUDF)

set(CURA_LIBRARY_NAME "cura${CURA_LIBRARY_FLAGS}")

set(TARGET_DEPENDENT_SOURCES "${CMAKE_SOURCE_DIR}/src/expression/extract_year.${TARGET_DEPENDENT_SOURCE_SUFFIX}")

add_library("${CURA_LIBRARY_NAME}"
    "${CMAKE_SOURCE_DIR}/src/c/api.cpp"
    "${CMAKE_SOURCE_DIR}/src/c/bridge.cpp"
    "${CMAKE_SOURCE_DIR}/src/data/column_factories.cpp"
    "${CMAKE_SOURCE_DIR}/src/data/fragment.cpp"
    "${CMAKE_SOURCE_DIR}/src/driver/driver.cpp"
    "${CMAKE_SOURCE_DIR}/src/driver/option.cpp"
    "${CMAKE_SOURCE_DIR}/src/execution/context.cpp"
    "${CMAKE_SOURCE_DIR}/src/execution/executor.cpp"
    "${CMAKE_SOURCE_DIR}/src/execution/memory_resource.cpp"
    "${CMAKE_SOURCE_DIR}/src/execution/pipeline.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/aggregation.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/binary_op.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/expressions.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/literal.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/ti_unary_op.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/unary_op.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/aggregate.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/aggregate_helper.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/bucket_aggregate.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/filter.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/hash_join.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/helper.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/kernel.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/limit.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/project.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/sort.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/sources.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/unions.cpp"
    "${CMAKE_SOURCE_DIR}/src/planning/explainer.cpp"
    "${CMAKE_SOURCE_DIR}/src/planning/hash_join_breaker.cpp"
    "${CMAKE_SOURCE_DIR}/src/planning/pipeline_generator.cpp"
    "${CMAKE_SOURCE_DIR}/src/planning/planner.cpp"
    "${CMAKE_SOURCE_DIR}/src/planning/validators.cpp"
    "${CMAKE_SOURCE_DIR}/src/relational/parsers.cpp"
    "${CMAKE_SOURCE_DIR}/src/type/data_type.cpp"
    "${TARGET_DEPENDENT_SOURCES}")

include_directories("${CMAKE_SOURCE_DIR}/include" "${RAPIDJSON_INCLUDE_DIRS}" "${ARROW_INCLUDE_DIR}")
target_link_libraries("${CURA_LIBRARY_NAME}" PUBLIC arrow)

if(ENABLE_CUDF)
  target_include_directories("${CURA_LIBRARY_NAME}" PUBLIC "${CUDA_INCLUDE_DIR}" "${CUDF_INCLUDE_DIR}" "${CUDF_LIBCUDACXX_INCLUDE_DIR}")
  target_link_libraries("${CURA_LIBRARY_NAME}" PUBLIC "${CUDA_LIBRARY}" "${CUDA_RT_LIBRARY}" "${CUDF_LIBRARIES}" "${RMM_LIBRARY}")
endif(ENABLE_CUDF)

if(BUILD_TESTS)
  include(CTest)
  include(ConfigureGoogleTest)

  if(GTEST_FOUND)
    message(STATUS "Google C++ Testing Framework (Google Test) found in ${GTEST_ROOT}")
    include_directories(${GTEST_INCLUDE_DIR})
    add_subdirectory(${CMAKE_SOURCE_DIR}/tests)
  else()
    message(AUTHOR_WARNING "Google C++ Testing Framework (Google Test) not found: automated tests are disabled")
  endif(GTEST_FOUND)
endif(BUILD_TESTS)

message(STATUS "CURA_TEST_LIST set to: ${CURA_TEST_LIST}")
