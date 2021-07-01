cmake_minimum_required(VERSION 3.16)

set(ARROW_ROOT ${CMAKE_BINARY_DIR}/arrow)

set(ARROW_CMAKE_ARGS " -DCMAKE_BUILD_TYPE=Release"
    " -DARROW_COMPUTE=ON"
    " -DARROW_BUILD_SHARED=OFF"
    " -DARROW_BUILD_STATIC=ON"
    " -DARROW_JEMALLOC=OFF"
    " -DARROW_DEPENDENCY_SOURCE=AUTO"
    " -DARROW_WITH_UTF8PROC=OFF"
    " -DARROW_CXXFLAGS=-Wno-range-loop-analysis")

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/Templates/Arrow.CMakeLists.txt.cmake"
    "${ARROW_ROOT}/CMakeLists.txt")

file(MAKE_DIRECTORY "${ARROW_ROOT}/build")
file(MAKE_DIRECTORY "${ARROW_ROOT}/install")

execute_process(
    COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    RESULT_VARIABLE ARROW_CONFIG
    WORKING_DIRECTORY ${ARROW_ROOT})

if(ARROW_CONFIG)
  message(FATAL_ERROR "Configuring Arrow failed: " ${ARROW_CONFIG})
endif(ARROW_CONFIG)

set(PARALLEL_BUILD -j)
if($ENV{PARALLEL_LEVEL})
  set(NUM_JOBS $ENV{PARALLEL_LEVEL})
  set(PARALLEL_BUILD "${PARALLEL_BUILD}${NUM_JOBS}")
endif($ENV{PARALLEL_LEVEL})

if(${NUM_JOBS})
  if(${NUM_JOBS} EQUAL 1)
    message(STATUS "ARROW BUILD: Enabling Sequential CMake build")
  elseif(${NUM_JOBS} GREATER 1)
    message(STATUS "ARROW BUILD: Enabling Parallel CMake build with ${NUM_JOBS} jobs")
  endif(${NUM_JOBS} EQUAL 1)
else()
  message(STATUS "ARROW BUILD: Enabling Parallel CMake build with all threads")
endif(${NUM_JOBS})

execute_process(
    COMMAND ${CMAKE_COMMAND} --build .. -- ${PARALLEL_BUILD}
    RESULT_VARIABLE ARROW_BUILD
    WORKING_DIRECTORY ${ARROW_ROOT}/build)

if(ARROW_BUILD)
  message(FATAL_ERROR "Building Arrow failed: " ${ARROW_BUILD})
endif(ARROW_BUILD)

message(STATUS "Arrow installed here: " ${ARROW_ROOT}/install)
set(ARROW_LIBRARY_DIR "${ARROW_ROOT}/install/lib")
set(ARROW_INCLUDE_DIR "${ARROW_ROOT}/install/include")

find_library(ARROW_LIB arrow
    NO_DEFAULT_PATH
    HINTS "${ARROW_LIBRARY_DIR}")

if(ARROW_LIB)
  message(STATUS "Arrow library: " ${ARROW_LIB})
  set(ARROW_FOUND TRUE)
endif(ARROW_LIB)

set(FLATBUFFERS_ROOT "${ARROW_ROOT}/build/flatbuffers_ep-prefix/src/flatbuffers_ep-install")

message(STATUS "FlatBuffers installed here: " ${FLATBUFFERS_ROOT})
set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_ROOT}/include")
set(FLATBUFFERS_LIBRARY_DIR "${FLATBUFFERS_ROOT}/lib")

add_definitions(-DARROW_METADATA_V4)
