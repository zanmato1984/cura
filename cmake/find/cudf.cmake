message(STATUS "Finding CUDA")

enable_language(CUDA)

# Things needed to compile CUDA code.
if(NOT CMAKE_CUDA_COMPILER)
  message(SEND_ERROR "CMake cannot locate a CUDA compiler")
endif(NOT CMAKE_CUDA_COMPILER)

set(CMAKE_CUDA_STANDARD 14)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

if(CMAKE_COMPILER_IS_GNUCXX)
  # Suppress parentheses warning which causes gmock to fail
  set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler -Wno-parentheses")
  if(CMAKE_CXX11_ABI)
  else()
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler -D_GLIBCXX_USE_CXX11_ABI=0")
  endif(CMAKE_CXX11_ABI)
endif(CMAKE_COMPILER_IS_GNUCXX)

if(CMAKE_CUDA_COMPILER_VERSION)
  # Compute the version. from CMAKE_CUDA_COMPILER_VERSION
  string(REGEX REPLACE "([0-9]+)\\.([0-9]+).*" "\\1" CUDA_VERSION_MAJOR ${CMAKE_CUDA_COMPILER_VERSION})
  string(REGEX REPLACE "([0-9]+)\\.([0-9]+).*" "\\2" CUDA_VERSION_MINOR ${CMAKE_CUDA_COMPILER_VERSION})
  set(CUDA_VERSION "${CUDA_VERSION_MAJOR}.${CUDA_VERSION_MINOR}" CACHE STRING "Version of CUDA as computed from nvcc.")
  mark_as_advanced(CUDA_VERSION)
endif()

message(STATUS "CUDA_VERSION_MAJOR: ${CUDA_VERSION_MAJOR}")
message(STATUS "CUDA_VERSION_MINOR: ${CUDA_VERSION_MINOR}")
message(STATUS "CUDA_VERSION: ${CUDA_VERSION}")

# Always set this convenience variable
set(CUDA_VERSION_STRING "${CUDA_VERSION}")

# Auto-detect available GPU compute architectures
set(GPU_ARCHS "ALL" CACHE STRING
    "List of GPU architectures (semicolon-separated) to be compiled for. Pass 'ALL' if you want to compile for all supported GPU architectures. Empty string means to auto-detect the GPUs on the current system")

if("${GPU_ARCHS}" STREQUAL "")
  include(eval_gpu_archs.cmake)
  evaluate_gpu_archs(GPU_ARCHS)
endif()

if("${GPU_ARCHS}" STREQUAL "ALL")
  # Check for embedded vs workstation architectures
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    # This is being built for Linux4Tegra or SBSA ARM64
    set(GPU_ARCHS "62")
    if((CUDA_VERSION_MAJOR EQUAL 9) OR (CUDA_VERSION_MAJOR GREATER 9))
      set(GPU_ARCHS "${GPU_ARCHS};72")
    endif()
    if((CUDA_VERSION_MAJOR EQUAL 11) OR (CUDA_VERSION_MAJOR GREATER 11))
      set(GPU_ARCHS "${GPU_ARCHS};75;80")
    endif()
  else()
    # This is being built for an x86 or x86_64 architecture
    set(GPU_ARCHS "60")
    if((CUDA_VERSION_MAJOR EQUAL 9) OR (CUDA_VERSION_MAJOR GREATER 9))
      set(GPU_ARCHS "${GPU_ARCHS};70")
    endif()
    if((CUDA_VERSION_MAJOR EQUAL 10) OR (CUDA_VERSION_MAJOR GREATER 10))
      set(GPU_ARCHS "${GPU_ARCHS};75")
    endif()
    if((CUDA_VERSION_MAJOR EQUAL 11) OR (CUDA_VERSION_MAJOR GREATER 11))
      set(GPU_ARCHS "${GPU_ARCHS};80")
    endif()
  endif()
endif()
message("GPU_ARCHS = ${GPU_ARCHS}")

foreach(arch ${GPU_ARCHS})
  set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -gencode=arch=compute_${arch},code=sm_${arch}")
endforeach()

list(GET GPU_ARCHS -1 ptx)
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -gencode=arch=compute_${ptx},code=compute_${ptx}")

set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} --expt-extended-lambda --expt-relaxed-constexpr")

# set warnings as errors
# TODO: remove `no-maybe-unitialized` used to suppress warnings in rmm::exec_policy
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Werror=cross-execution-space-call -Xcompiler -Wall,-Werror,-Wno-error=deprecated-declarations")

option(DISABLE_DEPRECATION_WARNING "Disable warnings generated from deprecated declarations." OFF)
if(DISABLE_DEPRECATION_WARNING)
  set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler -Wno-deprecated-declarations")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
endif(DISABLE_DEPRECATION_WARNING)

# Option to enable line info in CUDA device compilation to allow introspection when profiling / memchecking
option(CMAKE_CUDA_LINEINFO "Enable the -lineinfo option for nvcc (useful for cuda-memcheck / profiler" OFF)
if(CMAKE_CUDA_LINEINFO)
  set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -lineinfo")
endif(CMAKE_CUDA_LINEINFO)

# Debug options
if(CMAKE_BUILD_TYPE MATCHES Debug)
  message(STATUS "Building with debugging flags")
  set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -G -Xcompiler -rdynamic")
endif(CMAKE_BUILD_TYPE MATCHES Debug)

# Per-thread default stream
add_compile_definitions(CUDA_API_PER_THREAD_DEFAULT_STREAM)

# Things needed for CUDF.
set(CUDA_TOOLKIT_ROOT_DIR "/usr/local/cuda")
set(CUDA_LIBRARY_DIR "${CUDA_TOOLKIT_ROOT_DIR}/lib64")
set(CUDA_LIBRARY_STUBS_DIR "${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs")

find_library(CUDA_LIBRARY cuda HINTS "${CUDA_LIBRARY_STUBS_DIR}")
find_library(CUDA_RT_LIBRARY cudart HINTS "${CUDA_LIBRARY_DIR}")

find_path(CUDA_INCLUDE_DIR NAMES cuda.h HINTS "${CUDA_TOOLKIT_ROOT_DIR}/include")

message(STATUS "Using CUDA: ${CUDA_INCLUDE_DIR} : ${CUDA_LIBRARY}")
message(STATUS "Using CUDART: ${CUDA_RT_LIBRARY}")

message(STATUS "Finding CUDF")

if(NOT DEFINED ENV{CONDA_PREFIX})
  message(FATAL_ERROR "CONDA_PREFIX not defined.")
else()
  set(CONDA_PREFIX $ENV{CONDA_PREFIX})
  message(STATUS "Using CONDA_PREFIX: ${CONDA_PREFIX}")
endif(NOT DEFINED ENV{CONDA_PREFIX})

set(CUDF_MODULE_LIST
    "cudf"
    "cudf_ast"
    "cudf_base"
    "cudf_comms"
    "cudf_hash"
    "cudf_interop"
    "cudf_io"
    "cudf_join"
    "cudf_merge"
    "cudf_partitioning"
    "cudf_reductions"
    "cudf_replace"
    "cudf_reshape"
    "cudf_rolling"
    "cudf_transpose")

foreach(CUDF_MODULE IN LISTS CUDF_MODULE_LIST)
  find_library(CUDF_LIB NAMES ${CUDF_MODULE} HINTS "${CONDA_PREFIX}/lib")
  list(APPEND CUDF_LIBRARIES "${CUDF_LIB}")
  unset(CUDF_LIB CACHE)
endforeach()

find_path(CUDF_INCLUDE_DIR NAMES cudf HINTS "${CONDA_PREFIX}/include")
find_path(CUDF_LIBCUDACXX_INCLUDE_DIR NAMES simt/chrono HINTS "${CONDA_PREFIX}/include/libcudf/libcudacxx")

message(STATUS "Using CUDF: ${CUDF_INCLUDE_DIR} : ${CUDF_LIBRARIES}")
message(STATUS "Using LIBCUDACXX: ${CUDF_LIBCUDACXX_INCLUDE_DIR}")
