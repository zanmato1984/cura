if(ENABLE_CUDF)
  message(STATUS "Finding ARROW")

  if(NOT DEFINED ENV{CONDA_PREFIX})
    message(FATAL_ERROR "CONDA_PREFIX not defined.")
  else()
    set(CONDA_PREFIX "$ENV{CONDA_PREFIX}")
    message(STATUS "Using CONDA_PREFIX: ${CONDA_PREFIX}")
  endif(NOT DEFINED ENV{CONDA_PREFIX})

  find_path(ARROW_INCLUDE_DIR NAMES arrow HINTS "${CONDA_PREFIX}/include")

  find_library(ARROW_LIB arrow HINTS "${CONDA_PREFIX}/lib")

  add_library(arrow SHARED IMPORTED ${ARROW_LIB})
else()
  message(STATUS "Building ARROW")

  include(ConfigureArrow)

  if(NOT ARROW_FOUND)
    message(FATAL_ERROR "ARROW not found, please check your settings.")
  endif(NOT ARROW_FOUND)

  add_library(arrow STATIC IMPORTED ${ARROW_LIB})
endif(ENABLE_CUDF)

set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${ARROW_LIB})
message(STATUS "Using ARROW: ${ARROW_INCLUDE_DIR} : ${ARROW_LIB}")
