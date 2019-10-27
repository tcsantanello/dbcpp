CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

PROJECT(dbcpp)

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)

SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(CMAKE_CXX_EXTENSIONS OFF)
SET(CMAKE_CXX_FLAGS_DEBUG "-ggdb3 -Wall -O0")

IF(CONAN_EXPORTED)
  INCLUDE(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
  CONAN_BASIC_SETUP(TARGETS)
ELSE()
  IF(NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
    FILE(DOWNLOAD
         "https://github.com/conan-io/cmake-conan/raw/v0.15/conan.cmake"
         "${CMAKE_BINARY_DIR}/conan.cmake"
    )
  ENDIF()

  INCLUDE(${CMAKE_BINARY_DIR}/conan.cmake)
  CONAN_CMAKE_RUN(
    CONANFILE conanfile.py BASIC_SETUP CMAKE_TARGETS BUILD missing
  )
ENDIF()

# ##############################################################################
# OpenSSL
#
SET(OPENSSL_ROOT_DIR ${CONAN_BUILD_DIRS_OPENSSL})
FIND_PACKAGE(OpenSSL)

# ##############################################################################
# Boost
#
SET(ENV{BOOST_ROOT} ${CONAN_BUILD_DIRS_BOOST})
SET(Boost_NO_BOOST_CMAKE ON)
SET(Boost_USE_STATIC_LIBS ON)
SET(Boost_USE_MULTITHREADED ON)
FIND_PACKAGE(
  Boost
  COMPONENTS
  REQUIRED filesystem regex system thread
)
# ##############################################################################

# ##############################################################################
# SpeedLog
#
FIND_PATH(
  SPDLOG_INCLUDE_PATH
  NAMES spdlog/spdlog.h
  PATHS ${CONAN_SPDLOG_ROOT} /usr/include /usr/local/include
)
IF(NOT SPDLOG_INCLUDE_PATH)
  MESSAGE(FATAL_ERROR "Missing Speedlog")
ELSE()
  MESSAGE(STATUS "Speedlog found: ${SPDLOG_INCLUDE_PATH}")
ENDIF()

# ##############################################################################
# URI Header
#
FIND_PATH(
  URI_INCLUDE_PATH
  NAMES uri/uri.hh
  PATHS ${CONAN_CPPURI_ROOT} /usr/local/include /usr/include
)
IF(NOT URI_INCLUDE_PATH)
  MESSAGE(FATAL_ERROR "Missing URI Header ${CONAN_CPPURI_ROOT}")
ELSE()
  MESSAGE(STATUS "URI Header found: ${URI_INCLUDE_PATH}")
ENDIF()
# ##############################################################################

INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_BINARY_DIR}/include
  ${Boost_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIR}
  ${SPDLOG_INCLUDE_PATH}
  ${URI_INCLUDE_PATH}
  ${CONAN_INCLUDE_DIRS}
)

LINK_DIRECTORIES(${Boost_LIBRARY_DIRS} ${CONAN_LIB_DIRS})
ADD_DEFINITIONS(${CONAN_DEFINES})

# ##############################################################################
# ##############################################################################
# ##############################################################################

MESSAGE(STATUS "POSTGRESQL_ENABLE: ${POSTGRESQL_ENABLE}")

IF(POSTGRESQL_ENABLE)
  MESSAGE(STATUS "Enabling PostgreSQL")

  IF(NOT CONAN_LIBPQ_ROOT)
    FIND_PACKAGE(PostgreSQL)
    IF(PostgreSQL_FOUND)
      INCLUDE_DIRECTORIES(${PostgreSQL_INCLUDE_DIRS})
      SET(PSQL_ENABLED TRUE)
    ENDIF()
  ELSE()
    MESSAGE(STATUS "LIBPQ Found: ${CONAN_LIBPQ_ROOT}")
    SET(PSQL_ENABLED TRUE)
  ENDIF()
ENDIF()

IF(PSQL_ENABLED)
  MESSAGE(STATUS "Setting PostgreSQL properties")

  IF(NOT POSTGRESQL_USERNAME)
    SET(POSTGRESQL_USERNAME $ENV{POSTGRESQL_USERNAME})
    IF(NOT POSTGRESQL_USERNAME)
      SET(POSTGRESQL_USERNAME "testdb")
    ENDIF()
  ENDIF()

  IF(NOT POSTGRESQL_DATABASE)
    SET(POSTGRESQL_DATABASE $ENV{POSTGRESQL_DATABASE})
    IF(NOT POSTGRESQL_DATABASE)
      SET(POSTGRESQL_DATABASE ${POSTGRESQL_USERNAME})
    ENDIF()
  ENDIF()

  IF(NOT POSTGRESQL_PASSWORD)
    SET(POSTGRESQL_PASSWORD $ENV{POSTGRESQL_PASSWORD})
    IF(NOT POSTGRESQL_PASSWORD)
      SET(POSTGRESQL_PASSWORD "123456")
    ENDIF()
  ENDIF()

  IF(NOT POSTGRESQL_HOSTNAME)
    SET(POSTGRESQL_HOSTNAME $ENV{POSTGRESQL_HOSTNAME})
    IF(NOT POSTGRESQL_HOSTNAME)
      SET(POSTGRESQL_HOSTNAME "postgres")
    ENDIF()
  ENDIF()

  ADD_DEFINITIONS(
    -DPOSTGRESQL_USERNAME="${POSTGRESQL_USERNAME}"
    -DPOSTGRESQL_DATABASE="${POSTGRESQL_DATABASE}"
    -DPOSTGRESQL_PASSWORD="${POSTGRESQL_PASSWORD}"
    -DPOSTGRESQL_HOSTNAME="${POSTGRESQL_HOSTNAME}"
  )
ENDIF()

IF(FREETDS_ENABLE)
  IF(NOT CONAN_FREETDS_ROOT)
    FIND_PACKAGE(FreeTDS)
    IF(FreeTDS_FOUND)
      INCLUDE_DIRECTORIES(${FreeTDS_INCLUDE_DIRS})
      SET(FREETDS_ENABLED TRUE)
    ENDIF()
  ELSE()
    MESSAGE(STATUS "FreeTDS Found: ${CONAN_FREETDS_ROOT}")
    SET(FREETDS_ENABLED TRUE)
  ENDIF()
ENDIF()

IF(SQLITE3_ENABLE)
  IF(NOT CONAN_SQLITE3_ROOT)
    FIND_PACKAGE(SQLite3)
    IF(SQLite3_FOUND)
      INCLUDE_DIRECTORIES(${SQLite3_INCLUDE_DIRS})
      SET(SQLITE_ENABLED TRUE)
    ENDIF()
  ELSE()
    MESSAGE(STATUS "SQLite3 Found: ${CONAN_SQLITE3_ROOT}")
    SET(SQLITE_ENABLED TRUE)
  ENDIF()
ENDIF()

# ##############################################################################
# ##############################################################################
# ##############################################################################

ENABLE_TESTING()

ADD_DEFINITIONS(-DPSQL_ENABLED=${PSQL_ENABLED})
ADD_DEFINITIONS(-DSQLITE_ENABLED=${SQLITE_ENABLED})
ADD_DEFINITIONS(-DFREETDS_ENABLED=${FREETDS_ENABLED})

ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(test)
