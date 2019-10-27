
ADD_EXECUTABLE( db_test db_test.cc )
TARGET_LINK_LIBRARIES( db_test dbc++ )
ADD_TEST( NAME DBTest COMMAND db_test )
