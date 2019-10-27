
IF ( HAVE_FreeTDS )
  RETURN( )
ENDIF( )

SET( __FreeTDS_ERROR_MSG "FreeTDS library was not found.  Please specify a location" )

FIND_PATH( FreeTDS_INCLUDES
  ctpublic.h
  /usr/local/include
  /usr/include
  /opt/freetds/include
  )

FIND_LIBRARY( FreeTDS_LIBS
  NAMES ct
  PATHS
  /usr/lib/
  /usr/local/lib
  /opt/freetds/lib
  )

IF ( FreeTDS_INCLUDES AND FreeTDS_LIBS )
  SET( HAVE_FreeTDS True )
  MESSAGE( STATUS "FreeTDS Found: ${FreeTDS_LIBS}" )

  STRING( REGEX REPLACE "\(.*\)/libct.*" "\\1"
    FreeTDS_LIBRARY_PATHS "${FreeTDS_LIBS_PATHS}" )
  
  LINK_DIRECTORIES( ${FreeTDS_LIBRARY_PATHS} )
  INCLUDE_DIRECTORIES( ${FreeTDS_INCLUDES} )
ELSEIF( FreeTDS_FIND_REQUIRED )
  MESSAGE( FATAL_ERROR "${__FreeTDS_ERROR_MSG}" )
ELSE( )
  MESSAGE( STATUS "${__FreeTDS_ERROR_MSG}" ) 
ENDIF( )

MARK_AS_ADVANCED( FreeTDS_INCLUDES FreeTDS_LIBS FreeTDS_LIBS_PATH )
