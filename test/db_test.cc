
#include <algorithm>
#include <boost/interprocess/detail/os_thread_functions.hpp>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>

#include <spdlog/sinks/stdout_color_sinks.h>

#include "dbc++/dbcpp.hh"

#define PROCESS_ID( ) boost::interprocess::ipcdetail::get_current_process_id( )
#define PSQLURI "psql://" POSTGRESQL_USERNAME ":" POSTGRESQL_PASSWORD "@" POSTGRESQL_HOSTNAME "/" POSTGRESQL_DATABASE
#define SQLITEURI "sqlite://memory"

static void test( std::stringstream &page, dbcpp::Connection &connection, const std::string &mainQuery ) {
  auto statement = connection.createStatement( mainQuery );
  auto now       = std::chrono::high_resolution_clock::now( );

  statement << now;
  statement << "Hello";
  statement << 3.1415926;
  statement << now;
  statement << nullptr;
  statement << true;
  statement << false;

  auto result = statement.executeQuery( );

  page << "Rows:   " << result.rows( ) << "\n";
  page << "Fields: " << result.fields( ) << "\n";
  {
    auto names = result.fieldNames( );
    page << "    ";
    std::copy( names.begin( ), names.end( ), std::ostream_iterator< std::string >( page, ", " ) );
    page << "\n";
  }

  if ( result.next( ) ) {
    auto param1    = result.get< dbcpp::DBTime >( 0 );
    auto dbTime    = result.get< dbcpp::DBTime >( 1 );
    auto string    = result.get< std::string >( 2 );
    auto pi        = result.get< double >( 3 );
    auto param1Str = result.get< std::string >( 4 );
    auto nowStr    = result.get< std::string >( 5 );
    auto nil       = result.get< std::string >( 6 );
    auto _true     = result.get< bool >( 7 );
    auto _false    = result.get< bool >( 8 );
    auto date      = result.get< dbcpp::DBTime >( 9 );

    result.get( 0 ) >> param1;
    result.get( 1 ) >> dbTime;
    result.get( 2 ) >> string;
    result.get( 3 ) >> pi;
    result.get( 4 ) >> param1Str;
    result.get( 5 ) >> nowStr;
    result.get( 6 ) >> nil;
    result.get( 7 ) >> _true;
    result.get( 8 ) >> _false;
    result.get( 9 ) >> date;

    page << "[ PARAM1 ]: " << param1.time_since_epoch( ).count( ) << "\n"
         << "[ NOW ]: " << dbTime.time_since_epoch( ).count( ) << "\n"
         << "[ STRING ]: " << string << "\n"
         << "[ PI ]: " << pi << "\n"
         << "[ PARAM1_STR ]: " << param1Str << "\n"
         << "[ DBTIME ]: " << nowStr << "\n"
         << "[ NIL ]: [" << nil << "] "
         << "(" << ( result.isNull( 6 ) ? "true" : "false" ) << ")\n"
         << "[ True ]: " << _true << "\n"
         << "[ False ]: " << _false << "\n"
         << "[ PARAM1->STR ]: " << result.get< std::string >( 0 ) << "\n"
         << "[ DATE ]: " << date.time_since_epoch( ).count( ) << "\n"
         << "--------------------------------------------------------\n";

    for ( auto &&field : result ) {
      page << "[ " << field.name( ) << " ]: (" << field.get< std::string >( ) << ")"
           << "\n";
    }
  }
}

std::string executeDBTest( std::string uri, const std::string &mainQuery ) {
  std::stringstream page;
  dbcpp::Pool       pool( std::move( uri ), 1 );
  auto              connection = pool.getConnection( );
  std::string       ddl;
  std::string       tablename   = "test_" + std::to_string( ( uint32_t ) PROCESS_ID( ) );
  std::string       nullQuery   = ( "select * from " + tablename +
                            " where ( ( i is null )"
                            " and ( ? is null ) ) or ( i = ? )" );
  std::string       insertQuery = "insert into " + tablename + " ( i, v ) values ( ?, ? )";
  std::string       selectQuery = "SELECT * FROM " + tablename + " ORDER BY id DESC";

  if ( uri.compare( 0, 6, "sqlite" ) == 0 ) {
    ddl = "CREATE TABLE " + tablename + " ( id INTEGER PRIMARY KEY AUTOINCREMENT, i INTEGER, v VARCHAR(10) )";
  } else if ( uri.compare( 0, 4, "psql" ) == 0 ) {
    ddl = "CREATE TABLE " + tablename + " ( id SERIAL, i INTEGER, v VARCHAR( 10 ) )";
  }

  try {
    auto statement = connection.createStatement( ddl );
    statement.execute( );
    connection.commit( );
  } catch ( ... ) {
  }

  page << "--------------------------------------------------------\n";

  auto statement = connection << nullQuery;
  statement << ( int32_t * ) nullptr;
  statement << ( int32_t * ) nullptr;

  statement.execute( );

  page << "--------------------------------------------------------\n";

  statement = connection << insertQuery;
  statement << ( int32_t * ) nullptr;
  statement << ( std::string * ) nullptr;

  statement.executeUpdate( );
  connection.commit( );

  page << "--------------------------------------------------------\n";

  statement = connection << selectQuery;
  {
    auto result = statement.executeQuery( );

    if ( result.next( ) ) {
      for ( auto &&field : result ) {
        page << "[ " << field.name( ) << " ]: ";
        if ( field.isNull( ) == false ) {
          page << " (" << field.get< std::string >( ) << ")";
        } else {
          page << " <NULL> ";
        }
        page << "\n";
      }
    }
  }

  page << "--------------------------------------------------------\n";

  test( page, connection, mainQuery );

  page << "--------------------------------------------------------\n";

#if 0
  if ( uri.compare( 0, 4, "psql" ) == 0 ) {
    auto statement = connection.createStatement( "SELECT generate_series( 1, 200 ) as V" );
    auto result    = statement.executeQuery( );

    while ( result.next( ) ) {
      for ( auto &&field : result ) {
        page << "[ " << field.name( ) << " ]: ";
        if ( field.isNull( ) == false ) {
          page << " (" << field.get< std::string >( ) << ")";
        } else {
          page << " <NULL> ";
        }
        page << "\n";
      }
    }
  }

  page << "--------------------------------------------------------\n";
#endif

  try {
    statement = connection.createStatement( "DROP TABLE " + tablename );
    statement.execute( );
    connection.commit( );
  } catch ( ... ) {
  }

  return page.str( );
}

void log_init( void ) {
  auto        sink    = std::make_shared< spdlog::sinks::stdout_color_sink_mt >( );
  std::string names[] = { "dbcpp::psql", "dbcpp::sqlite", "dbcpp::Pool", "dbcpp::Driver" };
  for ( auto &&name : names ) {
    dbcpp::create_logger( name, { sink } )->set_level( spdlog::level::trace );
  }
}

int main( int argc, char *argv[] ) {
  log_init( );

  std::string resultPages[] = { executeDBTest( PSQLURI,
                                               "SELECT ? as param1" // Current clock
                                               ", now() as now"     // DB time
                                               ", ? as string"      // 'Hello'
                                               ", ? as Pi"          // 3.1415926
                                               ", ?::varchar as param1_str"
                                               ", now()::varchar as now_str"
                                               ", ? as NIL"
                                               ", ? as True"
                                               ", ? as False"
                                               ", now()::date as date" ),
                                executeDBTest( SQLITEURI,
                                               "SELECT ? as param1"
                                               ", datetime('now') as now"
                                               ", ? as string"
                                               ", ? as Pi"
                                               ", ? as param1_str"
                                               ", datetime('now') as now_str"
                                               ", ? as NIL"
                                               ", ? as True"
                                               ", ? as False"
                                               ", date() as date" ) };
  auto        pages         = std::vector< std::vector< std::string > >{ };
  auto        maxLen        = std::string::size_type{ };
  auto        maxLine       = std::vector< void * >::size_type{ };

  for ( auto &&page : resultPages ) {
    auto pageLines = std::vector< std::string >{ };
    auto input     = std::istringstream{ page };
    auto line      = std::string{ };

    while ( getline( input, line, '\n' ) ) {
      pageLines.push_back( line );
      maxLen = std::max( maxLen, line.length( ) );
    }

    maxLine = std::max( maxLine, pageLines.size( ) );
    pages.emplace_back( std::move( pageLines ) );
  }

  maxLen += 5;

  for ( decltype( maxLine ) num = 0; num < maxLine; ++num ) {
    for ( auto &&page : pages ) {
      if ( page.size( ) > num ) {
        auto line = page[ num ];
        std::cout << std::setw( maxLen ) << std::left << line;
      }
    }
    std::cout << "\n";
  }

  return 0;
}
