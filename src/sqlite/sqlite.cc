
#include "dbc++/dbcpp.hh"
#include <algorithm>
#include <boost/variant.hpp>
#include <chrono>
#include <endian.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <sqlite3.h>
#include <string>
#include <time.h>

#include <spdlog/spdlog.h>

#include <spdlog/sinks/null_sink.h>

#define JULIAN_TO_EPOCH 2440587.5

static const std::string btos[] = { "false", "true" };

#define LOG( logger, lvl, ... )                                                                                        \
  do {                                                                                                                 \
    if ( logger->should_log( spdlog::level::lvl ) ) {                                                                  \
      SPDLOG_LOGGER_CALL( logger, spdlog::level::lvl, __VA_ARGS__ );                                                   \
    }                                                                                                                  \
  } while ( 0 )

#define DBCPP_EXCEPTION( ... )                                                                                         \
  do {                                                                                                                 \
    DBException ex( fmt::format( __VA_ARGS__ ) );                                                                      \
    LOG( logger, trace, "{}", ex.what( ) );                                                                            \
    throw ex;                                                                                                          \
  } while ( 0 )

namespace dbcpp {
  namespace sqlite {
    using DBConnection = std::shared_ptr< interface::Connection >;
    using DBStatement  = std::shared_ptr< interface::Statement >;
    using DBResultSet  = std::shared_ptr< interface::ResultSet >;
    using DBField      = std::shared_ptr< interface::Field >;

    /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */
    struct SQLiteStatement;
    struct SQLiteConnection;

    /** Postgresql logger */
    std::shared_ptr< spdlog::logger > logger = create_logger( "dbcpp::sqlite", { } );

    struct SQLiteDriver : public dbcpp::Driver::Base {
      struct SQLiteDatabase;

      static std::mutex                                               mapLock;
      static std::map< std::string, std::weak_ptr< SQLiteDatabase > > databases;

      struct SQLiteDatabase : public std::enable_shared_from_this< SQLiteDatabase > {
        std::mutex                      mutex;
        std::string                     path;
        std::shared_ptr< sqlite3 >      handle;
        std::weak_ptr< SQLiteDatabase > self;

        ~SQLiteDatabase( ) {
          std::lock_guard< std::mutex > guard( mapLock );
          auto                          weak = databases[ path ];

          // If this object is still the referenced object from the index...
          if ( !self.owner_before( weak ) && !weak.owner_before( self ) ) {
            LOG( logger, trace, "Reference count for {} reached zero, removing index entry", path );

            databases.erase( path );
          }
        }
      };

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

      struct SQLiteConnection : public interface::Connection, public std::enable_shared_from_this< SQLiteConnection > {
        /* Members */

        std::shared_ptr< SQLiteDatabase > cxn;
        std::string                       path;

        /* Implemented Interface */
        explicit SQLiteConnection( Uri *uri )
          : path( uri->toString( ).substr( sizeof( "sqlite://" ) - 1 ) ) {
          if ( path == "memory" ) {
            path = ":memory:";
          }
        }

        DBStatement createStatement( std::string query ) override {
          return std::make_shared< SQLiteStatement >( shared_from_this( ), query );
        }

        bool connect( ) override {
          std::lock_guard< std::mutex > guard( mapLock );

          if ( !( cxn = databases[ path ].lock( ) ) ) {
            sqlite3 *tmp = nullptr;
            cxn          = std::make_shared< SQLiteDatabase >( );
            cxn->self    = cxn;

            if ( sqlite3_open( path.c_str( ), &tmp ) != SQLITE_OK ) {
              return false;
            }

            LOG( logger, info, "Connected to database at {}", path );

            sqlite3_extended_result_codes( tmp, 1 );
            cxn->handle.reset( tmp, sqlite3_close );

            databases[ path ] = cxn;
          } else {
            LOG( logger, info, "Sharing existing database connection/session for {}", path );
          }

          return true;
        }

        void setAutoCommit( bool ac ) override {
          LOG( logger, debug, "Ignoring auto commit {}", ac ? "enable" : "disable" );
        }

        void commit( ) override {}
        void rollback( ) override {}
        void begin( ) {}
        bool disconnect( ) override { return false; }
        bool reconnect( ) override { return true; }

        bool test( ) override {
          try {
            Statement statement = createStatement( "SELECT 1" );
            ResultSet result    = statement.executeQuery( );

            if ( result.next( ) ) {
              return result.get< int32_t >( 0 ) == 1;
            }
          } catch ( ... ) {
          }
          return false;
        }
      };

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

#if 0
      struct Visitor : public boost::static_visitor<> {
        std::vector< std::string > &columnNames;
        size_t                      column = 0;

        explicit Visitor( std::vector< std::string > &names )
          : columnNames( names ) {}

        template < class T >
        void operator( )( T &value ) {
          std::cout << columnNames[ column++ ] << ": I " << value << "\n";
        }

        void operator( )( decltype( nullptr ) ) { std::cout << columnNames[ column++ ] << ": NULL\n"; }

        void operator( )( std::string &value ) { std::cout << columnNames[ column++ ] << ": S[" << value << "]\n"; }

        void operator( )( std::vector< uint8_t > &value ) {
          std::stringstream ss;
          ss << std::hex;

          for ( auto &&v : value ) {
            ss << std::setw( 2 ) << std::setfill( '0' ) << static_cast< uint32_t >( v );
          }

          std::cout << columnNames[ column++ ] << ": B[" << ss.str( ) << "]\n";
        }

        void operator( )( double &value ) { std::cout << columnNames[ column++ ] << ": D " << value << "\n"; }
      };
#endif

      struct SQLiteStatement : public interface::Statement, public std::enable_shared_from_this< SQLiteStatement > {
        static const int SQLite_IntType    = 0;
        static const int SQLite_DblType    = 1;
        static const int SQLite_BlobType   = 2;
        static const int SQLite_NullType   = 3;
        static const int SQLite_StringType = 4;
        using COLUMN  = boost::variant< int64_t, double, std::vector< uint8_t >, decltype( nullptr ), std::string >;
        using COLUMNS = std::vector< COLUMN >;
        using ROWS    = std::vector< COLUMNS >;

        std::shared_ptr< SQLiteConnection > connection;
        std::vector< std::string >          columnNames;
        std::vector< int >                  columnTypes;
        std::shared_ptr< sqlite3_stmt >     handle;
        std::string                         query;
        ROWS::size_type                     affected;
        COLUMNS::size_type                  fields;
        ROWS                                results;

        SQLiteStatement( std::shared_ptr< SQLiteConnection > _connection, const std::string &_query )
          : connection( std::move( _connection ) )
          , query( _query )
          , affected( 0 ) {
          const char *  endPtr = nullptr;
          sqlite3_stmt *tmp    = nullptr;

          if ( query.length( ) == 0 ) {
            DBCPP_EXCEPTION( "Query is empty" );
          }

          std::lock_guard< std::mutex > guard( connection->cxn->mutex );

          if ( sqlite3_prepare_v2( connection->cxn->handle.get( ),
                                   query.c_str( ), //
                                   query.length( ),
                                   &tmp,
                                   &endPtr ) != SQLITE_OK ) {
            auto code  = sqlite3_extended_errcode( connection->cxn->handle.get( ) );
            auto msg   = sqlite3_errstr( code );
            auto dbmsg = sqlite3_errmsg( connection->cxn->handle.get( ) );

            DBCPP_EXCEPTION( "Error preparing query '{}' {}: {}", query, msg, dbmsg );
          }

          handle.reset( tmp, sqlite3_finalize );
          fields = sqlite3_column_count( handle.get( ) );

          LOG( logger, trace, "Query {} resulted in {} fields", query, fields );
        }

        void reset( ) {
          LOG( logger, debug, "Resetting bound parameters for query" );

          sqlite3_reset( handle.get( ) );
          sqlite3_clear_bindings( handle.get( ) );
        }

        bool setParam( size_t parameter, dbcpp::interface::safebool value ) override {
          return setParam( parameter, btos[ !!value.val ] );
        }
        bool setParam( size_t parameter, uint8_t value ) override { return setParam( parameter, ( int16_t ) value ); }
        bool setParam( size_t parameter, uint16_t value ) override { return setParam( parameter, ( int32_t ) value ); }
        bool setParam( size_t parameter, uint32_t value ) override { return setParam( parameter, ( int64_t ) value ); }
        bool setParam( size_t parameter, uint64_t value ) override { return setParam( parameter, ( int64_t ) value ); }
        bool setParam( size_t parameter, int8_t value ) override { return setParam( parameter, ( int32_t ) value ); }
        bool setParam( size_t parameter, int16_t value ) override { return setParam( parameter, ( int32_t ) value ); }
        bool setParam( size_t parameter, int32_t value ) override { return setParam( parameter, ( int64_t ) value ); }
        bool setParam( size_t parameter, int64_t value ) override {
          LOG( logger, trace, "Set parameter #{} to int", parameter + 1 );

          return sqlite3_bind_int64( handle.get( ), ( int ) parameter + 1, value ) == SQLITE_OK;
        }

        bool setParam( size_t parameter, float value ) override { return setParam( parameter, ( double ) value ); }
        bool setParam( size_t parameter, long double value ) override {
          return setParam( parameter, ( double ) value );
        }
        bool setParam( size_t parameter, double value ) override {
          LOG( logger, trace, "Set parameter #{} to double", parameter + 1 );
          return sqlite3_bind_double( handle.get( ), ( int ) parameter + 1, value ) == SQLITE_OK;
        }

        bool setParam( size_t parameter, std::string value ) override {
          LOG( logger, trace, "Set parameter #{} to string", parameter + 1 );

          return sqlite3_bind_text64( handle.get( ), //
                                      parameter + 1,
                                      value.c_str( ),
                                      value.length( ),
                                      SQLITE_TRANSIENT,
                                      SQLITE_UTF8 ) == SQLITE_OK;
        }

        bool setParam( size_t parameter, std::vector< uint8_t > value ) override {
          LOG( logger, trace, "Set parameter #{} to bytea", parameter + 1 );

          return sqlite3_bind_blob64( handle.get( ), parameter + 1, &value[ 0 ], value.size( ), SQLITE_TRANSIENT ) ==
                 SQLITE_OK;
        }

        bool setParam( size_t parameter, DBTime value ) override {
          time_t _time = DBClock::to_time_t( value );
          if ( _time ) {
            struct tm _tm          = { 0 };
            char      block[ 128 ] = "";
            gmtime_r( &_time, &_tm );
            ::strftime( block, sizeof( block ), "%F %T", &_tm );
            return setParam( parameter, block );
          }
          return setParamNull( parameter, FieldType::DATE );
        }

        bool setParamNull( size_t parameter, FieldType type ) override {
          LOG( logger, trace, "Set parameter #{} to null", parameter + 1 );
          return sqlite3_bind_null( handle.get( ), parameter + 1 );
        }

        void execute( ) override {
          std::lock_guard< std::mutex > guard( connection->cxn->mutex );

          columnTypes.resize( fields, SQLITE_NULL );

          LOG( logger, trace, "Executing query: {}", query );

          do {
            switch ( sqlite3_step( handle.get( ) ) ) {
              case SQLITE_ROW: {
                COLUMNS columns;

                for ( size_t field = 0; field < fields; ++field ) {
                  /* Get the data as strings ... */
                  auto        type = sqlite3_column_type( handle.get( ), field );
                  std::string name = sqlite3_column_name( handle.get( ), field );

                  switch ( type ) {
                    case SQLITE_INTEGER: {
                      int64_t value = sqlite3_column_int64( handle.get( ), field );
                      columns.emplace_back( COLUMN{ value } );
                      break;
                    }
                    case SQLITE_FLOAT: {
                      double value = sqlite3_column_double( handle.get( ), field );
                      columns.emplace_back( COLUMN{ value } );
                      break;
                    }
                    case SQLITE_BLOB: {
                      std::vector< uint8_t > bytes;
                      bytes.resize( sqlite3_column_bytes( handle.get( ), field ) );
                      memcpy( &bytes[ 0 ], sqlite3_column_blob( handle.get( ), field ), bytes.size( ) );
                      columns.emplace_back( COLUMN{ bytes } );
                      break;
                    }
                    case SQLITE_NULL: {
                      columns.emplace_back( COLUMN{ nullptr } );
                      break;
                    }
                    case SQLITE_TEXT: {
                      const char *text  = ( const char * ) sqlite3_column_text( handle.get( ), field );
                      std::string value = text;
                      columns.emplace_back( COLUMN{ std::move( value ) } );
                      break;
                    }
                  }

                  if ( columnTypes[ field ] == SQLITE_NULL ) {
                    columnTypes[ field ] = type;
                  }

                  std::transform( name.begin( ), name.end( ), name.begin( ), ::toupper );
                  columnNames.emplace_back( name );
                }

#if 0
                Visitor visitor( columnNames );
                std::for_each( columns.begin( ), columns.end( ), boost::apply_visitor( visitor ) );
#endif
                LOG( logger, trace, "Result fields: ({}) {}", columnNames.size( ), fmt::join( columnNames, ", " ) );

                results.emplace_back( columns );

                break;
              }

              case SQLITE_DONE: { /* No more data */
                /* Get the number of rows... */
                affected = sqlite3_changes( connection->cxn->handle.get( ) );
                return;
              }

              default: {
                auto code  = sqlite3_extended_errcode( connection->cxn->handle.get( ) );
                auto msg   = sqlite3_errstr( code );
                auto dbmsg = sqlite3_errmsg( connection->cxn->handle.get( ) );
                throw DBException( std::string( msg ) + ": " + dbmsg );
              }
            }

          } while ( true );
        }

        int executeUpdate( ) override {
          execute( );

          return affected;
        }

        DBResultSet getResults( ) override { return std::make_shared< SQLiteResultSet >( shared_from_this( ) ); }
      };

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */
      struct SQLiteResultSet;

      struct SQLiteField : public interface::Field {
        SQLiteResultSet *results;
        size_t           field = 0;

        SQLiteField( SQLiteResultSet *_results, size_t _field )
          : results( _results )
          , field( _field ) {}

        std::string name( ) const override { return results->fieldName( field ); }

        SQLiteStatement::COLUMNS &getRow( ) const { return results->stmt->results[ results->current ]; }

        SQLiteStatement::COLUMN &getCol( ) const {
          auto &columns = getRow( );
          return columns[ field ];
        }

        bool isNull( ) const override { return getCol( ).which( ) == SQLiteStatement::SQLite_NullType; }

        uint8_t  getU8( ) const { return getI32( ); }
        int8_t   getI8( ) const { return getI32( ); }
        uint16_t getU16( ) const { return getI32( ); }
        int16_t  getI16( ) const { return getI32( ); }
        uint32_t getU32( ) const { return getI32( ); }
        int32_t  getI32( ) const { return getI64( ); }
        uint64_t getU64( ) const { return getI64( ); }
        int64_t  getI64( ) const {
          auto    col   = getCol( );
          int64_t value = 0;

          switch ( col.which( ) ) {
            case SQLiteStatement::SQLite_IntType: {
              value = boost::get< int64_t >( col );
              break;
            }
            case SQLiteStatement::SQLite_DblType: {
              value = ( int64_t ) boost::get< double >( col );
              break;
            }
            case SQLiteStatement::SQLite_StringType: {
              value = stoi( boost::get< std::string >( col ) );
              break;
            }
            default: {
              break;
            }
          }

          return value;
        }

        double getDouble( ) const {
          auto   col   = getCol( );
          double value = nan( "" );

          switch ( col.which( ) ) {
            case SQLiteStatement::SQLite_IntType: {
              value = ( double ) boost::get< int64_t >( col );
              break;
            }

            case SQLiteStatement::SQLite_DblType: {
              value = boost::get< double >( col );
              break;
            }

            case SQLiteStatement::SQLite_StringType: {
              value = stod( boost::get< std::string >( col ) );
              break;
            }

            default: {
              break;
            }
          }

          return value;
        }

        long double getLongDouble( ) const { return getDouble( ); }
        float       getFloat( ) const { return getDouble( ); }

        std::string getString( ) const {
          auto col = getCol( );

          switch ( col.which( ) ) {
            case SQLiteStatement::SQLite_IntType: {
              return std::to_string( boost::get< int64_t >( col ) );
            }

            case SQLiteStatement::SQLite_DblType: {
              return std::to_string( boost::get< double >( col ) );
            }

            case SQLiteStatement::SQLite_StringType: {
              return boost::get< std::string >( col );
            }

            default: {
              return "";
            }
          }
        }

        std::vector< uint8_t > getBlob( ) const {
          auto &col = getCol( );
          return boost::get< std::vector< uint8_t > >( col );
        }

        DBTime getTime( ) const {
          DBTime time;

          switch ( type( ) ) {
            case FieldType::DATE:
            case FieldType::VARCHAR: {
              struct tm tm = { 0 };
              strptime( getString( ).c_str( ), "%F %T", &tm );
              time = DBTime( std::chrono::seconds( mktime( &tm ) ) );
              break;
            }
            case FieldType::BIGINT: {
              time = DBTime( std::chrono::seconds( getU64( ) ) );
              break;
            }
            case FieldType::DOUBLE: {
              auto seconds = static_cast< uint64_t >( ( getDouble( ) - JULIAN_TO_EPOCH ) * 86400 );
              time         = DBTime( std::chrono::seconds( seconds ) );
              break;
            }
            default: {
              throw DBException( "Invalid field type - time not supported" );
            }
          }

          return time;
        }
      };

      struct SQLiteIntegerField : SQLiteField {
        SQLiteIntegerField( SQLiteResultSet *results, size_t field )
          : SQLiteField( results, field ) {}

        FieldType type( ) const override { return FieldType::BIGINT; }
        void      get( int8_t &val ) const override { val = getI8( ); }
        void      get( uint8_t &val ) const override { val = getU8( ); }
        void      get( int16_t &val ) const override { val = getI16( ); }
        void      get( uint16_t &val ) const override { val = getU16( ); }
        void      get( int32_t &val ) const override { val = getI32( ); }
        void      get( uint32_t &val ) const override { val = getU32( ); }
        void      get( int64_t &val ) const override { val = getI64( ); }
        void      get( uint64_t &val ) const override { val = getU64( ); }
        void      get( DBTime &val ) const override { val = getTime( ); }
        void      get( std::string &val ) const override {
          std::stringstream ss;
          if ( !isNull( ) ) {
            ss << getI64( );
          }
          val = ss.str( );
        }
      };

      struct SQLiteVarcharField : SQLiteField {
        SQLiteVarcharField( SQLiteResultSet *results, size_t field )
          : SQLiteField( results, field ) {}

        FieldType type( ) const override { return FieldType::VARCHAR; }
        void      get( DBTime &val ) const override { val = getTime( ); }
        void      get( std::string &val ) const override { val = getString( ); }
      };

      struct SQLiteTimestampField : SQLiteVarcharField {
        SQLiteTimestampField( SQLiteResultSet *results, size_t field )
          : SQLiteVarcharField( results, field ) {}
        FieldType type( ) const override { return FieldType::DATE; }
      };

      struct SQLiteByteAField : SQLiteField {
        SQLiteByteAField( SQLiteResultSet *results, size_t field )
          : SQLiteField( results, field ) {}

        FieldType type( ) const override { return FieldType::BLOB; }
        void      get( std::vector< uint8_t > &val ) const override { val = getBlob( ); }
      };

      struct SQLiteFloatField : SQLiteField {
        SQLiteFloatField( SQLiteResultSet *results, size_t field )
          : SQLiteField( results, field ) {}

        FieldType type( ) const override { return FieldType::DOUBLE; }
        void      get( float &val ) const override { val = getFloat( ); }
        void      get( double &val ) const override { val = getDouble( ); }
        void      get( long double &val ) const override { val = getLongDouble( ); }
        void      get( std::string &val ) const override {
          std::stringstream ss;
          if ( !isNull( ) ) {
            ss << getFloat( );
          }
          val = ss.str( );
        }
      };

      struct SQLiteNullField : SQLiteField {
        SQLiteNullField( SQLiteResultSet *results, size_t field )
          : SQLiteField( results, field ) {}
        FieldType type( ) const override { return FieldType::UNKNOWN; }
        void      get( float &val ) const override { val = nanf( "" ); }
        void      get( double &val ) const override { val = nan( "" ); }
        void      get( long double &val ) const override { val = nanl( "" ); }
        void      get( std::string &val ) const override { val = ""; }
      };

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

      struct SQLiteResultSet : public interface::ResultSet, public std::enable_shared_from_this< SQLiteResultSet > {
        std::vector< std::unique_ptr< SQLiteField > > columns;
        std::shared_ptr< SQLiteStatement >            stmt;
        size_t                                        current;

        explicit SQLiteResultSet( std::shared_ptr< SQLiteStatement > _stmt )
          : stmt( std::move( _stmt ) )
          , current( -1 ) {
          for ( size_t field = 0; field < stmt->fields; ++field ) {
            switch ( stmt->columnTypes[ field ] ) {
              case SQLITE_INTEGER: {
                columns.emplace_back( std::unique_ptr< SQLiteField >( new SQLiteIntegerField( this, field ) ) );
                break;
              }
              case SQLITE_FLOAT: {
                columns.emplace_back( std::unique_ptr< SQLiteField >( new SQLiteFloatField( this, field ) ) );
                break;
              }
              case SQLITE_BLOB: {
                columns.emplace_back( std::unique_ptr< SQLiteField >( new SQLiteByteAField( this, field ) ) );
                break;
              }
              case SQLITE_NULL: {
                columns.emplace_back( std::unique_ptr< SQLiteField >( new SQLiteNullField( this, field ) ) );
                break;
              }
              case SQLITE_TEXT: {
                columns.emplace_back( std::unique_ptr< SQLiteField >( new SQLiteVarcharField( this, field ) ) );
                break;
              }
            }
          }
        }

        std::string                fieldName( size_t field ) const { return stmt->columnNames[ field ]; }
        std::vector< std::string > fieldNames( ) const override { return stmt->columnNames; }
        size_t                     fields( ) const override { return stmt->fields; }
        size_t                     rows( ) const override { return stmt->results.size( ); }
        size_t                     row( ) const override { return current; }
        bool                       next( ) override { return ++current < rows( ); }

        DBField get( size_t field ) const override {
          if ( field >= columns.size( ) ) {
            throw DBException( "Field index is out of range" );
          }
          return std::shared_ptr< interface::Field >( shared_from_this( ), columns[ field ].get( ) );
        }

        DBField get( std::string name ) const override {
          std::transform( name.begin( ), name.end( ), name.begin( ), ::toupper );
          auto iterator = std::find( stmt->columnNames.begin( ), stmt->columnNames.end( ), name );

          if ( iterator == stmt->columnNames.end( ) ) {
            throw DBException( std::string( "Unknown field named: " ) + name );
          }

          return get( iterator - stmt->columnNames.begin( ) );
        }
      };

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */
      DBConnection createConnection( Uri *const uri ) { return std::make_shared< SQLiteConnection >( uri ); }

      ~SQLiteDriver( ) = default;
    }; // namespace sqlite

    SQLiteDriver                                                           driver;
    std::mutex                                                             SQLiteDriver::mapLock;
    std::map< std::string, std::weak_ptr< SQLiteDriver::SQLiteDatabase > > SQLiteDriver::databases;
  } // namespace sqlite
} // namespace dbcpp

extern "C" {
/**
 * @brief Get the driver implementation
 */
dbcpp::Driver::Base *getDriver( ) {
  return dynamic_cast< dbcpp::Driver::Base * >( &dbcpp::sqlite::driver );
}
}
