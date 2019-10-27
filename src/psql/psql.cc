
#include "dbc++/dbcpp.hh"
#include <algorithm>
#include <boost/variant.hpp>
#include <catalog/pg_type_d.h>
#include <cctype>
#include <chrono>
#include <endian.h>
#include <functional>
#include <iomanip>
#include <libpq-fe.h>
#include <map>
#include <memory>
#include <spdlog/spdlog.h>
#include <sstream>
#include <string>

#include <spdlog/sinks/null_sink.h>

#define PQclear( x )                                                                                                   \
  do {                                                                                                                 \
    PQclear( x );                                                                                                      \
    x = nullptr;                                                                                                       \
  } while ( 0 )

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
  namespace psql {
    using DBConnection = std::shared_ptr< interface::Connection >;
    using DBStatement  = std::shared_ptr< interface::Statement >;
    using DBResultSet  = std::shared_ptr< interface::ResultSet >;
    using DBField      = std::shared_ptr< interface::Field >;

    enum QueryType {
      UNKNOWN = 0,
      SELECT,
      DELETE,
      INSERT,
      UPDATE,
    };

    /** Postgresql logger */
    std::shared_ptr< spdlog::logger > logger = create_logger( "dbcpp::psql", { } );

    auto FOR_UPDATE_PAT = std::string{ "for update" };
    auto PSQLEpoch      = /* POSTGRES_EPOCH_DATE - January 1, 2000, 00:00:00 */
      std::chrono::duration_cast< std::chrono::microseconds >( std::chrono::seconds( 946684800 ) );

    static inline QueryType queryType( const std::string &query ) {
      auto cmd = query.substr( 0, 6 );

      switch ( cmd[ 0 ] ) {
        case 's':
        case 'S': {
          if ( !strcasecmp( cmd.c_str( ) + 1, "elect" ) ) {
            return SELECT;
          }
          break;
        }
        case 'd':
        case 'D': {
          if ( !strcasecmp( cmd.c_str( ) + 1, "elete" ) ) {
            return DELETE;
          }
          break;
        }
        case 'i':
        case 'I': {
          if ( !strcasecmp( cmd.c_str( ) + 1, "nsert" ) ) {
            return INSERT;
          }
          break;
        }
        case 'u':
        case 'U': {
          if ( !strcasecmp( cmd.c_str( ) + 1, "pdate" ) ) {
            return UPDATE;
          }
          break;
        }
        default:
          break;
      }
      return UNKNOWN;
    }

    static inline int32_t normalizeParameters( std::string query, std::string &output ) {
      uint32_t bindNum    = 1;
      bool     paramValid = true;

      for ( std::string::size_type pos = 0; pos < query.length( ); ++pos ) {
        char value = query.substr( pos, 1 ).c_str( )[ 0 ];

        if ( ( paramValid ) && ( value == '?' ) ) {
          std::ostringstream ss;
          ss << '$' << bindNum++;
          query.replace( pos, 1, ss.str( ) );
          ++pos;
        } else if ( '\'' == value ) {
          paramValid = !paramValid;
        }
      }

      output = query;

      return bindNum - 1;
    }

    template < typename It >
    inline std::pair< It, It > find_an( const It hbeg, const It hend, const It nbeg, const It nend ) {
      std::pair< It, It > rc{ hend, hbeg };
      It                  nit = nbeg;

      for ( ; rc.second != hend; ++rc.second ) {
        if ( std::isspace( *nit ) ) {
          if ( std::isspace( *rc.second ) ) {
            continue;
          }

          ++nit;
        }

        if ( std::tolower( *nit ) != std::tolower( *rc.second ) ) {
          nit = nbeg;
          continue;
        }

        if ( nit++ == nbeg ) {
          rc.first = rc.second;
        }

        if ( nit == nend ) {
          break;
        }
      }

      return rc;
    }

    template < typename _type >
    inline std::pair< typename _type::iterator, typename _type::iterator > find_an( _type &haystack, _type &needle ) {
      return find_an( haystack.begin( ), haystack.end( ), needle.begin( ), needle.end( ) );
    }

    static inline void result_trace( PGresult *result, const std::string &msg ) {
      if ( logger->should_log( spdlog::level::trace ) ) {
        auto status = PQresultStatus( result );
        logger->trace( "{}:", msg );
        logger->trace( "  Status:    '{}'", PQresStatus( status ) );
        logger->trace( "  CmdStatus: '{}'", PQcmdStatus( result ) );
        if ( ( status != PGRES_COMMAND_OK ) && ( status != PGRES_TUPLES_OK ) ) {
          logger->trace( "  ErrMsg:    '{}'", PQresultErrorMessage( result ) );
          logger->trace( "  Diag:" );
          logger->trace( "   - Severity:     '{}'", PQresultErrorField( result, PG_DIAG_SEVERITY ) ?: "" );
          logger->trace( "   - Severity NL:  '{}'", PQresultErrorField( result, PG_DIAG_SEVERITY_NONLOCALIZED ) ?: "" );
          logger->trace( "   - SQLState:     '{}'", PQresultErrorField( result, PG_DIAG_SQLSTATE ) ?: "" );
          logger->trace( "   - MsgPrimary:   '{}'", PQresultErrorField( result, PG_DIAG_MESSAGE_PRIMARY ) ?: "" );
          logger->trace( "   - MsgDetail:    '{}'", PQresultErrorField( result, PG_DIAG_MESSAGE_DETAIL ) ?: "" );
          logger->trace( "   - MsgHint:      '{}'", PQresultErrorField( result, PG_DIAG_MESSAGE_HINT ) ?: "" );
          logger->trace( "   - StmtPos:      '{}'", PQresultErrorField( result, PG_DIAG_STATEMENT_POSITION ) ?: "" );
          logger->trace( "   - IntPos:       '{}'", PQresultErrorField( result, PG_DIAG_INTERNAL_POSITION ) ?: "" );
          logger->trace( "   - IntQuery:     '{}'", PQresultErrorField( result, PG_DIAG_INTERNAL_QUERY ) ?: "" );
          logger->trace( "   - Context:      '{}'", PQresultErrorField( result, PG_DIAG_CONTEXT ) ?: "" );
          logger->trace( "   - Schema:       '{}'", PQresultErrorField( result, PG_DIAG_SCHEMA_NAME ) ?: "" );
          logger->trace( "   - Table:        '{}'", PQresultErrorField( result, PG_DIAG_TABLE_NAME ) ?: "" );
          logger->trace( "   - Column:       '{}'", PQresultErrorField( result, PG_DIAG_COLUMN_NAME ) ?: "" );
          logger->trace( "   - DataType:     '{}'", PQresultErrorField( result, PG_DIAG_DATATYPE_NAME ) ?: "" );
          logger->trace( "   - Constraint:   '{}'", PQresultErrorField( result, PG_DIAG_CONSTRAINT_NAME ) ?: "" );
          logger->trace( "   - File:         '{}'", PQresultErrorField( result, PG_DIAG_SOURCE_FILE ) ?: "" );
          logger->trace( "   - Line:         '{}'", PQresultErrorField( result, PG_DIAG_SOURCE_LINE ) ?: "" );
          logger->trace( "   - Function:     '{}'", PQresultErrorField( result, PG_DIAG_SOURCE_FUNCTION ) ?: "" );
        }
      }
    }

#define PG_RESULT_PROCESS( result, cxn, msg )                                                                          \
  do {                                                                                                                 \
    switch ( PQresultStatus( result ) ) {                                                                              \
      case PGRES_BAD_RESPONSE:                                                                                         \
      case PGRES_NONFATAL_ERROR:                                                                                       \
      case PGRES_FATAL_ERROR: {                                                                                        \
        auto errField = std::string{ PQresultErrorField( result, PG_DIAG_SQLSTATE ) };                                 \
        auto errMsg   = std::string{ PQresultErrorMessage( result ) };                                                 \
                                                                                                                       \
        result_trace( result, msg );                                                                                   \
                                                                                                                       \
        PQclear( result );                                                                                             \
        cxn->rollback( );                                                                                              \
                                                                                                                       \
        DBCPP_EXCEPTION( "{}: {}) {}", msg, errField, errMsg );                                                        \
      }                                                                                                                \
      default:                                                                                                         \
        break;                                                                                                         \
    }                                                                                                                  \
  } while ( 0 )

    /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

    struct PostgreSQLDriver : public dbcpp::Driver::Base {
      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

      struct PSQLConnection : public interface::Connection, public std::enable_shared_from_this< PSQLConnection > {
        /* Members */

        std::map< std::string, bool > prepared;
        std::shared_ptr< PGconn >     pgcxn;
        std::string                   uri;
        bool                          integer_datetimes;
        bool                          autoCommit;

        /* Implemented Interface */
        explicit PSQLConnection( Uri *const uri )
          : uri( uri->toString( ).replace( 0, 4, "postgres" ) )
          , integer_datetimes( false )
          , autoCommit( false ) {}

        DBStatement createStatement( std::string query ) override {
          auto binds = normalizeParameters( query, query );

          return std::make_shared< PSQLStatement >( shared_from_this( ), query, binds );
        }

        bool connect( ) override {
          pgcxn.reset( PQconnectdb( uri.c_str( ) ), PQfinish );

          if ( CONNECTION_OK == PQstatus( pgcxn.get( ) ) ) {
            integer_datetimes = strcmp( PQparameterStatus( pgcxn.get( ), "integer_datetimes" ) ?: "null", "on" ) == 0;
            PQsetErrorVerbosity( pgcxn.get( ), PQERRORS_VERBOSE );

            LOG( logger, debug, "Successfully connected to {}", uri );
            LOG(
              logger, trace, "Connection to {} does{} have integer date times", uri, integer_datetimes ? "" : " not" );

            begin( );
            return true;
          }

          return false;
        }

        void setAutoCommit( bool ac ) override {
          LOG( logger, trace, "{} auto commit", ac ? "Enabling" : "Disabling" );
          autoCommit = ac;
        }

        void commit( ) override {
          try {
            Statement statement = createStatement( "COMMIT" );
            statement.execute( );
            begin( );
          } catch ( DBException &ex ) {
            begin( );
            throw ex;
          }
        }

        void rollback( ) override {
          try {
            Statement statement = createStatement( "ROLLBACK" );
            statement.execute( );
            begin( );
          } catch ( DBException &ex ) {
            begin( );
            throw ex;
          }
        }

        void begin( ) {
          Statement statement = createStatement( "BEGIN" );
          statement.execute( );
        }

        bool disconnect( ) override {
          LOG( logger, trace, "Disconnecting from {}", uri );
          prepared.clear( );
          pgcxn.reset( );
          return true;
        }

        bool reconnect( ) override {
          disconnect( );
          return connect( );
        }

        bool test( ) override {
          try {
            Statement statement = createStatement( "SELECT 1::int" );
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

      struct PSQLStatement : public interface::Statement, public std::enable_shared_from_this< PSQLStatement > {
        struct AnyTypeVisitor : public boost::static_visitor<> {
          std::vector< const void * > *parameters;

          explicit AnyTypeVisitor( std::vector< const void * > *params )
            : parameters( params ) {}

          template < class T >
          void operator( )( T &value ) {
            parameters->push_back( &value );
          }
          void operator( )( decltype( nullptr ) ) { parameters->push_back( nullptr ); }
          void operator( )( std::string &value ) { parameters->push_back( value.c_str( ) ); }
          void operator( )( std::vector< uint8_t > &value ) { parameters->push_back( &value[ 0 ] ); }
        };

        typedef boost::variant< decltype( nullptr ),
                                bool,
                                int8_t,
                                int16_t,
                                int32_t,
                                int64_t,
                                float,
                                double,
                                std::string,
                                std::vector< uint8_t > >
          AnyType;

        std::shared_ptr< PSQLConnection > connection;
        std::vector< AnyType >            paramValues;
        std::vector< const void * >       parameters;
        std::vector< Oid >                paramTypes;
        std::vector< int >                paramLengths;
        std::vector< int >                paramFormats;
        std::string                       query;
        std::string                       id;
        std::vector< std::string >        columnNames;
        QueryType                         type;
        PGresult *                        result;
        size_t                            binds;
        size_t                            fields;
        size_t                            rows;

        PSQLStatement( std::shared_ptr< PSQLConnection > _connection, std::string _query, size_t _binds )
          : connection( std::move( _connection ) )
          , query( std::move( _query ) )
          , id( fmt::format( "stmt_{:X}", std::hash< std::string >{ }( query ) ) )
          , type( queryType( query ) )
          , result( nullptr )
          , binds( _binds )
          , fields( 0 )
          , rows( 0 ) {

          if ( query.length( ) == 0 ) {
            DBCPP_EXCEPTION( "Query is empty" );
          }

          if ( type == SELECT ) {
            bool forUpdate = find_an( query, FOR_UPDATE_PAT ).first != query.end( );

            query = fmt::format( "DECLARE {} CURSOR WITH{} HOLD FOR {}", id, forUpdate ? "OUT" : "", query );
          }

          parameters.reserve( binds );
          paramValues.resize( binds );
          paramTypes.resize( binds );
          paramLengths.resize( binds );
          paramFormats.resize( binds, 1 );
        }

        ~PSQLStatement( ) {
          if ( result != nullptr ) {
            PQclear( result );
          }

          if ( type == SELECT ) {
            auto closeQuery = fmt::format( "CLOSE {}", id );

            LOG( logger, trace, "Performing cursor close for {}", id );

            result =
              PQexecParams( connection->pgcxn.get( ), closeQuery.c_str( ), 0, nullptr, nullptr, nullptr, nullptr, 1 );

            result_trace( result, "Execute statement" );

            PQclear( result );
          }
        }

        bool setParamNull( size_t parameter, FieldType type ) override {
          if ( parameter < binds ) {
            const char *typeStr = "";
            Oid         oid     = VOIDOID;

#define FROM( x, y )                                                                                                   \
  case FieldType::x: {                                                                                                 \
    typeStr = { #x };                                                                                                  \
    oid     = y;                                                                                                       \
    break;                                                                                                             \
  }

            switch ( type ) {
              FROM( TINYINT, CHAROID );
              FROM( SMALLINT, INT2OID );
              FROM( INTEGER, INT4OID );
              FROM( BIGINT, INT8OID );
              FROM( REAL, FLOAT4OID );
              FROM( FLOAT, FLOAT8OID );
              FROM( DOUBLE, FLOAT8OID );
              FROM( BIT, BITOID );
              FROM( VARBIT, VARBITOID );
              FROM( BYTE, CHAROID );
              FROM( VARBYTE, BYTEAOID );
              FROM( CHAR, CHAROID );
              FROM( VARCHAR, VARCHAROID );
              FROM( DATE, DATEOID );
              FROM( TIME, TIMEOID );
              FROM( TIMESTAMP, TIMESTAMPOID );
              FROM( INET_ADDRESS, INETOID );
              FROM( MAC_ADDRESS, MACADDROID );
              FROM( BLOB, BYTEAOID );
              FROM( TEXT, TEXTOID );
              FROM( ROWID, OIDOID );
              FROM( BOOLEAN, BOOLOID );
              FROM( JSON, JSONOID );
              FROM( UUID, UUIDOID );
              FROM( XML, XMLOID );
              default:
                break;
            }
#undef FROM

            LOG( logger, trace, "Set parameter #{} of type {} to null", parameter + 1, typeStr );

            paramValues[ parameter ]  = AnyType( nullptr );
            paramTypes[ parameter ]   = oid;
            paramLengths[ parameter ] = 0;
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, uint8_t value ) override { return setParam( parameter, ( int16_t ) value ); }
        bool setParam( size_t parameter, uint16_t value ) override { return setParam( parameter, ( int32_t ) value ); }
        bool setParam( size_t parameter, uint32_t value ) override { return setParam( parameter, ( int64_t ) value ); }
        bool setParam( size_t parameter, uint64_t value ) override { return setParam( parameter, ( int64_t ) value ); }

        bool setParam( size_t parameter, dbcpp::interface::safebool value ) override {
          if ( parameter < binds ) {
            LOG( logger, trace, "Set parameter #{} to bool", parameter + 1 );

            paramValues[ parameter ]  = AnyType( ( bool ) value.val );
            paramTypes[ parameter ]   = BOOLOID;
            paramLengths[ parameter ] = sizeof( value.val );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, int8_t value ) override {
          if ( parameter < binds ) {
            LOG( logger, trace, "Set parameter #{} to byte", parameter + 1 );

            paramValues[ parameter ]  = AnyType( ( int8_t ) value );
            paramTypes[ parameter ]   = CHAROID;
            paramLengths[ parameter ] = sizeof( value );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, int16_t value ) override {
          if ( parameter < binds ) {
            LOG( logger, trace, "Set parameter #{} to short", parameter + 1 );

            paramValues[ parameter ]  = AnyType( ( int16_t ) htobe16( value ) );
            paramTypes[ parameter ]   = INT2OID;
            paramLengths[ parameter ] = sizeof( value );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, int32_t value ) override {
          if ( parameter < binds ) {
            LOG( logger, trace, "Set parameter #{} to int", parameter + 1 );

            paramValues[ parameter ]  = AnyType( ( int32_t ) htobe32( value ) );
            paramTypes[ parameter ]   = INT4OID;
            paramLengths[ parameter ] = sizeof( value );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, int64_t value ) override {
          if ( parameter < binds ) {
            LOG( logger, trace, "Set parameter #{} to long long", parameter + 1 );

            paramValues[ parameter ]  = AnyType( ( int64_t ) htobe64( value ) );
            paramTypes[ parameter ]   = INT8OID;
            paramLengths[ parameter ] = sizeof( value );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, float value ) override {
          int32_t val = *reinterpret_cast< int32_t * >( &value );

          if ( parameter < binds ) {
            LOG( logger, trace, "Set parameter #{} to float", parameter + 1 );

            paramValues[ parameter ]  = AnyType( ( int32_t ) htobe32( val ) );
            paramTypes[ parameter ]   = FLOAT4OID;
            paramLengths[ parameter ] = sizeof( val );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, double value ) override {
          int64_t val = *reinterpret_cast< int64_t * >( &value );

          if ( parameter < binds ) {
            LOG( logger, trace, "Set parameter #{} to double", parameter + 1 );

            paramValues[ parameter ]  = AnyType( ( int64_t ) htobe64( val ) );
            paramTypes[ parameter ]   = FLOAT8OID;
            paramLengths[ parameter ] = sizeof( val );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, long double value ) override {
          return setParam( parameter, ( double ) value );
        }

        bool setParam( size_t parameter, std::string value ) override {
          if ( parameter < binds ) {
            LOG( logger, trace, "Set parameter #{} to string", parameter + 1 );

            paramValues[ parameter ]  = AnyType( value );
            paramTypes[ parameter ]   = VARCHAROID;
            paramLengths[ parameter ] = value.size( );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, std::vector< uint8_t > value ) override {
          if ( parameter < binds ) {
            LOG( logger, trace, "Set parameter #{} to bytea", parameter + 1 );

            paramValues[ parameter ]  = AnyType( value );
            paramTypes[ parameter ]   = BYTEAOID;
            paramLengths[ parameter ] = value.size( );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, DBTime value ) override {
          if ( parameter < binds ) {
            LOG( logger, trace, "Set parameter #{} to long long", parameter + 1 );

            if ( connection->integer_datetimes ) {
              auto _time = value.time_since_epoch( );
              auto micro = ( std::chrono::duration_cast< std::chrono::microseconds >( _time ) - PSQLEpoch ).count( );

              paramValues[ parameter ]  = AnyType( ( int64_t ) htobe64( micro ) );
              paramTypes[ parameter ]   = TIMESTAMPOID;
              paramLengths[ parameter ] = sizeof( micro );

              return true;
            }
          }

          return false;
        }

        void execute( ) override {
          PQclear( result );

#if 0
          if ( paramTypes.empty( ) ) {
            executeStatement( );
          } else {
            executePrepared( );
          }
#endif

          executePrepared( );

          LOG( logger, trace, "Query {} resulted in {} fields", query, fields );

          if ( connection->autoCommit ) {
            auto cmd = query.substr( 0, 6 );

            std::transform( cmd.begin( ), cmd.end( ), cmd.begin( ), []( char ch ) { return std::tolower( ch ); } );

            if ( ( cmd == "delete" ) || ( cmd == "update" ) || ( cmd == "insert" ) ) {
              LOG( logger, trace, "Performing autocommit" );
              connection->commit( );
            }
          }
        }

        void executeStatement( ) {
          LOG( logger, trace, "Performing query: {}", query );

          result = PQexecParams( connection->pgcxn.get( ), query.c_str( ), 0, nullptr, nullptr, nullptr, nullptr, 1 );

          result_trace( result, "Execute statement" );

          PG_RESULT_PROCESS( result, connection, "Error encountered while executing statement" );

          fields = PQnfields( result );
        }

        void executePrepared( ) {
          if ( !connection->prepared[ id ] ) {
            LOG( logger, trace, "Preparing query {}", query );

            result =
              PQprepare( connection->pgcxn.get( ), id.c_str( ), query.c_str( ), paramTypes.size( ), &paramTypes[ 0 ] );

            if ( result == nullptr ) {
              DBCPP_EXCEPTION( "Error encountered while preparing statement" );
            }

            result_trace( result, "Prepare" );

            PG_RESULT_PROCESS( result, connection, "Error encountered while preparing statement" );

            LOG( logger, trace, "Statement preparation complete" );

            connection->prepared[ id ] = true;

            PQclear( result );
          } else {
            LOG( logger, trace, "Statement already prepared" );
          }

          /*
          if ( !cursor ) {
            result = PQdescribePrepared( connection->pgcxn.get( ), id.c_str( ) );
          } else {
            result = PQdescribePortal( connection->pgcxn.get( ), id.c_str( ) );
          }

          if ( result == nullptr ) {
            DBCPP_EXCEPTION( "Error encountered while getting statement meta-data" );
          }

          result_trace( result, "Describe prepared" );

          PG_RESULT_PROCESS( result, connection, "Error encountered while describing prepared statement" );

          PQclear( result );
          */

          {
            AnyTypeVisitor visitor( &parameters );
            parameters.clear( );
            std::for_each( paramValues.begin( ), paramValues.end( ), boost::apply_visitor( visitor ) );
          }

          result = PQexecPrepared( connection->pgcxn.get( ),
                                   id.c_str( ),
                                   parameters.size( ),
                                   ( const char *const * ) &parameters[ 0 ],
                                   &paramLengths[ 0 ],
                                   &paramFormats[ 0 ],
                                   1 );

          if ( result == nullptr ) {
            DBCPP_EXCEPTION( "Error encountered while executing statement, connection reset" );
          }

          result_trace( result, "Execute prepared statement" );

          PG_RESULT_PROCESS( result, connection, "Error encountered while executing prepared statement" );

          fetch( );
        }

        bool fetch( ) {
          if ( type == SELECT ) {
            auto fetchQuery = fmt::format( "FETCH FORWARD 100 FROM {}", id );

            PQclear( result );

            result =
              PQexecParams( connection->pgcxn.get( ), fetchQuery.c_str( ), 0, nullptr, nullptr, nullptr, nullptr, 1 );

            if ( result == nullptr ) {
              DBCPP_EXCEPTION( "Failed to fetch the next 100 rows from cursor {}", id );
            }

            result_trace( result, "Fetch forward" );

            PG_RESULT_PROCESS( result, connection, "Error enountered while executing cursor fetch" );
          }

          /* First fetch...  */
          if ( rows == 0 ) {
            fields = PQnfields( result );

            for ( size_t field = 0; field < fields; ++field ) {
              std::string name = PQfname( result, field );

              std::transform( name.begin( ), name.end( ), name.begin( ), ::toupper );

              columnNames.push_back( name );
            }

            LOG( logger, trace, "Result fields: ({}) {}", columnNames.size( ), fmt::join( columnNames, ", " ) );
          }

          rows = PQntuples( result );

          return rows > 0;
        }

        int executeUpdate( ) override {
          execute( );

          const char *tuples = PQcmdTuples( result );
          return *tuples ? std::stol( tuples ) : 0;
        }

        DBResultSet getResults( ) override {
          return !result ? nullptr : std::make_shared< PSQLResultSet >( shared_from_this( ) );
        }
      };

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */
      struct PSQLResultSet;

      struct PSQLField : public interface::Field {
        PSQLResultSet *results;
        size_t         field = 0;

        PSQLField( PSQLResultSet *_results, size_t _field )
          : results( _results )
          , field( _field ) {}

        FieldType type( ) const override {
          switch ( PQftype( results->stmt->result, field ) ) {
            case INT2OID:
              return FieldType::SMALLINT;
            case INT4OID:
              return FieldType::INTEGER;
            case INT8OID:
              return FieldType::BIGINT;
            case NUMERICOID:
              return FieldType::NUMERIC;
            case FLOAT4OID:
              return FieldType::FLOAT;
            case FLOAT8OID:
              return FieldType::DOUBLE;
            case BITOID:
              return FieldType::BIT;
            case VARBITOID:
              return FieldType::VARBIT;
            case BYTEAOID:
              return FieldType::VARBYTE;
            case CHAROID:
              return FieldType::CHAR;
            case NAMEOID:
            case VARCHAROID:
              return FieldType::VARCHAR;
            case DATEOID:
              return FieldType::DATE;
            case TIMEOID:
              return FieldType::TIME;
            case TIMETZOID:
              return FieldType::TIME;
            case TIMESTAMPOID:
              return FieldType::TIMESTAMP;
            case TIMESTAMPTZOID:
              return FieldType::TIMESTAMP;
            case MACADDROID:
              return FieldType::MAC_ADDRESS;
            case INETOID:
              return FieldType::INET_ADDRESS;
            case TEXTOID:
              return FieldType::TEXT;
            case OIDOID:
              return FieldType::ROWID;
            case BOOLOID:
              return FieldType::BOOLEAN;
            case JSONOID:
              return FieldType::JSON;
            case UUIDOID:
              return FieldType::UUID;
            case XMLOID:
              return FieldType::XML;
            default:
              break;
          }
          return FieldType::UNKNOWN;
        }

        bool isNull( ) const override { return PQgetisnull( results->stmt->result, results->current, field ) != 0; }

        void *value( ) const { return PQgetvalue( results->stmt->result, results->current, field ); }

        size_t length( ) const { return PQgetlength( results->stmt->result, results->current, field ); }

        std::string name( ) const override { return results->fieldName( field ); }

        bool    getBool( ) const { return getU8( ); }
        uint8_t getU8( ) const { return getI8( ); }
        int8_t  getI8( ) const {
          return ( PQbinaryTuples( results->stmt->result ) ) //
                   ? ( *( int8_t * ) value( ) )
                   : std::stoi( std::string{ ( char * ) value( ) } );
        }
        uint16_t getU16( ) const { return getI16( ); }
        int16_t  getI16( ) const {
          return ( PQbinaryTuples( results->stmt->result ) ) //
                   ? ( be16toh( *( int16_t * ) value( ) ) )
                   : std::stoi( std::string{ ( char * ) value( ) } );
        }
        uint32_t getU32( ) const { return getI32( ); }
        int32_t  getI32( ) const {
          return ( PQbinaryTuples( results->stmt->result ) ) //
                   ? ( be32toh( *( int32_t * ) value( ) ) )
                   : std::stoi( std::string{ ( char * ) value( ) } );
        }
        uint64_t getU64( ) const { return getI64( ); }
        int64_t  getI64( ) const {
          return ( PQbinaryTuples( results->stmt->result ) ) //
                   ? ( be64toh( *( int64_t * ) value( ) ) )
                   : std::stoll( std::string{ ( char * ) value( ) } );
        }
        double getDouble( ) const {
          if ( PQbinaryTuples( results->stmt->result ) ) {
            int64_t tmp = be64toh( *( int64_t * ) value( ) );
            return *( double * ) &tmp;
          }
          return std::stod( std::string{ ( char * ) value( ) } );
        }
        long double getLongDouble( ) const { return getDouble( ); }
        float       getFloat( ) const {
          if ( PQbinaryTuples( results->stmt->result ) ) {
            int32_t tmp = be32toh( *( int32_t * ) value( ) );
            return *( float * ) &tmp;
          }
          return std::stof( std::string{ ( char * ) value( ) } );
        }
        std::string            getString( ) const { return std::string( ( char * ) value( ) ); }
        std::vector< uint8_t > getBlob( ) const {
          auto                   ptr  = ( uint8_t * ) value( );
          size_t                 size = length( );
          std::vector< uint8_t > blob( size );
          std::copy( ptr, ptr + size, blob.begin( ) );
          return blob;
        }
        DBTime getDate( ) const {
          if ( results->stmt->connection->integer_datetimes == true ) {
            auto   val = std::chrono::seconds( getU32( ) * 86400 );
            DBTime _time( PSQLEpoch + val );

            return _time;
          } else {
            return DBTime( std::chrono::seconds( 0 ) );
          }
        }
        DBTime getTime( ) const {
          if ( results->stmt->connection->integer_datetimes == true ) {
            std::chrono::microseconds duration( getU64( ) );
            DBTime                    _time( duration + PSQLEpoch );

            return _time;
          } else {
            return DBTime( std::chrono::seconds( 0 ) );
          }
        }
      };

      struct PSQLVarcharField : PSQLField {
        PSQLVarcharField( PSQLResultSet *results, size_t field )
          : PSQLField( results, field ) {}

        void get( std::string &val ) const override { val = getString( ); }
      };

      struct PSQLByteAField : PSQLField {
        PSQLByteAField( PSQLResultSet *results, size_t field )
          : PSQLField( results, field ) {}

        void get( std::vector< uint8_t > &val ) const override { val = getBlob( ); }
      };

      struct PSQLBoolField : PSQLField {
        PSQLBoolField( PSQLResultSet *results, size_t field )
          : PSQLField( results, field ) {}
        void get( bool &val ) const override { val = getBool( ); }
        void get( uint64_t &val ) const override { val = getU64( ); }
        void get( std::string &val ) const override { val = getBool( ) ? "true" : "false"; }
      };

      struct PSQLCharField : PSQLField {
        PSQLCharField( PSQLResultSet *results, size_t field )
          : PSQLField( results, field ) {}
        void get( bool &val ) const override { val = getBool( ); }
        void get( uint8_t &val ) const override { val = getU8( ); }
        void get( uint64_t &val ) const override { val = getU8( ); }
        void get( std::string &val ) const override {
          std::stringstream ss;
          ss << getI8( );
          val = ss.str( );
        }
      };

      struct PSQLInt2Field : PSQLField {
        PSQLInt2Field( PSQLResultSet *results, size_t field )
          : PSQLField( results, field ) {}
        void get( bool &val ) const override { val = getBool( ); }
        void get( uint16_t &val ) const override { val = getU16( ); }
        void get( uint64_t &val ) const override { val = getU16( ); }
        void get( std::string &val ) const override {
          std::stringstream ss;
          ss << getI16( );
          val = ss.str( );
        }
      };

      struct PSQLInt4Field : PSQLField {
        PSQLInt4Field( PSQLResultSet *results, size_t field )
          : PSQLField( results, field ) {}
        void get( bool &val ) const override { val = getBool( ); }
        void get( uint32_t &val ) const override { val = getU32( ); }
        void get( uint64_t &val ) const override { val = getU32( ); }
        void get( std::string &val ) const override {
          std::stringstream ss;
          ss << getI32( );
          val = ss.str( );
        }
      };

      struct PSQLInt8Field : PSQLField {
        PSQLInt8Field( PSQLResultSet *results, size_t field )
          : PSQLField( results, field ) {}
        void get( bool &val ) const override { val = getBool( ); }
        void get( uint64_t &val ) const override { val = getU64( ); }
        void get( std::string &val ) const override {
          std::stringstream ss;
          ss << getI64( );
          val = ss.str( );
        }
      };

      struct PSQLFloat4Field : PSQLField {
        PSQLFloat4Field( PSQLResultSet *results, size_t field )
          : PSQLField( results, field ) {}
        void get( float &val ) const override { val = getFloat( ); }
        void get( long double &val ) const override { val = getFloat( ); }
        void get( std::string &val ) const override {
          std::stringstream ss;
          ss << getFloat( );
          val = ss.str( );
        }
      };

      struct PSQLFloat8Field : PSQLField {
        PSQLFloat8Field( PSQLResultSet *results, size_t field )
          : PSQLField( results, field ) {}
        void get( double &val ) const override { val = getDouble( ); }
        void get( long double &val ) const override { val = getDouble( ); }
        void get( std::string &val ) const override {
          std::stringstream ss;
          ss << getDouble( );
          val = ss.str( );
        }
      };

      struct PSQLTimestampField : PSQLField {
        PSQLTimestampField( PSQLResultSet *results, size_t field )
          : PSQLField( results, field ) {}
        void get( DBTime &val ) const override { val = getTime( ); }
        void get( std::string &val ) const override {
          std::stringstream ss;
          struct tm         tm     = { 0 };
          auto              dbTime = getTime( );
          time_t            _time  = DBClock::to_time_t( dbTime );
          auto              us     = dbTime - std::chrono::seconds( _time );

          gmtime_r( &_time, &tm );

          ss << std::setfill( ' ' )   //
             << std::setw( 4 )        //
             << ( 1900 + tm.tm_year ) //
             << "-"                   //
             << std::setfill( '0' )   //
             << std::setw( 2 )        //
             << ( tm.tm_mon + 1 )     //
             << "-"                   //
             << std::setfill( '0' )   //
             << std::setw( 2 )        //
             << tm.tm_mday            //
             << ' '                   //
             << std::setfill( '0' )   //
             << std::setw( 2 )        //
             << tm.tm_hour            //
             << ':'                   //
             << std::setfill( '0' )   //
             << std::setw( 2 )        //
             << tm.tm_min             //
             << ':'                   //
             << std::setfill( '0' )   //
             << std::setw( 2 )        //
             << tm.tm_sec             //
             << '.'                   //
             << std::setfill( '0' )   //
             << std::setw( 6 )        //
             << std::chrono::time_point_cast< std::chrono::microseconds >( us ).time_since_epoch( ).count( );

          val = ss.str( );
        }
      };

      struct PSQLDateField : PSQLField {
        PSQLDateField( PSQLResultSet *results, size_t field )
          : PSQLField( results, field ) {}
        void get( DBTime &val ) const override { val = getDate( ); }
        void get( std::string &val ) const override {
          std::stringstream ss;
          struct tm         tm     = { 0 };
          auto              dbTime = getDate( );
          time_t            _time  = DBClock::to_time_t( dbTime );

          gmtime_r( &_time, &tm );

          ss << std::setfill( ' ' )   //
             << std::setw( 4 )        //
             << ( 1900 + tm.tm_year ) //
             << "-"                   //
             << std::setfill( '0' )   //
             << std::setw( 2 )        //
             << ( tm.tm_mon + 1 )     //
             << "-"                   //
             << std::setfill( '0' )   //
             << std::setw( 2 )        //
             << tm.tm_mday;           //

          val = ss.str( );
        }
      };

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

      struct PSQLResultSet : public interface::ResultSet, public std::enable_shared_from_this< PSQLResultSet > {
        std::vector< std::unique_ptr< PSQLField > > columns;
        std::shared_ptr< PSQLStatement >            stmt;
        size_t                                      current;

        PSQLResultSet( std::shared_ptr< PSQLStatement > _stmt )
          : stmt( std::move( _stmt ) )
          , current( -1 ) {

          columns.reserve( stmt->fields );

          for ( size_t field = 0; field < stmt->fields; ++field ) {
            switch ( PQftype( stmt->result, field ) ) {
              case BYTEAOID: {
                columns.emplace_back( std::unique_ptr< PSQLField >( new PSQLByteAField( this, field ) ) );
                break;
              }
              case NAMEOID:
              case CSTRINGOID:
              case VARCHAROID: {
                columns.emplace_back( std::unique_ptr< PSQLField >( new PSQLVarcharField( this, field ) ) );
                break;
              }
              case BOOLOID: {
                columns.emplace_back( std::unique_ptr< PSQLField >( new PSQLBoolField( this, field ) ) );
                break;
              }
              case INT2OID: {
                columns.emplace_back( std::unique_ptr< PSQLField >( new PSQLInt2Field( this, field ) ) );
                break;
              }
              case INT4OID: {
                columns.emplace_back( std::unique_ptr< PSQLField >( new PSQLInt4Field( this, field ) ) );
                break;
              }
              case INT8OID: {
                columns.emplace_back( std::unique_ptr< PSQLField >( new PSQLInt8Field( this, field ) ) );
                break;
              }
              case FLOAT4OID: {
                columns.emplace_back( std::unique_ptr< PSQLField >( new PSQLFloat4Field( this, field ) ) );
                break;
              }
              case FLOAT8OID: {
                columns.emplace_back( std::unique_ptr< PSQLField >( new PSQLFloat8Field( this, field ) ) );
                break;
              }
              case TIMESTAMPOID:
              case TIMESTAMPTZOID: {
                columns.emplace_back( std::unique_ptr< PSQLField >( new PSQLTimestampField( this, field ) ) );
                break;
              }
              case DATEOID: {
                columns.emplace_back( std::unique_ptr< PSQLField >( new PSQLDateField( this, field ) ) );
                break;
              }
              default: {
                columns.emplace_back( std::unique_ptr< PSQLField >( new PSQLField( this, field ) ) );
                break;
              }
            }
          }
        }

        std::string                fieldName( size_t field ) const { return stmt->columnNames[ field ]; }
        std::vector< std::string > fieldNames( ) const override { return stmt->columnNames; }
        size_t                     fields( ) const override { return stmt->fields; }
        size_t                     rows( ) const override { return stmt->rows; }
        size_t                     row( ) const override { return current; }
        bool                       next( ) override {
          if ( ++current >= rows( ) ) {
            if ( stmt->fetch( ) ) {
              current = 0;
            }
          }
          return current < rows( );
        }

        DBField get( size_t field ) const override {
          if ( field >= columns.size( ) ) {
            DBCPP_EXCEPTION( "Field index is out of range" );
          }
          return std::shared_ptr< interface::Field >( shared_from_this( ), columns[ field ].get( ) );
        }

        DBField get( std::string name ) const override {
          std::transform( name.begin( ), name.end( ), name.begin( ), ::toupper );
          auto iterator = std::find( stmt->columnNames.begin( ), stmt->columnNames.end( ), name );

          if ( iterator == stmt->columnNames.end( ) ) {
            DBCPP_EXCEPTION( "Unknown field named: {}", name );
          }

          return get( iterator - stmt->columnNames.begin( ) );
        }
      };

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

      DBConnection createConnection( Uri *const uri ) { return std::make_shared< PSQLConnection >( uri ); }

      ~PostgreSQLDriver( ) = default;
    }; // namespace psql

    PostgreSQLDriver driver;
  } // namespace psql
} // namespace dbcpp

extern "C" {
/**
 * @brief Get the driver implementation
 */
dbcpp::Driver::Base *getDriver( ) {
  return dynamic_cast< dbcpp::Driver::Base * >( &dbcpp::psql::driver );
}
}
