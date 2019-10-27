
#include "dbc++/dbcpp.hh"
#include <cspublic.h>
#include <ctpublic.h>

#include <algorithm>
#include <boost/variant.hpp>
#include <chrono>

namespace dbcpp {
  namespace tds {
    using DBConnection = std::shared_ptr< interface::connection >;
    using DBStatement  = std::shared_ptr< interface::Statement >;
    using DBResultSet  = std::shared_ptr< interface::ResultSet >;
    using DBField      = std::shared_ptr< interface::Field >;
    using FieldType    = internal::FieldType;

    /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

    struct FreeTDSDriver {
      static FreeTDSDriver registrar;

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

      struct TDSConnection : public dbcpp::DBConnection, public std::enable_shared_from_this< TDSConnection > {
        /* Members */

        std::map< std::string, bool >    prepared;
        std::shared_ptr< CS_CONTEXT >    context;
        std::shared_ptr< CS_CONNECTION > connection;
        Uri *                            uri;
        bool                             autoCommit;
        int                              version;

        /* Implemented Interface */
        TDSConnection( Uri *_uri ), autoCommit( false ), uri( _uri ), version( CS_VERSION_150 ) {
          CS_CONTEXT *ctx = nullptr;

          if ( cs_ctx_alloc( version, &ctx ) != CS_SUCCESS ) {
            return false;
          }

          if ( ct_init( &ctx, version ) != CS_SUCCCESS ) {
            cs_ctx_drop( ctx );

            return false;
          }

          context.reset( ctx, []( CS_CONTEXT *ctx ) {
            ct_exit( ctx, CS_UNUSED );
            cs_ctx_drop( ctx );
          } );
        }

        DBStatement createStatement( std::string query ) {
          auto binds = 0;

          return std::make_shared< TDSStatement >( shared_from_this( ), query, binds );
        }

        bool connect( ) {
          auto cxn      = ( CS_CONNECTION * ) nullptr;
          auto appName  = char[ 256 ];
          auto trueProp = CS_TRUE;

          if ( ct_con_alloc( ctx, &cxn ) != CS_SUCCESS ) {
            return false;
          }

          snprintf( appName, sizeof( appName ), "TDS-DBC++:%ld", getpid( ) );

          /*
           * Set the username, password, application name, host-name, etc for this connection
           */
          if ( ( ct_con_props( cxn, CS_SET, CS_USERNAME, uri->user( ), CS_NULLTERM, 0x00 ) != CS_SUCCESS ) ||
               ( ct_con_props( cxn, CS_SET, CS_PASSWORD, uri->password( ), CS_NULLTERM, 0x00 ) != CS_SUCCESS ) ||
               ( ct_con_props( cxn, CS_SET, CS_APPNAME, appName, CS_NULLTERM, 0x00 ) != CS_SUCCESS ) ||
               ( ct_con_props( cxn, CS_SET, CS_BULK_LOGIN, &trueProp, sizeof( trueProp ), 0x00 ) != CS_SUCCESS ) ) {
            ct_con_drop( cxn );
            return false;
          }

          connection.reset( cxn, ct_con_drop );

          return true;
        }

        void setAutoCommit( bool ac ) { autoCommit = ac; }

        void commit( ) {
          try {
            Statement statement = createStatement( "COMMIT" );
            statement.execute( );
            begin( );
          } catch ( DBException ex ) {
            begin( );
            throw( ex );
          }
        }

        void rollback( ) {
          try {
            Statement statement = createStatement( "ROLLBACK" );
            statement.execute( );
            begin( );
          } catch ( DBException ex ) {
            begin( );
            throw( ex );
          }
        }

        void begin( ) {
          Statement statement = createStatement( "BEGIN" );
          statement.execute( );
        }

        bool disconnect( ) {
          connection.reset( );
          return true;
        }

        bool reconnect( ) {
          disconnect( );
          return connect( );
        }

        bool test( ) {
          try {
            Statement statement = createStatement( "SELECT 1" );
            ResultSet result    = statement.executeQuery( );

            if ( result.next( ) == true ) {
              return result.get< int32_t >( 0 ) == 1;
            }
          } catch ( ... ) {
          }
          return false;
        }
      };

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

      struct TDSStatement : public dbcpp::DBStatement, public std::enable_shared_from_this< TDSStatement > {

        struct AnyTypeVisitor : public boost::static_visitor<> {
          std::vector< const void * > *parameters;

          AnyTypeVisitor( std::vector< const void * > *params )
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
                                int8_t,
                                int16_t,
                                int32_t,
                                int64_t,
                                float,
                                double,
                                std::string,
                                std::vector< uint8_t > >
          AnyType;

        std::shared_ptr< TDSConnection > connection;
        std::vector< AnyType >           paramValues;
        std::vector< const void * >      parameters;
        std::vector< Oid >               paramTypes;
        std::vector< int >               paramLengths;
        std::vector< int >               paramFormats;
        std::string                      query;
        std::vector< std::string >       columnNames;
        PGresult *                       result;
        size_t                           binds;
        size_t                           fields;
        size_t                           affected;
        size_t                           rows;

        TDSStatement( std::shared_ptr< TDSConnection > _connection, std::string _query, size_t _binds )
          : connection( _connection )
          , query( _query )
          , result( nullptr )
          , binds( _binds )
          , fields( 0 )
          , affected( 0 )
          , rows( 0 ) {
          if ( query.length( ) == 0 ) {
            throw DBException( "Query is empty" );
          }

          parameters.reserve( binds );
          paramValues.resize( binds );
          paramTypes.resize( binds );
          paramLengths.resize( binds );
          paramFormats.resize( binds, 1 );
        }

        ~TDSStatement( ) {
          if ( result != nullptr ) {
            PQclear( result );
          }
        }

        bool setParamNull( size_t parameter, FieldType type ) {
          if ( parameter < binds ) {
            Oid oid = VOIDOID;

            switch ( type ) {
              case FieldType::TINYINT: {
                oid = CHAROID;
                break;
              }

              case FieldType::SMALLINT: {
                oid = INT2OID;
                break;
              }

              case FieldType::INTEGER: {
                oid = INT4OID;
                break;
              }

              case FieldType::BIGINT: {
                oid = INT8OID;
                break;
              }

              case FieldType::NUMERIC: {
                oid = NUMERICOID;
                break;
              }

              case FieldType::REAL: {
                oid = FLOAT4OID;
                break;
              }

              case FieldType::FLOAT:
              case FieldType::DOUBLE: {
                oid = FLOAT8OID;
                break;
              }

              case FieldType::BIT: {
                oid = BITOID;
                break;
              }

              case FieldType::VARBIT: {
                oid = VARBITOID;
                break;
              }

              case FieldType::BYTE: {
                oid = CHAROID;
                break;
              }

              case FieldType::VARBYTE: {
                oid = BYTEAOID;
                break;
              }

              case FieldType::CHAR: {
                oid = CHAROID;
                break;
              }

              case FieldType::VARCHAR: {
                oid = VARCHAROID;
                break;
              }

              case FieldType::DATE: {
                oid = DATEOID;
                break;
              }

              case FieldType::TIME: {
                oid = TIMEOID;
                break;
              }

              case FieldType::TIMESTAMP: {
                oid = TIMESTAMPOID;
                break;
              }

              case FieldType::INET_ADDRESS: {
                oid = INETOID;
                break;
              }

              case FieldType::MAC_ADDRESS: {
                oid = MACADDROID;
                break;
              }

              case FieldType::BLOB: {
                oid = BYTEAOID;
                break;
              }

              case FieldType::TEXT: {
                oid = TEXTOID;
                break;
              }

              case FieldType::ROWID: {
                oid = OIDOID;
                break;
              }

              case FieldType::BOOLEAN: {
                oid = BOOLOID;
                break;
              }

              case FieldType::JSON: {
                oid = JSONOID;
                break;
              }

              case FieldType::UUID: {
                oid = UUIDOID;
                break;
              }

              case FieldType::XML: {
                oid = XMLOID;
                break;
              }

              default: {
                oid = VOIDOID;
                break;
              }
            }

            paramValues[ parameter ]  = AnyType( nullptr );
            paramTypes[ parameter ]   = oid;
            paramLengths[ parameter ] = 0;
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, uint8_t value ) { return setParam( parameter, ( int16_t ) value ); }

        bool setParam( size_t parameter, uint16_t value ) { return setParam( parameter, ( int32_t ) value ); }

        bool setParam( size_t parameter, uint32_t value ) { return setParam( parameter, ( int64_t ) value ); }

        bool setParam( size_t parameter, uint64_t value ) { return setParam( parameter, ( int64_t ) value ); }

        bool setParam( size_t parameter, int8_t value ) {
          if ( parameter < binds ) {
            paramValues[ parameter ]  = AnyType( ( int8_t ) value );
            paramTypes[ parameter ]   = CHAROID;
            paramLengths[ parameter ] = sizeof( value );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, int16_t value ) {
          if ( parameter < binds ) {
            paramValues[ parameter ]  = AnyType( ( int16_t ) htobe16( value ) );
            paramTypes[ parameter ]   = INT2OID;
            paramLengths[ parameter ] = sizeof( value );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, int32_t value ) {
          if ( parameter < binds ) {
            paramValues[ parameter ]  = AnyType( ( int32_t ) htobe32( value ) );
            paramTypes[ parameter ]   = INT4OID;
            paramLengths[ parameter ] = sizeof( value );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, int64_t value ) {
          if ( parameter < binds ) {
            paramValues[ parameter ]  = AnyType( ( int64_t ) htobe64( value ) );
            paramTypes[ parameter ]   = INT8OID;
            paramLengths[ parameter ] = sizeof( value );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, float value ) {
          if ( setParam( parameter, *( int32_t * ) &value ) == true ) {
            paramTypes[ parameter ] = FLOAT4OID;
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, double value ) {
          if ( setParam( parameter, *( int64_t * ) &value ) == true ) {
            paramTypes[ parameter ] = FLOAT8OID;
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, long double value ) { return setParam( parameter, ( double ) value ); }

        bool setParam( size_t parameter, std::string value ) {
          if ( parameter < binds ) {
            paramValues[ parameter ]  = AnyType( value );
            paramTypes[ parameter ]   = VARCHAROID;
            paramLengths[ parameter ] = value.size( );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, std::vector< uint8_t > value ) {
          if ( parameter < binds ) {
            paramValues[ parameter ]  = AnyType( value );
            paramTypes[ parameter ]   = BYTEAOID;
            paramLengths[ parameter ] = value.size( );
            return true;
          }
          return false;
        }

        bool setParam( size_t parameter, DBTime value ) {
          if ( connection->integer_datetimes == true ) {
            auto _time = value.time_since_epoch( );
            auto micro = std::chrono::duration_cast< std::chrono::microseconds >( _time ) - TDSEpoch;

            if ( setParam( parameter, micro.count( ) ) == true ) {
              paramTypes[ parameter ] = TIMESTAMPOID;
              return true;
            }
          } else {
          }

          return false;
        }

        void execute( ) {
          auto cquery = query.c_str( );

          PQclear( result );

          if ( connection->prepared[ query ] == false ) {
            result = PQprepare( connection->pgcxn.get( ), cquery, cquery, paramTypes.size( ), &paramTypes[ 0 ] );

            if ( result == nullptr ) {
              throw DBException( "Error encountered while preparing statement" );
            }

            switch ( PQresultStatus( result ) ) {
              case PGRES_BAD_RESPONSE:
              case PGRES_NONFATAL_ERROR:
              case PGRES_FATAL_ERROR: {
                std::stringstream ss;

                ss << "Error encountered while preparing statement: "        //
                   << PQresultErrorField( result, PG_DIAG_SQLSTATE ) << ") " //
                   << PQresultErrorMessage( result );

                PQclear( result );

                connection->rollback( );

                throw DBException( ss.str( ) );
              }
              default:
                break;
            }

            connection->prepared[ query ] = true;

            PQclear( result );
          }

          result = PQdescribePrepared( connection->pgcxn.get( ), cquery );

          if ( result == nullptr ) {
            throw DBException( "Error encountered while getting statement meta-data" );
          }

          switch ( PQresultStatus( result ) ) {
            case PGRES_BAD_RESPONSE:
            case PGRES_NONFATAL_ERROR:
            case PGRES_FATAL_ERROR: {
              std::stringstream ss;

              ss << "Error encountered while describing preparing statement: " //
                 << PQresultErrorField( result, PG_DIAG_SQLSTATE ) << ") "     //
                 << PQresultErrorMessage( result );

              PQclear( result );

              connection->rollback( );

              throw DBException( ss.str( ) );
            }
            default:
              break;
          }

          fields = PQnfields( result );

          PQclear( result );

          {
            AnyTypeVisitor visitor( &parameters );
            std::for_each( paramValues.begin( ), paramValues.end( ), boost::apply_visitor( visitor ) );
          }

          result = PQexecPrepared( connection->pgcxn.get( ),
                                   cquery,
                                   parameters.size( ),
                                   ( const char *const * ) &parameters[ 0 ],
                                   &paramLengths[ 0 ],
                                   &paramFormats[ 0 ],
                                   1 );

          if ( result == nullptr ) {
            result = PQexec( connection->pgcxn.get( ), "" );

            if ( result != nullptr ) {
              auto status = PQresultStatus( result );

              PQclear( result );

              switch ( status ) {
                case PGRES_EMPTY_QUERY:
                case PGRES_COMMAND_OK:
                case PGRES_TUPLES_OK: {
                  std::stringstream ss;

                  ss << "Error encountered while executing statement"          //
                     << PQresultErrorField( result, PG_DIAG_SQLSTATE ) << ") " //
                     << PQresultErrorMessage( result );

                  PQclear( result );

                  connection->rollback( );

                  throw DBException( ss.str( ) );
                }
                default:
                  break;
              }
            }

            throw DBException( "Error encountered while executing statement, connection reset" );
          }

          switch ( PQresultStatus( result ) ) {
            case PGRES_BAD_RESPONSE:
            case PGRES_NONFATAL_ERROR:
            case PGRES_FATAL_ERROR: {
              std::stringstream ss;

              ss << "Error encountered while executing statement: "        //
                 << PQresultErrorField( result, PG_DIAG_SQLSTATE ) << ") " //
                 << PQresultErrorMessage( result );

              PQclear( result );

              connection->rollback( );

              throw DBException( ss.str( ) );
            }
            default:
              break;
          }

          if ( connection->autoCommit == true ) {
            if ( ( strncasecmp( cquery, "delete", 6 ) == 0 ) || ( strncasecmp( cquery, "update", 6 ) == 0 ) ||
                 ( strncasecmp( cquery, "insert", 6 ) == 0 ) ) {
              connection->commit( );
            }
          }
        }

        int executeUpdate( ) {
          execute( );

          affected = std::stol( PQcmdTuples( result ) ?: "0" );

          return affected;
        }

        DBResultSet getResults( ) {
          if ( result != nullptr ) {
            if ( rows == 0 ) {
              rows = PQntuples( result );

              for ( size_t field = 0; field < fields; ++field ) {
                std::string name = PQfname( result, field );

                std::transform( name.begin( ), name.end( ), name.begin( ), ::toupper );

                columnNames.push_back( name );
              }
            }

            return std::make_shared< TDSResultSet >( shared_from_this( ) );
          }

          return nullptr;
        }
      };

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */
      struct TDSResultSet;

      struct TDSField : public dbcpp::DBField {
        TDSResultSet *results;
        size_t        field = 0;

        TDSField( TDSResultSet *_results, size_t _field )
          : results( _results )
          , field( _field ) {}

        FieldType type( ) const {
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

        bool isNull( ) const { return PQgetisnull( results->stmt->result, results->current, field ) != 0; }

        void *value( ) const { return PQgetvalue( results->stmt->result, results->current, field ); }

        size_t length( ) const { return PQgetlength( results->stmt->result, results->current, field ); }

        std::string name( ) const { return results->fieldName( field ); }

        uint8_t getU8( ) const { return getI8( ); }

        int8_t getI8( ) const {
          int8_t val = *( int8_t * ) value( );
          return val;
        }

        uint16_t getU16( ) const { return getI16( ); }

        int16_t getI16( ) const {
          int16_t val = be16toh( *( int16_t * ) value( ) );
          return val;
        }

        uint32_t getU32( ) const { return getI32( ); }

        int32_t getI32( ) const {
          int32_t val = be32toh( *( int32_t * ) value( ) );
          return val;
        }

        uint64_t getU64( ) const { return getI64( ); }

        int64_t getI64( ) const {
          int64_t val = be64toh( *( int64_t * ) value( ) );
          return val;
        }

        double getDouble( ) const {
          int64_t tmp = getI64( );
          return *( double * ) &tmp;
        }

        long double getLongDouble( ) const { return getDouble( ); }

        float getFloat( ) const {
          int32_t tmp = getI32( );
          return *( float * ) &tmp;
        }

        std::string getString( ) const { return std::string( ( char * ) value( ) ); }

        std::vector< uint8_t > getBlob( ) const {
          uint8_t *              ptr  = ( uint8_t * ) value( );
          size_t                 size = length( );
          std::vector< uint8_t > blob( size );
          blob.insert( blob.begin( ), ptr, ptr + size );
          return blob;
        }

        DBTime getTime( ) const {
          if ( results->stmt->connection->integer_datetimes == true ) {
            std::chrono::microseconds duration( getU64( ) );
            DBTime                    _time( duration + TDSEpoch );

            return _time;
          } else {
            return DBTime( std::chrono::seconds( 0 ) );
          }
        }
      };

      struct TDSVarcharField : TDSField {

        TDSVarcharField( TDSResultSet *results, size_t field )
          : TDSField( results, field ) {}

        void get( std::string &val ) const { val = getString( ); }
      };

      struct TDSByteAField : TDSField {

        TDSByteAField( TDSResultSet *results, size_t field )
          : TDSField( results, field ) {}

        void get( std::vector< uint8_t > &val ) const { val = getBlob( ); }
      };

      struct TDSCharField : TDSField {

        TDSCharField( TDSResultSet *results, size_t field )
          : TDSField( results, field ) {}

        void get( uint8_t &val ) const { val = getU8( ); }

        void get( uint64_t &val ) const { val = getU8( ); }

        void get( std::string &val ) const {
          std::stringstream ss;
          ss << getI8( );
          val = ss.str( );
        }
      };

      struct TDSInt2Field : TDSField {

        TDSInt2Field( TDSResultSet *results, size_t field )
          : TDSField( results, field ) {}

        void get( uint16_t &val ) const { val = getU16( ); }

        void get( uint64_t &val ) const { val = getU16( ); }

        void get( std::string &val ) const {
          std::stringstream ss;
          ss << getI16( );
          val = ss.str( );
        }
      };

      struct TDSInt4Field : TDSField {

        TDSInt4Field( TDSResultSet *results, size_t field )
          : TDSField( results, field ) {}

        void get( uint32_t &val ) const { val = getU32( ); }

        void get( uint64_t &val ) const { val = getU32( ); }

        void get( std::string &val ) const {
          std::stringstream ss;
          ss << getI32( );
          val = ss.str( );
        }
      };

      struct TDSInt8Field : TDSField {

        TDSInt8Field( TDSResultSet *results, size_t field )
          : TDSField( results, field ) {}

        void get( uint64_t &val ) const { val = getU64( ); }

        void get( std::string &val ) const {
          std::stringstream ss;
          ss << getI64( );
          val = ss.str( );
        }
      };

      struct TDSFloat4Field : TDSField {

        TDSFloat4Field( TDSResultSet *results, size_t field )
          : TDSField( results, field ) {}

        void get( float &val ) const { val = getFloat( ); }

        void get( long double &val ) const { val = getFloat( ); }

        void get( std::string &val ) const {
          std::stringstream ss;
          ss << getFloat( );
          val = ss.str( );
        }
      };

      struct TDSFloat8Field : TDSField {

        TDSFloat8Field( TDSResultSet *results, size_t field )
          : TDSField( results, field ) {}

        void get( double &val ) const { val = getDouble( ); }

        void get( long double &val ) const { val = getDouble( ); }

        void get( std::string &val ) const {
          std::stringstream ss;
          ss << getDouble( );
          val = ss.str( );
        }
      };

      struct TDSTimestampField : TDSField {

        TDSTimestampField( TDSResultSet *results, size_t field )
          : TDSField( results, field ) {}

        void get( DBTime &val ) const { val = getTime( ); }

        void get( std::string &val ) const {
          std::stringstream ss;
          struct tm         tm     = {0};
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

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

      struct TDSResultSet : public dbcpp::DBResultSet, public std::enable_shared_from_this< TDSResultSet > {
        std::vector< std::unique_ptr< TDSField > > columns;
        std::shared_ptr< TDSStatement >            stmt;
        size_t                                     current;

        TDSResultSet( std::shared_ptr< TDSStatement > _stmt )
          : stmt( _stmt )
          , current( -1 ) {

          columns.reserve( stmt->fields );

          for ( size_t field = 0; field < stmt->fields; ++field ) {
            switch ( PQftype( stmt->result, field ) ) {
              case BYTEAOID: {
                columns.push_back( std::unique_ptr< TDSField >( new TDSByteAField( this, field ) ) );
                break;
              };

              case CSTRINGOID:
              case VARCHAROID: {
                columns.push_back( std::unique_ptr< TDSField >( new TDSVarcharField( this, field ) ) );
                break;
              }

              case INT2OID: {
                columns.push_back( std::unique_ptr< TDSField >( new TDSInt2Field( this, field ) ) );
                break;
              }
              case INT4OID: {
                columns.push_back( std::unique_ptr< TDSField >( new TDSInt4Field( this, field ) ) );
                break;
              }
              case INT8OID: {
                columns.push_back( std::unique_ptr< TDSField >( new TDSInt8Field( this, field ) ) );
                break;
              }
              case FLOAT4OID: {
                columns.push_back( std::unique_ptr< TDSField >( new TDSFloat4Field( this, field ) ) );
                break;
              }
              case FLOAT8OID: {
                columns.push_back( std::unique_ptr< TDSField >( new TDSFloat8Field( this, field ) ) );
                break;
              }
              case TIMESTAMPOID:
              case TIMESTAMPTZOID: {
                columns.push_back( std::unique_ptr< TDSField >( new TDSTimestampField( this, field ) ) );
                break;
              }
              default: {
                columns.push_back( std::unique_ptr< TDSField >( new TDSField( this, field ) ) );
                break;
              }
            }
          }
        }

        std::string fieldName( size_t field ) const { return stmt->columnNames[ field ]; }

        DBStatement statement( ) const { return stmt; }

        std::vector< std::string > fieldNames( ) const { return stmt->columnNames; }

        size_t fields( ) const { return stmt->fields; }

        size_t rows( ) const { return stmt->rows; }

        size_t row( ) const { return current; }

        bool next( ) { return ++current < rows( ); }

        DBField get( size_t field ) const {
          if ( field >= columns.size( ) ) {
            throw DBException( "Field index is out of range" );
          }
          return std::shared_ptr< dbcpp::DBField >( shared_from_this( ), columns[ field ].get( ) );
        }

        DBField get( std::string name ) const {
          std::transform( name.begin( ), name.end( ), name.begin( ), ::toupper );
          auto iterator = std::find( stmt->columnNames.begin( ), stmt->columnNames.end( ), name );

          if ( iterator == stmt->columnNames.end( ) ) {
            throw DBException( std::string( "Unknown field named: " ) + name );
          }

          return get( iterator - stmt->columnNames.begin( ) );
        }
      };

      /* - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - */

      FreeTDSDriver( ) {
        auto func = std::bind( &FreeTDSDriver::createConnection, this, std::placeholders::_1 );
        Driver::registerType( "tds", func );
      }

      DBConnection createConnection( Uri *uri ) { return std::make_shared< TDSConnection >( uri ); }

      ~FreeTDSDriver( ) {}
    };

    FreeTDSDriver FreeTDSDriver::registrar;
  } // namespace tds
} // namespace dbcpp
