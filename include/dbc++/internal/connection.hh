#ifndef __DBCPP_INTERNAL_CONNECTION_HH__
#define __DBCPP_INTERNAL_CONNECTION_HH__

#include "../dbi/connection.hh"
#include "statement.hh"
#include <algorithm>
#include <functional>
#include <string>

namespace dbcpp {
  namespace internal {
    class Connection {
     public:
      using connection_t   = interface::Connection;
      using shared_cxn     = std::shared_ptr< connection_t >;
      using pool_release_f = std::function< void( shared_cxn ) >;

      ~Connection( ) {
        if ( poolRelease != nullptr ) {
          poolRelease( connection );
        }
      }

      Connection( shared_cxn _connection, pool_release_f release = nullptr )
        : connection( std::move( _connection ) )
        , poolRelease( release ) {}

      bool      connect( ) { return connection->connect( ); }
      bool      disconnect( ) { return connection->disconnect( ); }
      bool      reconnect( ) { return connection->reconnect( ); }
      bool      test( ) { return connection->test( ); }
      void      commit( ) { connection->commit( ); }
      void      rollback( ) { connection->rollback( ); }
      void      setAutoCommit( bool ac = true ) { return connection->setAutoCommit( ac ); }
      Statement createStatement( std::string string ) const {
        auto notspace = []( const char &val ) -> bool { return !isspace( val ); };
        string.erase( string.begin( ), std::find_if( string.begin( ), string.end( ), notspace ) );
        string.erase( std::find_if( string.rbegin( ), string.rend( ), notspace ).base( ), string.end( ) );
        return connection->createStatement( std::move( string ) );
      }
      Statement operator<<( const std::string &string ) const { return createStatement( string ); }

     private:
      shared_cxn     connection;
      pool_release_f poolRelease;
    };
  } // namespace internal
} // namespace dbcpp

#endif
