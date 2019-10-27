#ifndef __DBCPP_INTERNAL_POOL_HH__
#define __DBCPP_INTERNAL_POOL_HH__

#include "connection.hh"
#include <algorithm>
#include <mutex>
#include <queue>
#include <thread>

namespace dbcpp {
  namespace internal {
    /** Database connection pool */
    class Pool {
      using connection_t = std::shared_ptr< interface::Connection >;

      connection_t connect( );

      static void addIndex( size_t index, std::queue< size_t > &queue, std::mutex &lock ) {
        std::lock_guard< std::mutex > guard( lock );
        queue.push( index );
      }

      static bool getNextIndex( size_t &index, std::queue< size_t > &queue, std::mutex &lock ) {
        std::lock_guard< std::mutex > guard( lock );

        if ( queue.empty( ) ) {
          return false;
        }

        index = queue.front( );
        queue.pop( );

        return true;
      }

      void addConnectionIndex( size_t index ) { addIndex( index, queue, queueLock ); }
      void addReconnectIndex( size_t index ) { addIndex( index, reconnect, reconnectLock ); }

      void addConnection( connection_t connection ) {
        auto found = std::find( connections.begin( ), connections.end( ), connection );

        if ( found != connections.end( ) ) {
          addConnectionIndex( static_cast< size_t >( connections.end( ) - found - 1 ) );
        }
      }

      void addReconnect( connection_t connection ) {
        auto found = std::find( connections.begin( ), connections.end( ), connection );

        if ( found != connections.end( ) ) {
          addReconnectIndex( static_cast< size_t >( connections.end( ) - found - 1 ) );
        }
      }

      connection_t getNextConnection( std::queue< size_t > &queue, std::mutex &lock ) {
        size_t index = 0;

        if ( !getNextIndex( index, queue, lock ) ) {
          return nullptr;
        }

        return connections[ index ];
      }

     protected:
      void add( connection_t connection ) { addConnection( connection ); }

     public:
      Pool( const std::string &             uri,
            size_t                          count       = 10,
            bool                            _autoCommit = false,
            std::chrono::duration< double > checkPeriod = std::chrono::minutes( 5 ) );

      Pool( Uri *                           uri,
            size_t                          count       = 10,
            bool                            autoCommit  = false,
            std::chrono::duration< double > checkPeriod = std::chrono::minutes( 5 ) )
        : Pool( *uri, count, autoCommit, checkPeriod ) {}

      ~Pool( ) {
        asyncTestRunning = false;
        asyncTest.join( );
      }

      /**
       * @brief Sets the automatic commit flag for the pool connections
       * @param ac true for automatic commit, false to disable
       */
      void setAutoCommit( bool ac = true ) { autoCommit = ac; };

      /**
       * @brief Get a connection from the pool.
       *
       * If the pool is empty create a new connection, otherwise wait for one
       * to become available
       * @return valid database connection
       * @throws DBException if no connection can be created
       */
      Connection getConnection( ) noexcept( false ) {
        if ( queue.empty( ) ) {
          auto cxn = connect( );

          if ( !cxn->test( ) ) {
            throw DBException( "Unable to connect to the database" );
          }
          return cxn;
        }

        do {
          if ( auto ptr = getNextConnection( queue, queueLock ) ) {
            if ( ptr->test( ) ) {
              ptr->setAutoCommit( autoCommit );
              return Connection( ptr, [this]( connection_t cxn ) { addConnection( cxn ); } );
            } else {
              addReconnect( ptr );
            }
          }
        } while ( true );
      }

     private:
      std::vector< connection_t > connections;
      std::queue< size_t >        queue;
      std::mutex                  queueLock;
      std::queue< size_t >        reconnect;
      std::mutex                  reconnectLock;
      std::unique_ptr< Uri >      uri;
      std::thread                 asyncTest;
      bool                        autoCommit;
      bool                        asyncTestRunning;
    };
  } // namespace internal
} // namespace dbcpp

#endif
