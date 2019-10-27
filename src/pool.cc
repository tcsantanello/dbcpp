
#include "dbc++/dbcpp.hh"
#include <algorithm>
#include <spdlog/spdlog.h>

#include <spdlog/sinks/null_sink.h>

namespace dbcpp {
  /** Driver logger */
  std::shared_ptr< spdlog::logger > logger = create_logger( "dbcpp::Pool", { } );

#define LOG( logger, lvl, ... )                                                                                        \
  do {                                                                                                                 \
    if ( logger->should_log( spdlog::level::lvl ) ) {                                                                  \
      SPDLOG_LOGGER_CALL( logger, spdlog::level::lvl, __VA_ARGS__ );                                                   \
    }                                                                                                                  \
  } while ( 0 )

  Pool::Pool( const std::string &_uri, size_t count, bool _autoCommit, std::chrono::duration< double > checkPeriod )
    : uri( Uri::parse( _uri ) )
    , autoCommit( _autoCommit ) {
    auto endpoint = fmt::format( "{}://{}:{}/{}", uri->scheme( ), uri->host( ), uri->port( ), uri->resource( ) );

    LOG( logger, info, "Creating connection pool of {} for {}", count, endpoint );

    count = std::max( count, ( size_t ) 1 );
    for ( size_t num = 0; num < count; ++num ) {
      connections.push_back( connect( ) );
      queue.push( connections.size( ) - 1 );
    }

    LOG( logger, info, "Connection pool of {} for {} completed", count, endpoint );

    LOG( logger,
         info,
         "Initializing pool monitor thread for {} connection{} for {} every {:.3f}s", //
         count,
         count > 1 ? "s" : "",
         endpoint,
         checkPeriod.count( ) );

    asyncTest = std::thread( [ this, checkPeriod, endpoint ]( ) -> void {
      const auto SLEEP = std::chrono::microseconds( 125 );
      auto       slept = std::chrono::microseconds( 0 );

      for ( asyncTestRunning = true; asyncTestRunning; ) {
        auto sleepFor = SLEEP;

        /* Every check period, poll all the connections */
        if ( slept >= checkPeriod ) {
          LOG( logger,
               debug,
               "Check period of {:.3f}s expired for {}, checking connections", //
               checkPeriod.count( ),
               endpoint );

          if ( !queue.empty( ) ) {
            for ( size_t size = queue.size( ); size > 0; --size ) {
              try {
                getConnection( );
              } catch ( ... ) {
              }
            }

            slept = std::chrono::microseconds( 0 );
          }
        }

        /* Rebuild failed connections */
        if ( auto connection = getNextConnection( reconnect, reconnectLock ) ) {
          LOG( logger, debug, "Initiating reconnection for pool resource of {}", endpoint );

          auto start    = DBClock::now( );
          auto rc       = connection->reconnect( );
          auto end      = DBClock::now( );
          auto duration = std::chrono::duration_cast< std::chrono::microseconds >( end - start );

          if ( rc ) { // It worked! Start using it
            LOG( logger, debug, "Reconnection for pool resource of {}, successful", endpoint );

            addConnection( connection );
          } else { // Uhoh, schedule another attempt
            LOG( logger, debug, "Reconnection for pool resource of {}, failed", endpoint );

            addReconnect( connection );
          }

          // Account for our time...
          slept += duration;
          sleepFor -= duration;

          if ( sleepFor <= std::chrono::microseconds( 0 ) ) {
            continue;
          }
        }

        // Time for a nap till our next "shift"
        std::this_thread::sleep_for( sleepFor );
        slept += sleepFor;
      }
    } );
  }

  /* Pool::connection_t Pool::connect( ) found in driver.cc */
} // namespace dbcpp
