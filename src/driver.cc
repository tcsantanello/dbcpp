
#include "dbc++/dbcpp.hh"
#include <boost/dll/shared_library.hpp>
#include <map>
#include <mutex>

#include <spdlog/spdlog.h>

#include <spdlog/sinks/null_sink.h>

namespace dbcpp {
  /**
   * @brief Create and register a logger with the library's spdlog
   * @param logger spdlog logger to register in the library
   */
  void register_logger( std::shared_ptr< spdlog::logger > logger ) { spdlog::register_logger( logger ); }

  /**
   * @brief Create and register a logger with the library's spdlog
   * @param name logger name
   * @param sinks logger targets/sinks
   */
  std::shared_ptr< spdlog::logger > create_logger( std::string name, std::vector< spdlog::sink_ptr > sinks ) {
    std::shared_ptr< spdlog::logger > logger = spdlog::get( name );

    if ( !logger ) {
      if ( sinks.empty( ) ) {
        sinks.emplace_back( std::make_shared< spdlog::sinks::null_sink_mt >( ) );
      }

      logger = std::make_shared< spdlog::logger >( name, sinks.begin( ), sinks.end( ) );

      spdlog::register_logger( logger );
    } else {
      auto &lsinks = logger->sinks( );

      std::copy( sinks.begin( ), sinks.end( ), std::back_inserter( lsinks ) );
      std::sort( lsinks.begin( ), lsinks.end( ) );

      lsinks.erase( std::unique( lsinks.begin( ), lsinks.end( ) ), lsinks.end( ) );
    }

    return logger;
  }

#define LOG( logger, lvl, ... )                                                                                        \
  do {                                                                                                                 \
    if ( logger->should_log( spdlog::level::lvl ) ) {                                                                  \
      SPDLOG_LOGGER_CALL( logger, spdlog::level::lvl, __VA_ARGS__ );                                                   \
    }                                                                                                                  \
  } while ( 0 )

  namespace Driver {
    struct DriverInfo {
      boost::dll::shared_library library;
      Base *                     driver;
    };

    /** Database driver map lock */
    std::mutex driverLock;
    /** Database drivers by schema name */
    std::map< std::string, DriverInfo > drivers;
    /** Driver logger */
    std::shared_ptr< spdlog::logger > logger = create_logger( "dbcpp::Driver", { } );

    /**
     * @brief Get the driver by uri schema
     * @param uri connection uri
     * @return driver
     */
    Base *getDriver( Uri const *uri ) {
      auto entry = drivers.find( uri->scheme( ) );

      if ( entry == drivers.end( ) ) {
        std::lock_guard< std::mutex > guard( driverLock );
        boost::dll::shared_library    library(
          uri->scheme( ), boost::dll::load_mode::append_decorations | boost::dll::load_mode::search_system_folders );

        LOG( logger, trace, "Loaded library for '{}'", uri->scheme( ) );

        if ( library.has( "getDriver" ) == true ) {
          Base *driver = library.get< Base *( ) >( "getDriver" )( );

          drivers[ uri->scheme( ) ] = DriverInfo{ std::move( library ), driver };

          LOG( logger, trace, "Cached driver for {}", uri->scheme( ) );

          return driver;
        }

        LOG( logger, trace, "Supporting driver for '{}' not found", uri->scheme( ) );
        throw std::runtime_error( "Supporting database type not found" );
      } else {
        LOG( logger, trace, "Database driver {} found in cache", uri->scheme( ) );
      }

      return entry->second.driver;
    }

    /**
     * @brief Create a database connection
     * @param uri connection creation uri
     * @return database connection
     */
    static std::shared_ptr< interface::Connection > createConnection( Uri *const uri ) {
      std::shared_ptr< interface::Connection > rawCxn;
      auto                                     driver = getDriver( uri );

      LOG( logger, trace, "Creating database connection for type '{}'", uri->scheme( ) );
      if ( !( rawCxn = driver->createConnection( uri ) ) ) {
        LOG( logger, trace, "Database connection creation failed for '{}'", uri->scheme( ) );
        throw std::runtime_error( "Connection create failed" );
      }

      if ( !rawCxn->connect( ) ) {
        LOG( logger, trace, "Database connection failed to connect '{}'", uri->scheme( ) );
        throw std::runtime_error( "Unable to connect" );
      }

      return rawCxn;
    }

    /**
     * @brief Connect to a database by uri string
     * @param uri resource identifier string
     * @return database connection
     */
    Connection connect( const std::string &uri ) {
      std::shared_ptr< Uri > _uri( Uri::parse( uri ) );

      return connect( _uri.get( ) );
    }

    /**
     * @brief Connect to a database by uri
     * @param uri resource identifier
     * @return database connection
     */
    Connection connect( Uri *uri ) { return Connection( createConnection( uri ) ); }
  } // namespace Driver

  /***************************************************************************************************
   * Special case for pools that require the above namespace methods
   ***********/

  /**
   * @brief Establish a pool connection
   * @return established connection
   */
  internal::Pool::connection_t internal::Pool::connect( ) {
    auto driver = Driver::getDriver( uri.get( ) );
    auto cxn    = driver->createConnection( uri.get( ) );

    if ( cxn == nullptr ) {
      throw std::runtime_error( "Connection create failed" );
    }

    cxn->setAutoCommit( autoCommit );

    if ( !cxn->connect( ) ) {
      throw std::runtime_error( "Unable to connect" );
    }

    return cxn;
  }
} // namespace dbcpp
