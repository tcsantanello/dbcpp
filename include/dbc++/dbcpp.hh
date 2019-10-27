#ifndef __DBCPP_HH__
#define __DBCPP_HH__

#include <memory>
#include <spdlog/spdlog.h>
#include <uri/uri.hh>

// clang-format off
#include <dbc++/dbi/connection.hh>
#include <dbc++/dbi/field.hh>
#include <dbc++/dbi/resultset.hh>
#include <dbc++/dbi/statement.hh>

#include <dbc++/internal/base_types.hh>
#include <dbc++/internal/pool.hh>
#include <dbc++/internal/connection.hh>
#include <dbc++/internal/field.hh>
#include <dbc++/internal/resultset.hh>
#include <dbc++/internal/statement.hh>
// clang-format on

namespace dbcpp {
  using namespace internal;

  void                              register_logger( std::shared_ptr< spdlog::logger > logger );
  std::shared_ptr< spdlog::logger > create_logger( std::string name, std::vector< spdlog::sink_ptr > sinks );

  namespace Driver {
    /**
     * @brief Base driver definition
     */
    struct Base {
      virtual std::shared_ptr< interface::Connection > createConnection( Uri * ) = 0;
      virtual ~Base( ) {}
    };

    typedef std::function< std::shared_ptr< interface::Connection >( Uri * ) > CreateConnection;

    Connection connect( const std::string & ) noexcept( false );
    Connection connect( Uri * ) noexcept( false );
  } // namespace Driver
} // namespace dbcpp

#endif
