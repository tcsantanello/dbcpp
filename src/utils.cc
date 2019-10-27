#include "dbc++/internal/utils.hh"
#include <ios>
#include <sstream>

namespace dbcpp {
  namespace internal {
    namespace utils {
      bool stob( const std::string value ) {
        std::istringstream iss( value );
        bool               rc = false;

        iss >> rc;

        if ( !iss.fail( ) ) {
          return rc;
        }

        iss.clear( );
        iss >> std::boolalpha >> rc;

        if ( !iss.fail( ) ) {
          return rc;
        }

        throw std::invalid_argument( value + "is not a valid boolean" );
      }
    } // namespace utils
  }   // namespace internal
} // namespace dbcpp
