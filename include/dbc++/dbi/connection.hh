#ifndef __DBCPP_DBI_CONNECTION_HH__
#define __DBCPP_DBI_CONNECTION_HH__

#include "statement.hh"
#include <memory>

namespace dbcpp {
  namespace interface {
    /** Database connection interface */
    struct Connection {

      /**
       * @brief Establish the database connection
       * @return true on success, false on failure
       */
      virtual bool connect( ) = 0;

      /**
       * @brief Disconnect from the database
       * @return true on success, false on failure
       */
      virtual bool disconnect( ) = 0;

      /**
       * @brief Reconnect (disconnect, followed by a connect) the database connection
       * @return true on success, false on failure
       */
      virtual bool reconnect( ) = 0;

      /**
       * @brief Test the viability of the connection
       * @return true if good, false if bad
       */
      virtual bool test( ) = 0;

      /**
       * @brief Enable or disable the automatic commits
       * @param ac auto commit flag
       */
      virtual void setAutoCommit( bool ac ) = 0;

      /**
       * @brief Commit the current transaction
       * @note Throws DBException
       */
      virtual void commit( ) = 0;

      /**
       * @brief Rollback the current transaction
       * @note Throws DBException
       */
      virtual void rollback( ) = 0;

      /**
       * @brief Create a prepared statement
       * @param query query string
       * @return prepared statement
       */
      virtual std::shared_ptr< Statement > createStatement( std::string query ) = 0;
    };
  } // namespace interface
} // namespace dbcpp

#endif
