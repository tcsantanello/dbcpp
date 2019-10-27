#ifndef __DBCPP_DBI_RESULTSET_HH__
#define __DBCPP_DBI_RESULTSET_HH__

#include "field.hh"
#include <memory>

namespace dbcpp {
  namespace interface {
    /** Query Result Set interface */
    struct ResultSet {
      /**
       * @brief Get the query result column names
       * @return field names
       */
      virtual std::vector< std::string > fieldNames( ) const = 0;

      /**
       * @brief Get the number of fields in the result set
       * @return field count
       */
      virtual size_t fields( ) const = 0;

      /**
       * @brief Get the number of rows in the result set
       * @return row count
       */
      virtual size_t rows( ) const = 0;

      /**
       * @brief Get the current row number
       * @return row number
       */
      virtual size_t row( ) const = 0;

      /**
       * @brief Advance to the next row
       * @return true on successful advance, false if no more rows
       */
      virtual bool next( ) = 0;

      /**
       * @brief Get a result field by column number
       * @param field column number
       * @return result field
       */
      virtual std::shared_ptr< Field > get( size_t field ) const = 0;

      /**
       * @brief Get a result field by column name
       * @param field column name
       * @return result field
       */
      virtual std::shared_ptr< Field > get( std::string field ) const = 0;
    };

  } // namespace interface
} // namespace dbcpp

#endif
