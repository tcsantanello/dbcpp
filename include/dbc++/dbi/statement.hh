#ifndef __DBCPP_DBI_STATEMENT_HH__
#define __DBCPP_DBI_STATEMENT_HH__

#include "resultset.hh"

#include <memory>

namespace dbcpp {
  namespace interface {
    /**
     * Implicit bool protection wrapper
     */
    struct safebool {
      explicit safebool( bool v )
        : val( v ) {}
      bool val;
    };

    /** Prepared statement interface */
    struct Statement {
      /**
       * @brief Set the parameter as null
       * @param parameter parameter index (0 start)
       * @param type declared type for the null
       * @return true on success, false on failure
       */
      virtual bool setParamNull( size_t parameter, FieldType type ) = 0;

      /**
       * @brief Set the boolean parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, safebool value ) = 0;

      /**
       * @brief Set the unsigned 8 bit integer parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, uint8_t value ) = 0;

      /**
       * @brief Set the unsigned 16 bit integer parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, uint16_t value ) = 0;

      /**
       * @brief Set the unsigned 32 bit integer parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, uint32_t value ) = 0;

      /**
       * @brief Set the unsigned 64 bit integer parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, uint64_t value ) = 0;

      /**
       * @brief Set the signed 8 bit integer parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, int8_t value ) = 0;

      /**
       * @brief Set the signed 16 bit integer parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, int16_t value ) = 0;

      /**
       * @brief Set the signed 32 bit integer parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, int32_t value ) = 0;

      /**
       * @brief Set the signed 64 bit integer parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, int64_t value ) = 0;

      /**
       * @brief Set the single precision floating point parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, float value ) = 0;

      /**
       * @brief Set the double precision floating point parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, double value ) = 0;

      /**
       * @brief Set the double precision floating point parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, long double value ) { return setParam( parameter, ( double ) value ); }

      /**
       * @brief Set the string parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, std::string value ) = 0;

      /**
       * @brief Set the binary blob parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, VarByte value ) = 0;

      /**
       * @brief Set the date/time parameter value
       * @param parameter parameter index (0 start)
       * @param value parameter value
       * @return true on success, false on failure
       */
      virtual bool setParam( size_t parameter, DBTime value ) = 0;

      /**
       * @brief Reset the statement for reuse
       */
      virtual void reset( ) {}

      /**
       * @brief Execute an modification query (delete,insert,update)
       * @note Throws DBException
       * @return number of affected rows
       */
      virtual int executeUpdate( ) = 0;

      /**
       * @brief Execute the query
       * @note Throws DBException
       */
      virtual void execute( ) = 0;

      /**
       * @brief Get the result set of the executed query
       * @note Throws DBException
       * @return result set
       */
      virtual std::shared_ptr< ResultSet > getResults( ) = 0;
    };
  } // namespace interface
} // namespace dbcpp

#endif
