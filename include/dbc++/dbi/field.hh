#ifndef __DBCPP_DBI_FIELD_HH__
#define __DBCPP_DBI_FIELD_HH__

#include "../internal/base_types.hh"
#include "../internal/utils.hh"
#include <sstream>
#include <cstdlib>

namespace dbcpp {
  namespace interface {
    /** Result field / column */
    struct Field {
      /**
       * @brief Get the name of the field
       * @return field name
       */
      virtual std::string name( ) const = 0;

      /**
       * @brief Get the field type
       * @return field type
       */
      virtual FieldType type( ) const = 0;

      /**
       * @brief Identify if the field is null
       * @return true if null, false if not
       */
      virtual bool isNull( ) const = 0;

      /**
       * @brief Get the value of the field as a timestamp
       * @param value database time variable
       */
      virtual void get( DBTime &value ) const { value = DBTime( std::chrono::seconds( 0 ) ); }

      /**
       * @brief Get the value of the field as a string
       * @param value string variable
       */
      virtual void get( std::string &value ) const { value = ""; }

      /**
       * @brief Get the value of the field as a binary blob
       * @param value blob variable
       */
      virtual void get( VarByte &value ) const {
        std::string str;
        get( str );
        value.insert( value.end( ), str.begin( ), str.end( ) );
      }

      /**
       * @brief Get the value of a field as a boolean
       * @param value boolean variable
       */
      virtual void get( bool &value ) const {
        std::string str;
        get( str );
        value = dbcpp::internal::utils::stob( str );
      }

      /**
       * @brief Get the value of the field as a signed 8 bit integer
       * @param value 8 bit variable
       */
      virtual void get( int8_t &value ) const {
        uint8_t tmp = 0;
        get( tmp );
        value = tmp;
      }

      /**
       * @brief Get the value of the field as an unsigned 8 bit integer
       * @param value 8 bit variable
       */
      virtual void get( uint8_t &value ) const {
        uint16_t tmp = 0;
        get( tmp );
        value = tmp;
      }

      /**
       * @brief Get the value of the field as an signed 16 bit integer
       * @param value 16 bit variable
       */
      virtual void get( int16_t &value ) const {
        uint16_t tmp = 0;
        get( tmp );
        value = tmp;
      }

      /**
       * @brief Get the value of the field as an unsigned 16 bit integer
       * @param value 16 bit variable
       */
      virtual void get( uint16_t &value ) const {
        uint32_t tmp = 0;
        get( tmp );
        value = tmp;
      }

      /**
       * @brief Get the value of the field as an signed 32 bit integer
       * @param value 32 bit variable
       */
      virtual void get( int32_t &value ) const {
        uint32_t tmp = 0;
        get( tmp );
        value = tmp;
      }

      /**
       * @brief Get the value of the field as an unsigned 32 bit integer
       * @param value 32 bit variable
       */
      virtual void get( uint32_t &value ) const {
        uint64_t tmp = 0;
        get( tmp );
        value = tmp;
      }

      /**
       * @brief Get the value of the field as an signed 64 bit integer
       * @param value 64 bit variable
       */
      virtual void get( int64_t &value ) const {
        uint64_t tmp = 0;
        get( tmp );
        value = tmp;
      }

      /**
       * @brief Get the value of the field as an unsigned 64 bit integer
       * @param value 64 bit variable
       */
      virtual void get( uint64_t &value ) const {
        std::string str;
        get( str );
        value = std::strtoll( str.c_str( ), 0x00, 0 );
      }

      /**
       * @brief Get the value of the field as a single floating point
       * @param value floating point variable
       */
      virtual void get( float &value ) const {
        double tmp = 0.0;
        get( tmp );
        value = tmp;
      }

      /**
       * @brief Get the value of the field as a double floating point
       * @param value floating point variable
       */
      virtual void get( double &value ) const {
        long double tmp = 0.0;
        get( tmp );
        value = tmp;
      }

      /**
       * @brief Get the value of the field as a long double floating point
       * @param value floating point variable
       */
      virtual void get( long double &value ) const {
        std::string str;
        get( str );
        value = std::strtold( str.c_str( ), 0x00 );
      }
    };
  } // namespace interface
} // namespace dbcpp

#endif
