#ifndef __DBCPP_INTERNAL_BASE_TYPES_HH__
#define __DBCPP_INTERNAL_BASE_TYPES_HH__

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace dbcpp {
  /** Database clock type (time reference clock) */
  using DBClock = std::chrono::high_resolution_clock;
  /** Database time type */
  using DBTime = std::chrono::time_point< DBClock >;
  /** Variable Byte */
  using VarByte = std::vector< uint8_t >;

  /**
   * Database Exception
   */
  class DBException : public std::exception {
    const std::string message;

   public:
    explicit DBException( const std::string &msg )
      : message( msg ) {}

    const char *what( ) const noexcept override { return message.c_str( ); }
  };

  /** Database Types */
  enum FieldType {
    UNKNOWN = 0,        /**< Unspecified type */
                        /*
                         * Integral types
                         */
    TINYINT  = 1,       /**< 1 Byte integer: int8_t                  */
    SMALLINT = 2,       /**< 2 Byte integer: int16_t                 */
    INTEGER  = 3,       /**< 4 Byte integer: int32_t                 */
    BIGINT   = 4,       /**< 8 Byte integer: int64_t                 */
    DECIMAL  = 5,       /**< Variable precision and scale numeric    */
    NUMERIC  = DECIMAL, /**< DECIMAL type alias */
                        /*
                         * Floating point types
                         */
    FLOAT  = 6,         /**< Variable precision floating point       */
    DOUBLE = 7,         /**< Double precision floating point: double */
    REAL   = 8,         /**< Single precision floating point: double */
                        /*
                         * Bit array types
                         */
    BIT    = 9,         /**< Fixed length bit map/set                */
    VARBIT = 10,        /**< Variable length bit map/set             */
                        /*
                         * Binary array types
                         */
    BYTE    = 11,       /**< Fixed length binary data                */
    VARBYTE = 12,       /**< Variable length binary data             */
                        /*
                         * Character array types
                         */
    CHAR    = 13,       /**< Fixed length character string           */
    VARCHAR = 14,       /**< Variable length character string        */
                        /*
                         * Date/time types
                         */
    DATE      = 15,     /**< Date without time                       */
    TIME      = 16,     /**< Time of day ( time without date )       */
    TIMESTAMP = 17,     /**< Date and time                           */
                        /*
                         * Networking addresses
                         */
    INET_ADDRESS = 18,  /**< IP/Network address                      */
    MAC_ADDRESS  = 19,  /**< MAC address                             */
                        /*
                         * Large objects
                         */
    BLOB = 20,          /**< Binary large object                     */
    CLOB = 21,          /**< Character large object                  */
    TEXT = CLOB,        /**< CLOB alias                              */
                        /*
                         * Other
                         */
    ROWID   = 22,       /**< Unique row identifier                   */
    BOOLEAN = 23,       /**< Boolean [true/false]                    */
    JSON    = 24,       /**< Javascript Object Notation              */
    UUID    = 25,       /**< Universally Unique Identifier type      */
    XML     = 26,       /**< Stored XML type                         */
  };

  namespace FieldTypeDecoder {
    template < typename T >
    struct type {
      static const FieldType value = FieldType::UNKNOWN;
    };

#define FieldTypeDecodeEntry( _type, _dbtype )                                                                         \
  template <>                                                                                                          \
  struct type< _type > {                                                                                               \
    static const FieldType value = FieldType::_dbtype;                                                                 \
  }

    FieldTypeDecodeEntry( bool, BOOLEAN );
    FieldTypeDecodeEntry( uint8_t, BYTE );
    FieldTypeDecodeEntry( int8_t, CHAR );
    FieldTypeDecodeEntry( uint16_t, INTEGER );
    FieldTypeDecodeEntry( int16_t, SMALLINT );
    FieldTypeDecodeEntry( uint32_t, BIGINT );
    FieldTypeDecodeEntry( int32_t, INTEGER );
    FieldTypeDecodeEntry( uint64_t, BIGINT );
    FieldTypeDecodeEntry( int64_t, BIGINT );
    FieldTypeDecodeEntry( float, REAL );
    FieldTypeDecodeEntry( double, DOUBLE );
    FieldTypeDecodeEntry( std::string, VARCHAR );
    FieldTypeDecodeEntry( VarByte, VARBYTE );
    FieldTypeDecodeEntry( DBTime, TIMESTAMP );
#undef FieldTypeDecodeEntry

    template < typename T >
    constexpr FieldType get( T ) {
      return type< T >::value;
    }
  } // namespace FieldTypeDecoder
} // namespace dbcpp

#endif
