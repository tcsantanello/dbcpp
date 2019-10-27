#ifndef __DBCPP_INTERNAL_STATEMENT_HH__
#define __DBCPP_INTERNAL_STATEMENT_HH__

#include "../dbi/statement.hh"
#include "resultset.hh"

namespace dbcpp {
  namespace internal {
    /** SQL statement */
    class Statement {
     public:
      using statement_t = interface::Statement;

      Statement( )
        : Statement( nullptr ) {}
      Statement( std::shared_ptr< statement_t > _statement )
        : nextParam( 0 )
        , statement( std::move( _statement ) )
        , reset( false ) {}

      ResultSet getResults( ) { return ResultSet{ statement->getResults( ) }; }
      ResultSet executeQuery( ) {
        execute( );
        return getResults( );
      }

      int executeUpdate( ) {
        int updated = statement->executeUpdate( );
        reset       = true;
        nextParam   = 0;
        return updated;
      }

      void execute( ) {
        statement->execute( );
        reset     = true;
        nextParam = 0;
      }

      /**
       * @brief Set the parameter as null
       * @param parameter parameter index (0 start)
       * @param type parameter type
       * @return true on success, false on failure
       */
      bool setParamNull( size_t parameter, Field::Type type ) {
        if ( reset ) {
          statement->reset( );
          reset = false;
        }

        return statement->setParamNull( parameter, type );
      }

      template < typename T >
      bool setParam( size_t parameter, T *value ) {
        if ( reset ) {
          statement->reset( );
          reset = false;
        }

        if ( !value ) {
          return setParamNull( parameter, FieldTypeDecoder::type< T >::value );
        }
        return setParam( parameter, *value );
      }

      bool setParam( size_t parameter, const char *value ) {
        if ( reset ) {
          statement->reset( );
          reset = false;
        }

        if ( !value ) {
          return setParamNull( parameter, FieldTypeDecoder::type< std::string >::value );
        }

        return statement->setParam( parameter, std::string{ value } );
      }

      template < typename T, typename std::enable_if< std::is_same< bool, T >::value >::type * = nullptr >
      bool setParam( size_t parameter, T value ) {
        if ( reset ) {
          statement->reset( );
          reset = false;
        }

        return statement->setParam( parameter, dbcpp::interface::safebool{ value } );
      }

      template < typename T, typename std::enable_if< !std::is_same< bool, T >::value >::type * = nullptr >
      bool setParam( size_t parameter, T value ) {
        if ( reset ) {
          statement->reset( );
          reset = false;
        }

        return statement->setParam( parameter, value );
      }

      Statement &operator<<( decltype( nullptr ) ) {
        setParamNull( nextParam++, FieldTypeDecoder::type< decltype( nullptr ) >::value );
        return *this;
      }

      template < typename T >
      Statement &operator<<( T value ) {
        setParam( nextParam++, value );
        return *this;
      }

     private:
      size_t                         nextParam;
      std::shared_ptr< statement_t > statement;
      bool                           reset;
    };
  } // namespace internal
} // namespace dbcpp

#endif
