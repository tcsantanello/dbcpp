#ifndef __DBCPP_INTERNAL_RESULT_SET_HH__
#define __DBCPP_INTERNAL_RESULT_SET_HH__

#include "../dbi/resultset.hh"
#include "field.hh"
#include <memory>

namespace dbcpp {
  namespace internal {
    /** Query Result Set */
    class ResultSet {
     public:
      /**
       * Result set field iterator
       */
      class forward_iterator {
        /**
         * @brief Compare two field iterators
         * @param rhs right hand side
         * @return true if equal, false if not
         */
        bool equals( forward_iterator &rhs ) const { return ( rs == rhs.rs ) && ( field == rhs.field ); }

        /**
         * @brief Advance to the next column in the result set
         */
        void next( ) {
          if ( ++column < rs.fields( ) ) {
            field = rs.get( column );
          } else {
            field = Field( nullptr );
          }
        }

       public:
        forward_iterator( const ResultSet &_rs, size_t _field )
          : rs( _rs )
          , column( _field )
          , field( _field < _rs.fields( ) ? _rs.get( _field ) : Field( nullptr ) ) {}

        /**
         * @brief Advance to the next column
         * @return this
         */
        forward_iterator &operator++( ) {
          next( );
          return *this;
        }

        /**
         * @brief Advance to the next column
         * @return forward_iterator representing the previous state
         */
        forward_iterator operator++( int ) {
          auto tmp = *this;
          next( );
          return tmp;
        }

        /**
         * @brief Get the current field
         * @return current field
         */
        Field const &operator*( ) const { return field; }

        /**
         * @brief Access the current field
         * @return current field
         */
        const Field *operator->( ) const { return &field; }

        /**
         * @brief Check if two forward_iterators are not identical
         * @param rhs right hand forward_iterator
         * @return false if equal, true if not
         */
        bool operator!=( forward_iterator &rhs ) const { return !equals( rhs ); }

        /**
         * @brief Check if two forward_iterators are identical
         * @param rhs right hand forward_iterator
         * @return true if equal, false if not
         */
        bool operator==( forward_iterator &rhs ) const { return equals( rhs ); }

       private:
        const ResultSet &rs;     /**< Associated result set */
        size_t           column; /**< column index */
        Field            field;  /**< Current field */
      };

      using iterator     = forward_iterator;
      using result_set_t = interface::ResultSet;

      ResultSet( )
        : ResultSet( nullptr ) {}

      explicit ResultSet( std::shared_ptr< result_set_t > _results )
        : results( _results ) {}

      /**
       * @brief Get the column/field names
       * @return field names
       */
      std::vector< std::string > fieldNames( ) const { return results->fieldNames( ); }

      /**
       * @brief Get the number of result fields
       * @return field count
       */
      size_t fields( ) const { return results->fields( ); }

      /**
       * @brief Get the number of result rows
       * @return row count
       */
      size_t rows( ) const { return results->rows( ); }

      /**
       * @brief Get the current row number
       * @return current row number
       */
      size_t row( ) const { return results->row( ); }

      /**
       * @brief Advance to the next result row
       * @return true on success, false for no more rows
       */
      bool next( ) { return results->next( ); }

      /**
       * @brief Get a column value by column number
       * @param field column number  (zero start)
       * @return value
       */
      template < typename T >
      T get( size_t field ) const {
        return get( field ).get< T >( );
      }

      /**
       * @brief Get a column value by column name
       * @param field column name
       * @return value
       */
      template < typename T >
      T get( const std::string &field ) const {
        return get( field ).get< T >( );
      }

      /**
       * @brief Identify if a field is null
       * @param field field number (zero start)
       * @return true if null, false if not
       */
      bool isNull( size_t field ) const { return get( field ).isNull( ); }

      /**
       * @brief Get the type of the field
       * @param field field number (zero start)
       * @return field type
       */
      Field::Type type( size_t field ) const { return get( field ).type( ); }

      /**
       * @brief Get the type of the field
       * @param field field name
       * @return field type
       */
      Field::Type type( const std::string &field ) const { return get( field ).type( ); }

      /**
       * @brief Identify if a field is null
       * @param field field name
       * @return true if null, false if not
       */
      bool isNull( const std::string &field ) const { return get( field ).isNull( ); }

      /**
       * @brief Get a field column from the result set
       * @param field field number (zero start)
       * @return field
       */
      Field get( size_t field ) const { return Field( results->get( field ) ); }

      /**
       * @brief Get a field column from the result set
       * @param field field name
       * @return field
       */
      Field get( std::string field ) const { return Field( results->get( field ) ); }

      /**
       * @brief Get a field column from the result set
       * @param field field number (zero start)
       * @return field
       */
      Field operator[]( size_t field ) const { return get( field ); }

      /**
       * @brief Get a field column from the result set
       * @param field field name
       * @return field
       */
      Field operator[]( const std::string &field ) const { return get( field ); }

      /**
       * @brief Get a field iterator representing the start of the set
       * @return field iterator
       */
      forward_iterator begin( ) const { return forward_iterator( *this, 0 ); }

      /**
       * @brief Get a field iterator representing the end of the set
       * @return field iterator
       */
      forward_iterator end( ) const { return forward_iterator( *this, results->fields( ) ); }

      /**
       * @brief Check if two result sets are identical
       * @param rhs right hand result set
       * @return true if equal, false if not
       */
      bool operator==( const ResultSet &rhs ) const { return results == rhs.results; }

     private:
      std::shared_ptr< result_set_t > results; /**< Driver Implementation pointer */
    };
  } // namespace internal
} // namespace dbcpp

#endif
