#ifndef __DBCPP_INTERNAL_FIELD_HH__
#define __DBCPP_INTERNAL_FIELD_HH__

#include "../dbi/field.hh"
#include <memory>

namespace dbcpp {
  namespace internal {
    /** Query Result Field */
    class Field {
     public:
      using Type    = FieldType;
      using field_t = interface::Field;

      explicit Field( std::shared_ptr< field_t > _field )
        : field( std::move( _field ) ) {}

      /**
       * @brief Get the result field/column name
       * @return field name
       */
      std::string name( ) const { return field->name( ); }

      /**
       * @brief Get the type of the field
       * @return field type
       */
      Type type( ) const { return field->type( ); }

      /**
       * @brief Identify if the field is null
       * @return true if null, false if not
       */
      bool isNull( ) const { return field->isNull( ); }

      /**
       * @brief Get the field value
       * @return value
       */
      template < typename T >
      T get( ) const {
        T val;
        field->get( val );
        return val;
      }

      /**
       * @brief Store the value of the field in the target variable
       * @param v target variable
       * @return this
       */
      template < typename T >
      const Field &operator>>( T &v ) const {
        v = get< T >( );
        return *this;
      }

      /**
       * @brief Check if two fields are identical
       * @param rhs right hand field
       * @return true if equal, false if not
       */
      bool operator==( const Field &rhs ) const { return field == rhs.field; }

     private:
      std::shared_ptr< field_t > field; /**< Driver Implementation pointer */
    };
  } // namespace internal
} // namespace dbcpp

#endif
