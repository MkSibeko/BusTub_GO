// Package types provides the type system for BusTub-Go.
//
// The type system defines:
// - TypeID: Enumeration of supported data types
// - Value: A typed database value
// - Type operations: Comparison, arithmetic, serialization
//
// ASSIGNMENT NOTES:
// ==================
// Understanding the type system is important for:
// - Expression evaluation in executors
// - Tuple serialization and deserialization
// - Index key comparison
//
// This mirrors: bustub/src/include/type/
package types

import (
	"fmt"
	"strconv"
)

// TypeID identifies the data type of a value.
type TypeID int

const (
	// TypeInvalid represents an invalid or uninitialized type.
	TypeInvalid TypeID = iota

	// TypeBoolean is a boolean (true/false) type.
	TypeBoolean

	// TypeTinyInt is an 8-bit signed integer.
	TypeTinyInt

	// TypeSmallInt is a 16-bit signed integer.
	TypeSmallInt

	// TypeInteger is a 32-bit signed integer.
	TypeInteger

	// TypeBigInt is a 64-bit signed integer.
	TypeBigInt

	// TypeDecimal is a fixed-point decimal number.
	TypeDecimal

	// TypeVarchar is a variable-length string.
	TypeVarchar

	// TypeTimestamp is a timestamp (date and time).
	TypeTimestamp
)

// String returns the name of the type.
func (t TypeID) String() string {
	switch t {
	case TypeBoolean:
		return "BOOLEAN"
	case TypeTinyInt:
		return "TINYINT"
	case TypeSmallInt:
		return "SMALLINT"
	case TypeInteger:
		return "INTEGER"
	case TypeBigInt:
		return "BIGINT"
	case TypeDecimal:
		return "DECIMAL"
	case TypeVarchar:
		return "VARCHAR"
	case TypeTimestamp:
		return "TIMESTAMP"
	default:
		return "INVALID"
	}
}

// Size returns the size in bytes for fixed-size types.
// Returns -1 for variable-length types (VARCHAR).
func (t TypeID) Size() int {
	switch t {
	case TypeBoolean:
		return 1
	case TypeTinyInt:
		return 1
	case TypeSmallInt:
		return 2
	case TypeInteger:
		return 4
	case TypeBigInt:
		return 8
	case TypeDecimal:
		return 8
	case TypeTimestamp:
		return 8
	case TypeVarchar:
		return -1 // Variable length
	default:
		return 0
	}
}

// IsNumeric returns true if the type is a numeric type.
func (t TypeID) IsNumeric() bool {
	switch t {
	case TypeTinyInt, TypeSmallInt, TypeInteger, TypeBigInt, TypeDecimal:
		return true
	default:
		return false
	}
}

// IsCoercibleTo returns true if this type can be coerced to the target type.
func (t TypeID) IsCoercibleTo(target TypeID) bool {
	if t == target {
		return true
	}
	// Numeric types can be coerced to each other
	if t.IsNumeric() && target.IsNumeric() {
		return true
	}
	return false
}

// =============================================================================
// VALUE REPRESENTATION
// =============================================================================

// Value represents a typed database value.
//
// A Value can hold any of the supported types.
// It uses a tagged union style representation.
type Value struct {
	// typeID identifies the type of this value.
	typeID TypeID

	// isNull indicates if this is a NULL value.
	isNull bool

	// Storage for the value (only one will be used based on typeID)
	boolVal   bool
	intVal    int64 // Used for all integer types
	floatVal  float64
	stringVal string
}

// NewBoolean creates a new boolean value.
func NewBoolean(val bool) Value {
	return Value{typeID: TypeBoolean, boolVal: val}
}

// NewInteger creates a new 32-bit integer value.
func NewInteger(val int32) Value {
	return Value{typeID: TypeInteger, intVal: int64(val)}
}

// NewBigInt creates a new 64-bit integer value.
func NewBigInt(val int64) Value {
	return Value{typeID: TypeBigInt, intVal: val}
}

// NewVarchar creates a new varchar value.
func NewVarchar(val string) Value {
	return Value{typeID: TypeVarchar, stringVal: val}
}

// NewDecimal creates a new decimal value.
func NewDecimal(val float64) Value {
	return Value{typeID: TypeDecimal, floatVal: val}
}

// NewNull creates a null value of the given type.
func NewNull(typeID TypeID) Value {
	return Value{typeID: typeID, isNull: true}
}

// GetTypeID returns the type of this value.
func (v Value) GetTypeID() TypeID {
	return v.typeID
}

// IsNull returns true if this is a NULL value.
func (v Value) IsNull() bool {
	return v.isNull
}

// GetBoolean returns the boolean value.
func (v Value) GetBoolean() bool {
	if v.typeID != TypeBoolean {
		panic("Value is not a boolean")
	}
	return v.boolVal
}

// GetInteger returns the integer value.
func (v Value) GetInteger() int32 {
	if v.typeID != TypeInteger {
		panic("Value is not an integer")
	}
	return int32(v.intVal)
}

// GetBigInt returns the bigint value.
func (v Value) GetBigInt() int64 {
	if v.typeID != TypeBigInt && v.typeID != TypeInteger {
		panic("Value is not an integer type")
	}
	return v.intVal
}

// GetVarchar returns the varchar value.
func (v Value) GetVarchar() string {
	if v.typeID != TypeVarchar {
		panic("Value is not a varchar")
	}
	return v.stringVal
}

// GetDecimal returns the decimal value.
func (v Value) GetDecimal() float64 {
	if v.typeID != TypeDecimal {
		panic("Value is not a decimal")
	}
	return v.floatVal
}

// String returns a string representation of the value.
func (v Value) String() string {
	if v.isNull {
		return "NULL"
	}
	switch v.typeID {
	case TypeBoolean:
		if v.boolVal {
			return "true"
		}
		return "false"
	case TypeTinyInt, TypeSmallInt, TypeInteger, TypeBigInt:
		return strconv.FormatInt(v.intVal, 10)
	case TypeDecimal:
		return strconv.FormatFloat(v.floatVal, 'f', -1, 64)
	case TypeVarchar:
		return v.stringVal
	default:
		return "INVALID"
	}
}

// =============================================================================
// VALUE COMPARISON
// =============================================================================

// CompareResult represents the result of a comparison.
type CompareResult int

const (
	CompareLessThan    CompareResult = -1
	CompareEqual       CompareResult = 0
	CompareGreaterThan CompareResult = 1
	CompareNull        CompareResult = 2 // Comparison with NULL
)

// CompareTo compares this value to another.
//
// Returns:
// - CompareLessThan if this < other
// - CompareEqual if this == other
// - CompareGreaterThan if this > other
// - CompareNull if either value is NULL
func (v Value) CompareTo(other Value) CompareResult {
	if v.isNull || other.isNull {
		return CompareNull
	}

	// Types must be comparable
	if v.typeID != other.typeID && !v.typeID.IsCoercibleTo(other.typeID) {
		panic(fmt.Sprintf("Cannot compare %s with %s", v.typeID, other.typeID))
	}

	switch v.typeID {
	case TypeBoolean:
		if v.boolVal == other.boolVal {
			return CompareEqual
		}
		if !v.boolVal && other.boolVal {
			return CompareLessThan
		}
		return CompareGreaterThan

	case TypeTinyInt, TypeSmallInt, TypeInteger, TypeBigInt:
		if v.intVal < other.intVal {
			return CompareLessThan
		}
		if v.intVal > other.intVal {
			return CompareGreaterThan
		}
		return CompareEqual

	case TypeDecimal:
		if v.floatVal < other.floatVal {
			return CompareLessThan
		}
		if v.floatVal > other.floatVal {
			return CompareGreaterThan
		}
		return CompareEqual

	case TypeVarchar:
		if v.stringVal < other.stringVal {
			return CompareLessThan
		}
		if v.stringVal > other.stringVal {
			return CompareGreaterThan
		}
		return CompareEqual

	default:
		panic(fmt.Sprintf("Comparison not supported for type %s", v.typeID))
	}
}

// Equals returns true if the values are equal.
func (v Value) Equals(other Value) bool {
	return v.CompareTo(other) == CompareEqual
}

// =============================================================================
// VALUE ARITHMETIC
// =============================================================================

// Add adds two values.
func (v Value) Add(other Value) Value {
	if v.isNull || other.isNull {
		return NewNull(v.typeID)
	}

	switch v.typeID {
	case TypeInteger:
		return NewInteger(int32(v.intVal + other.intVal))
	case TypeBigInt:
		return NewBigInt(v.intVal + other.intVal)
	case TypeDecimal:
		return NewDecimal(v.floatVal + other.floatVal)
	default:
		panic(fmt.Sprintf("Add not supported for type %s", v.typeID))
	}
}

// Subtract subtracts other from this value.
func (v Value) Subtract(other Value) Value {
	if v.isNull || other.isNull {
		return NewNull(v.typeID)
	}

	switch v.typeID {
	case TypeInteger:
		return NewInteger(int32(v.intVal - other.intVal))
	case TypeBigInt:
		return NewBigInt(v.intVal - other.intVal)
	case TypeDecimal:
		return NewDecimal(v.floatVal - other.floatVal)
	default:
		panic(fmt.Sprintf("Subtract not supported for type %s", v.typeID))
	}
}

// Multiply multiplies two values.
func (v Value) Multiply(other Value) Value {
	if v.isNull || other.isNull {
		return NewNull(v.typeID)
	}

	switch v.typeID {
	case TypeInteger:
		return NewInteger(int32(v.intVal * other.intVal))
	case TypeBigInt:
		return NewBigInt(v.intVal * other.intVal)
	case TypeDecimal:
		return NewDecimal(v.floatVal * other.floatVal)
	default:
		panic(fmt.Sprintf("Multiply not supported for type %s", v.typeID))
	}
}

// Divide divides this value by other.
func (v Value) Divide(other Value) Value {
	if v.isNull || other.isNull {
		return NewNull(v.typeID)
	}

	// Check for division by zero
	switch other.typeID {
	case TypeInteger, TypeBigInt:
		if other.intVal == 0 {
			panic("Division by zero")
		}
	case TypeDecimal:
		if other.floatVal == 0 {
			panic("Division by zero")
		}
	}

	switch v.typeID {
	case TypeInteger:
		return NewInteger(int32(v.intVal / other.intVal))
	case TypeBigInt:
		return NewBigInt(v.intVal / other.intVal)
	case TypeDecimal:
		return NewDecimal(v.floatVal / other.floatVal)
	default:
		panic(fmt.Sprintf("Divide not supported for type %s", v.typeID))
	}
}

// Min returns the minimum of two values.
func (v Value) Min(other Value) Value {
	cmp := v.CompareTo(other)
	if cmp == CompareLessThan || cmp == CompareEqual {
		return v
	}
	return other
}

// Max returns the maximum of two values.
func (v Value) Max(other Value) Value {
	cmp := v.CompareTo(other)
	if cmp == CompareGreaterThan || cmp == CompareEqual {
		return v
	}
	return other
}
