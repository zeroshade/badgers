// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package badgers

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

//go:generate stringer -type=TimeUnit

type TimeUnit int8

const (
	Nanoseconds TimeUnit = iota
	Microseconds
	Milliseconds
)

var timeToArrowMap = map[TimeUnit]arrow.TimeUnit{
	Nanoseconds:  arrow.Nanosecond,
	Microseconds: arrow.Microsecond,
	Milliseconds: arrow.Millisecond,
}

func (t TimeUnit) ToArrow() arrow.TimeUnit {
	return timeToArrowMap[t]
}

type CategoricalOrdering int8

const (
	Physical CategoricalOrdering = iota
	Lexical
)

type RevMapping interface {
	IsGlobal() bool
	IsLocal() bool
	GetCategories() []string
	BuildLocal(categories []string) RevMapping
	Len() uint
	Get(idx uint32) (string, bool)
	SameSrc(other RevMapping) bool
	Find(value string) (uint32, bool)
}

type DataType interface {
	ValueWithinRange(any) bool
	Clone() DataType
	ArrayType() arrow.DataType
}

type Field struct {
	Name string
	Type DataType
}

type Unknown struct{}

func (Unknown) ValueWithinRange(any) bool { return false }
func (Unknown) Clone() DataType           { return Unknown{} }
func (Unknown) ArrayType() arrow.DataType { return nil }
func (Unknown) String() string            { return "unknown" }

type Boolean struct{}

func (Boolean) String() string            { return "bool" }
func (Boolean) ValueWithinRange(any) bool { return false }
func (Boolean) Clone() DataType           { return Boolean{} }
func (Boolean) ArrayType() arrow.DataType { return arrow.FixedWidthTypes.Boolean }

type primitiveType[T NumericType] struct{}

func (primitiveType[T]) String() string  { return reflect.TypeOf(T(0)).String() }
func (primitiveType[T]) Clone() DataType { return primitiveType[T]{} }
func (primitiveType[T]) ValueWithinRange(v any) bool {
	return reflect.ValueOf(v).CanConvert(reflect.TypeOf(T(0)))
}
func (primitiveType[T]) ArrayType() arrow.DataType { return arrow.GetDataType[T]() }

type Uint8 = primitiveType[uint8]
type Int8 = primitiveType[int8]
type Uint16 = primitiveType[uint16]
type Int16 = primitiveType[int16]
type Uint32 = primitiveType[uint32]
type Int32 = primitiveType[int32]
type Uint64 = primitiveType[uint64]
type Int64 = primitiveType[int64]
type Float32 = primitiveType[float32]
type Float64 = primitiveType[float64]

type Decimal struct {
	Precision, Scale uint32
}

func (d Decimal) Clone() DataType           { return d }
func (Decimal) ValueWithinRange(v any) bool { return false }
func (d Decimal) ArrayType() arrow.DataType {
	return &arrow.Decimal128Type{
		Precision: int32(d.Precision),
		Scale:     int32(d.Scale),
	}
}

type String struct{}

func (String) String() string              { return "str" }
func (String) Clone() DataType             { return String{} }
func (String) ValueWithinRange(v any) bool { return false }
func (String) ArrayType() arrow.DataType   { return arrow.BinaryTypes.LargeString }

type Binary struct{}

func (Binary) String() string              { return "bin" }
func (Binary) Clone() DataType             { return Binary{} }
func (Binary) ValueWithinRange(v any) bool { return false }
func (Binary) ArrayType() arrow.DataType   { return arrow.BinaryTypes.LargeBinary }

type Date struct{}

func (Date) String() string              { return "date" }
func (Date) Clone() DataType             { return Date{} }
func (Date) ValueWithinRange(v any) bool { return false }
func (Date) ArrayType() arrow.DataType   { return arrow.FixedWidthTypes.Date32 }

type DateTime struct {
	Unit TimeUnit
	Zone string
}

func (d DateTime) String() string            { return fmt.Sprintf("datetime(unit=%s, zone=%s)", d.Unit, d.Zone) }
func (d DateTime) Clone() DataType           { return d }
func (DateTime) ValueWithinRange(v any) bool { return false }
func (d DateTime) ArrayType() arrow.DataType {
	return &arrow.TimestampType{Unit: d.Unit.ToArrow(), TimeZone: d.Zone}
}

type Duration struct {
	Unit TimeUnit
}

func (d Duration) String() string            { return fmt.Sprintf("duration(unit=%s)", d.Unit) }
func (d Duration) Clone() DataType           { return d }
func (Duration) ValueWithinRange(v any) bool { return false }
func (d Duration) ArrayType() arrow.DataType {
	return &arrow.DurationType{Unit: d.Unit.ToArrow()}
}

type Time struct{}

func (Time) String() string              { return "time" }
func (Time) Clone() DataType             { return Time{} }
func (Time) ValueWithinRange(v any) bool { return false }
func (Time) ArrayType() arrow.DataType   { return arrow.FixedWidthTypes.Time64ns }

type Null struct{}

func (Null) String() string              { return "null" }
func (Null) Clone() DataType             { return Null{} }
func (Null) ValueWithinRange(v any) bool { return false }
func (Null) ArrayType() arrow.DataType   { return arrow.Null }

type Categorical struct {
	Mapping  RevMapping
	Ordering CategoricalOrdering
}

func (d Categorical) Clone() DataType {
	return d
}

func (Categorical) ValueWithinRange(any) bool { return false }
func (Categorical) ArrayType() arrow.DataType { return arrow.PrimitiveTypes.Uint32 }

type Enum struct {
	Mapping  RevMapping
	Ordering CategoricalOrdering
}

func (d Enum) Clone() DataType {
	return d
}

func (Enum) ValueWithinRange(any) bool { return false }
func (Enum) ArrayType() arrow.DataType { return arrow.PrimitiveTypes.Uint32 }

type List struct {
	child DataType
}

func (l List) ValueWithinRange(any) bool { return false }
func (l List) Child() DataType           { return l.child }
func (l List) Clone() DataType {
	l.child = l.child.Clone()
	return l
}
func (l List) ArrayType() arrow.DataType {
	return arrow.ListOf(l.child.ArrayType())
}

type Struct []Field

func (s Struct) ValueWithinRange(any) bool { return false }
func (s Struct) NumFields() int            { return len(s) }
func (s Struct) ArrayType() arrow.DataType {
	fields := make([]arrow.Field, len(s))
	for i, f := range s {
		fields[i].Name = f.Name
		fields[i].Nullable = true
		fields[i].Type = f.Type.ArrayType()
	}
	return arrow.StructOf(fields...)
}

func (s Struct) Clone() DataType {
	return Struct(slices.Clone(s))
}

type Array struct {
	elemType DataType
	size     int
}

func (a Array) ValueWithinRange(any) bool { return false }
func (a Array) Elem() DataType            { return a.elemType }
func (a Array) Size() int                 { return a.size }
func (a Array) ArrayType() arrow.DataType {
	return arrow.FixedSizeListOf(int32(a.size), a.elemType.ArrayType())
}

func (a Array) Clone() DataType {
	a.elemType = a.elemType.Clone()
	return a
}

func InnerDtype(dt DataType) DataType {
	switch dt := dt.(type) {
	case List:
		return dt.child
	case Array:
		return dt.elemType
	default:
		return nil
	}
}

func IsKnown(dt DataType) bool {
	switch dt := dt.(type) {
	case List:
		return IsKnown(dt.child)
	case Struct:
		for _, f := range dt {
			if !IsKnown(f.Type) {
				return false
			}
		}
		return true
	case Unknown:
		return false
	default:
		return true
	}
}

func ToPhysical(dt DataType) DataType {
	switch dt := dt.(type) {
	case Date:
		return Int32{}
	case DateTime:
		return Int64{}
	case Duration:
		return Int64{}
	case Time:
		return Int64{}
	case Categorical, Enum:
		return Uint32{}
	case Array:
		return Array{
			elemType: ToPhysical(dt.elemType),
			size:     dt.size,
		}
	case List:
		return List{child: ToPhysical(dt.child)}
	case Struct:
		out := make([]Field, len(dt))
		for i, f := range dt {
			out[i].Name = f.Name
			out[i].Type = ToPhysical(f.Type)
		}
		return Struct(out)
	default:
		return dt.Clone()
	}
}

func IsLogical(dt DataType) bool {
	switch dt.(type) {
	case Date, DateTime, Duration, Time, Categorical, Enum, Array, List, Struct:
		return true
	default:
		return false
	}
}

func IsTemporal(dt DataType) bool {
	switch dt.(type) {
	case Date, DateTime, Time, Duration:
		return true
	default:
		return false
	}
}

func IsFloat(dt DataType) bool {
	switch dt.(type) {
	case Float32, Float64:
		return true
	default:
		return false
	}
}

func IsInteger(dt DataType) bool {
	switch dt.(type) {
	case Int8, Uint8, Int16, Uint16, Int32, Uint32, Int64, Uint64:
		return true
	default:
		return false
	}
}

func IsSignedInteger(dt DataType) bool {
	switch dt.(type) {
	case Int8, Int16, Int32, Int64:
		return true
	default:
		return false
	}
}

func IsUnsignedInteger(dt DataType) bool {
	switch dt.(type) {
	case Uint8, Uint16, Uint32, Uint64:
		return true
	default:
		return false
	}
}

func IsNumeric(dt DataType) bool {
	return IsInteger(dt) || IsFloat(dt)
}

type SignedIntegerType interface {
	int8 | int16 | int32 | int64
}

type UnsignedIntegerType interface {
	uint8 | uint16 | uint32 | uint64
}

type IntegerType interface {
	SignedIntegerType | UnsignedIntegerType
}

type FloatType interface {
	float32 | float64
}

type NumericType interface {
	IntegerType | FloatType
}

type TemporalType interface {
	arrow.Time64 | arrow.Date32 | arrow.Timestamp
}

type PrimitiveType interface {
	NumericType | TemporalType
}

type SimpleType interface {
	bool | PrimitiveType | string
}

type BadgersDataType interface {
	bool | PrimitiveType | string | []byte
}

func FromArrowType(dt arrow.DataType) DataType {
	switch dt.ID() {
	case arrow.INT8:
		return Int8{}
	case arrow.INT16:
		return Int16{}
	case arrow.INT32:
		return Int32{}
	case arrow.INT64:
		return Int64{}
	case arrow.UINT8:
		return Uint8{}
	case arrow.UINT16:
		return Uint16{}
	case arrow.UINT32:
		return Uint32{}
	case arrow.UINT64:
		return Uint64{}
	case arrow.FLOAT32:
		return Float32{}
	case arrow.FLOAT64:
		return Float64{}
	case arrow.BOOL:
		return Boolean{}
	case arrow.BINARY:
		return Binary{}
	case arrow.LARGE_BINARY:
		return Binary{}
	case arrow.DATE32:
		return Date{}
	case arrow.TIME64:
		return Time{}
	case arrow.STRING:
		return String{}
	case arrow.LARGE_STRING:
		return String{}
	default:
		panic("unimplemented type")
	}
}

func ToDataType[T BadgersDataType]() DataType {
	var z T
	switch any(z).(type) {
	case bool:
		return Boolean{}
	case int8:
		return Int8{}
	case int16:
		return Int16{}
	case int32:
		return Int32{}
	case int64:
		return Int64{}
	case uint8:
		return Uint8{}
	case uint16:
		return Uint16{}
	case uint32:
		return Uint32{}
	case uint64:
		return Uint64{}
	case float32:
		return Float32{}
	case float64:
		return Float64{}
	case arrow.Time64:
		return Time{}
	case arrow.Date32:
		return Date{}
	case arrow.Timestamp:
		return DateTime{Unit: Milliseconds}
	case string:
		return String{}
	case []byte:
		return Binary{}
	case arrow.Duration:
		return Duration{Unit: Milliseconds}
	default:
		return Unknown{}
	}
}

type Allocator = memory.Allocator
