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

package chunked

import (
	"context"
	"fmt"
	"slices"
	"unsafe"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/compute"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/arrow/scalar"
	"github.com/apache/badgers"
)

func NewPrimitive[T badgers.PrimitiveType](name string, vals []T) *Chunked[T] {
	dt := badgers.ToDataType[T]()

	data := unsafe.Pointer(unsafe.SliceData(vals))
	buf := memory.NewBufferBytes(unsafe.Slice((*byte)(data),
		len(vals)*int(unsafe.Sizeof(vals[0]))))

	d := array.NewData(dt.ArrayType(), len(vals), []*memory.Buffer{nil, buf}, nil, 0, 0)
	defer d.Release()

	arr := array.MakeFromData(d).(typedArray[T])
	return &Chunked[T]{
		chunks:    []arrow.Array{arr},
		field:     badgers.Field{Name: name, Type: dt},
		length:    uint64(len(vals)),
		nullCount: 0,
	}
}

func NewSimple[T badgers.SimpleType | []byte](name string, vals []T) *Chunked[T] {
	return NewSimpleWithAlloc(name, vals, memory.DefaultAllocator)
}

func NewSimpleWithAlloc[T badgers.SimpleType | []byte](name string, vals []T, alloc badgers.Allocator) *Chunked[T] {
	dt := badgers.ToDataType[T]()
	arrowType := dt.ArrayType()

	bldr := primitiveChunkedBuilder[T]{
		bldr:  array.NewBuilder(alloc, arrowType).(arrowPrimitiveBldr[T]),
		field: badgers.Field{Name: name, Type: dt},
	}
	bldr.AppendValues(vals, nil)
	bldr.ShrinkToFit()
	return bldr.Finish()
}

func NewNullableSimple[T badgers.SimpleType](name string, vals []*T, alloc badgers.Allocator) *Chunked[T] {
	dt := badgers.ToDataType[T]()
	arrowType := dt.ArrayType()

	bldr := primitiveChunkedBuilder[T]{
		bldr:  array.NewBuilder(alloc, arrowType).(arrowPrimitiveBldr[T]),
		field: badgers.Field{Name: name, Type: dt},
	}

	for _, v := range vals {
		bldr.AppendOptional(v)
	}

	bldr.ShrinkToFit()
	return bldr.Finish()
}

func FromArrowChunkedNoCheck[T badgers.SimpleType | []byte](field badgers.Field, chunks []arrow.Array) *Chunked[T] {
	out := &Chunked[T]{
		chunks: chunks,
		field:  field,
	}
	out.ComputeLen()
	return out
}

// func New[T badgers.BadgersDataType](name string, vals []T) *Simple[T] {
// 	switch v := any(vals).(type) {
// 	case []uint8:
// 		return NewPrimitive(name, v)
// 	case []uint16:
// 		return NewPrimitive(name, v)
// 	case []uint32:
// 		return NewPrimitive(name, v)
// 	case []uint64:
// 		return NewPrimitive(name, v)
// 	case []int8:
// 		return NewPrimitive(name, v)
// 	case []int16:
// 		return NewPrimitive(name, v)
// 	case []int32:
// 		return NewPrimitive(name, v)
// 	case []int64:
// 		return NewPrimitive(name, v)
// 	case []float32:
// 		return NewPrimitive(name, v)
// 	case []float64:
// 		return NewPrimitive(name, v)
// 	case [][]byte:
// 		return NewSimple(name, v, memory.DefaultAllocator)
// 	case []bool:
// 		return NewSimple(name, v, memory.DefaultAllocator)
// 	case []string:
// 		return NewSimple(name, v, memory.DefaultAllocator)
// 	}

// 	panic("bad data type for chunked")
// }

type Chunked[T badgers.SimpleType | []byte] struct {
	chunks    []arrow.Array
	field     badgers.Field
	length    uint64
	nullCount uint64

	chunkLens []int64
}

func (c *Chunked[T]) Retain() {
	for _, chunk := range c.chunks {
		chunk.Retain()
	}
}

func (c *Chunked[T]) Release() {
	for _, chunk := range c.chunks {
		chunk.Release()
	}
}

func (c *Chunked[T]) Len() uint64   { return c.length }
func (c *Chunked[T]) NullN() uint64 { return c.nullCount }
func (c *Chunked[T]) Rename(name string) {
	c.field.Name = name
}

func (c *Chunked[T]) Chunks() *[]arrow.Array {
	return &c.chunks
}

func (c *Chunked[T]) IsEmpty() bool { return c.length == 0 }

func (c *Chunked[T]) Field() badgers.Field { return c.field }

func (c *Chunked[T]) ComputeLen() {
	c.nullCount, c.length, c.chunkLens = 0, 0, make([]int64, len(c.chunks))

	for i, chunk := range c.chunks {
		c.nullCount += uint64(chunk.NullN())
		c.chunkLens[i] = int64(chunk.Len())
		c.length += uint64(chunk.Len())
	}
}

func (c *Chunked[T]) FirstNonNull() int64 {
	switch {
	case c.IsEmpty() || c.nullCount == c.length:
		return -1
	case c.nullCount == 0:
		return 0
	}

	idx := int64(0)
	for _, chunk := range c.chunks {
		if chunk.NullN() == chunk.Len() {
			idx += int64(chunk.Len())
			continue
		}

		return idx + findFirstNotNull(chunk.NullBitmapBytes(),
			int64(chunk.Data().Offset()), int64(chunk.Len()))
	}

	return -1
}

func (c *Chunked[T]) Append(other *Chunked[T]) {
	if len(c.chunks) == 1 && c.length == 0 {
		c.chunks[0].Release()

		c.chunks = slices.Clone(other.chunks)
		c.chunkLens = slices.Clone(other.chunkLens)
		for _, chunk := range c.chunks {
			chunk.Retain()
		}
	} else {
		for _, chunk := range other.chunks {
			if chunk.Len() > 0 {
				chunk.Retain()
				c.chunks = append(c.chunks, chunk)
				c.chunkLens = append(c.chunkLens, int64(chunk.Len()))
			}
		}
	}

	c.length += other.length
	c.nullCount += other.nullCount
}

func (c *Chunked[T]) GetUnchecked(i uint) T {
	idx, r := indexToChunkedIndex(c.chunkLens, int64(i))
	return c.chunks[idx].(typedArray[T]).Value(int(r))
}

func (c *Chunked[T]) Get(i uint) (val T, err error) {
	idx, r := indexToChunkedIndex(c.chunkLens, int64(i))
	if int(idx) >= len(c.chunks) {
		return val, fmt.Errorf("invalid chunk index")
	}

	if int(r) >= c.chunks[idx].Len() {
		return val, fmt.Errorf("invalid index")
	}

	val = c.chunks[idx].(typedArray[T]).Value(int(r))
	return
}

func (c *Chunked[T]) Slice(offset int64, length uint64) *Chunked[T] {
	chunks, l := slice(c.chunks, offset, length, uint64(c.length))
	out := &Chunked[T]{
		chunks: chunks,
		field:  c.field,
		length: l,
	}
	out.ComputeLen()
	return out
}

func datumToChunked[T badgers.SimpleType | []byte](field badgers.Field, result compute.Datum) *Chunked[T] {
	out := &Chunked[T]{field: field}
	switch result.Kind() {
	case compute.KindChunked:
		out.chunks = result.(*compute.ChunkedDatum).Chunks()
		out.chunkLens = make([]int64, len(out.chunks))
		for i, o := range out.chunks {
			o.Retain()
			out.chunkLens[i] = int64(o.Len())
			out.length += uint64(o.Len())
			out.nullCount += uint64(o.NullN())
		}
	case compute.KindArray:
		out.chunks = []arrow.Array{result.(*compute.ArrayDatum).MakeArray()}
		out.chunkLens = []int64{int64(out.chunks[0].Len())}
		out.length = uint64(out.chunks[0].Len())
		out.nullCount = uint64(out.chunks[0].NullN())
	}
	return out
}

func (c *Chunked[T]) Filter(ctx context.Context, mask *Bool) (*Chunked[T], error) {
	chkd := arrow.NewChunked(c.chunks[0].DataType(), c.chunks)
	defer chkd.Release()

	maskchunks := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, mask.chunks)
	defer maskchunks.Release()

	result, err := compute.Filter(ctx, compute.NewDatumWithoutOwning(chkd),
		compute.NewDatumWithoutOwning(maskchunks), *compute.DefaultFilterOptions())
	if err != nil {
		return nil, err
	}
	defer result.Release()

	return datumToChunked[T](c.field, result), nil
}

func callFuncBinaryReduce[Out badgers.SimpleType | []byte](ctx context.Context, outField badgers.Field, fn string, opts compute.FunctionOptions, arg1, arg2 compute.Datum, args ...compute.Datum) (*Chunked[Out], error) {
	result, err := compute.CallFunction(ctx, fn, opts, arg1, arg2)
	if err != nil {
		return nil, err
	}
	defer result.Release()

	for _, a := range args {
		result, err = compute.CallFunction(ctx, fn, opts, result, a)
		if err != nil {
			return nil, err
		}
		defer result.Release()
	}

	return datumToChunked[Out](outField, result), nil
}

func callFunc[Out badgers.SimpleType | []byte](ctx context.Context, outField badgers.Field, fn string, opts compute.FunctionOptions, args ...compute.Datum) (*Chunked[Out], error) {
	result, err := compute.CallFunction(ctx, fn, opts, args...)
	if err != nil {
		return nil, err
	}
	defer result.Release()

	return datumToChunked[Out](outField, result), nil
}

func callFuncScalar[In, Out badgers.SimpleType | []byte](ctx context.Context, fn string, lhs *Chunked[In], rhs scalar.Scalar, opts compute.FunctionOptions) (*Chunked[Out], error) {
	chkd := arrow.NewChunked(lhs.chunks[0].DataType(), lhs.chunks)
	defer chkd.Release()

	result, err := compute.CallFunction(ctx, fn, opts,
		compute.NewDatumWithoutOwning(chkd), compute.NewDatumWithoutOwning(rhs))
	if err != nil {
		return nil, err
	}
	defer result.Release()

	outField := lhs.field
	outField.Type = badgers.ToDataType[Out]()
	return datumToChunked[Out](outField, result), nil
}

func (c *Chunked[T]) LtScalar(ctx context.Context, rhs any) (*Bool, error) {
	return callFuncScalar[T, bool](ctx, "less", c, scalar.MakeScalar(rhs), nil)
}

func (c *Chunked[T]) LtEqScalar(ctx context.Context, rhs any) (*Bool, error) {
	return callFuncScalar[T, bool](ctx, "less_equal", c, scalar.MakeScalar(rhs), nil)
}

func (c *Chunked[T]) GtScalar(ctx context.Context, rhs any) (*Bool, error) {
	return callFuncScalar[T, bool](ctx, "greater", c, scalar.MakeScalar(rhs), nil)
}

func (c *Chunked[T]) GtEqScalar(ctx context.Context, rhs any) (*Bool, error) {
	return callFuncScalar[T, bool](ctx, "greater_equal", c, scalar.MakeScalar(rhs), nil)
}

func (c *Chunked[T]) EqScalar(ctx context.Context, rhs any) (*Bool, error) {
	return callFuncScalar[T, bool](ctx, "equal", c, scalar.MakeScalar(rhs), nil)
}

func (c *Chunked[T]) NotEqScalar(ctx context.Context, rhs any) (*Bool, error) {
	return callFuncScalar[T, bool](ctx, "not_equal", c, scalar.MakeScalar(rhs), nil)
}

type typedArray[T badgers.SimpleType | []byte] interface {
	arrow.Array
	Value(int) T
}

type Bool = Chunked[bool]
type Uint8 = Chunked[uint8]
type Uint16 = Chunked[uint16]
type Uint32 = Chunked[uint32]
type Uint64 = Chunked[uint64]
type Int8 = Chunked[int8]
type Int16 = Chunked[int16]
type Int32 = Chunked[int32]
type Int64 = Chunked[int64]
type Float32 = Chunked[float32]
type Float64 = Chunked[float64]
type String = Chunked[string]
type Binary = Chunked[[]byte]
