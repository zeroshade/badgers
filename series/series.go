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

package series

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/badgers"
	"github.com/apache/badgers/chunked"
)

func NewBinary(name string, vals [][]byte) *Series {
	return ToSeriesBinary(chunked.NewSimple(name, vals))
}

func NewSimple[T badgers.SimpleType](name string, vals []T) *Series {
	return ToSeriesSimple(chunked.NewSimple[T](name, vals))
}

func NewPrimitive[T badgers.PrimitiveType](name string, vals []T) *Series {
	return ToSeriesSimple(chunked.NewPrimitive(name, vals))
}

func NewNullable[T badgers.SimpleType](name string, vals []*T) *Series {
	return NewNullableWithAlloc(name, vals, memory.DefaultAllocator)
}

func NewNullableWithAlloc[T badgers.SimpleType](name string, vals []*T, alloc badgers.Allocator) *Series {
	return ToSeriesSimple(chunked.NewNullableSimple(name, vals, alloc))
}

type Series struct {
	s privateSeries
}

// Take(indices *chunked.Uint32) (Series, error)
// TakeUnchecked(indices *chunked.Uint32) (Series, error)
// TakeSlice(indices []uint32) (Series, error)
// TakeSliceUnchecked(indices []uint32) (Series, error)

// Rechunk() Series
// NewFromIndex(index, length uint) Series
// Cast(dt badgers.DataType) (Series, error)
// Get(idx uint) (any, error)
// NullCount() uint
// HasValidity() bool
// IsNull() *chunked.Bool
// IsNotNull() *chunked.Bool
// Reverse() Series

func (s *Series) ToSeries() *Series      { return s }
func (s *Series) IsSeries() bool         { return true }
func (s *Series) Type() badgers.DataType { return s.s.dtype() }
func (s *Series) Name() string {
	return s.s.field().Name
}

func (s *Series) Retain()  { s.s.retain() }
func (s *Series) Release() { s.s.release() }

func (s *Series) Chunks() *[]arrow.Array {
	return s.s.chunks()
}

func (s *Series) Rename(name string) {
	s.s.rename(name)
}

func (s *Series) Len() uint64 { return s.s.len() }

func (s *Series) Slice(offset int64, length uint64) *Series {
	return &Series{s: s.s.slice(offset, length)}
}

func (s *Series) Append(other *Series) error {
	return s.s.append(other.s)
}

func (s *Series) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "shape: (%d,)\n", s.Len())
	fmt.Fprintf(&b, "Series: '%s' [%s]\n", s.Name(), s.s.dtype())
	fmt.Fprintln(&b, "[")
	for _, c := range *s.Chunks() {
		for i := 0; i < c.Len(); i++ {
			fmt.Fprintf(&b, "\t%s\n", c.ValueStr(i))
		}
	}
	fmt.Fprint(&b, "]")
	return b.String()
}

func (s *Series) Get(idx uint) (any, error) {
	return s.s.get(idx)
}

func (s *Series) GetUnchecked(idx uint) any { return s.s.getUnchecked(idx) }

func (s *Series) Filter(ctx context.Context, mask *chunked.Bool) (*Series, error) {
	result, err := s.s.filter(ctx, mask)
	if err != nil {
		return nil, err
	}

	return &Series{s: result}, nil
}

func (s *Series) LessScalar(ctx context.Context, rhs any) (*chunked.Bool, error) {
	return s.s.lessScalar(ctx, rhs)
}

func (s *Series) LessEqScalar(ctx context.Context, rhs any) (*chunked.Bool, error) {
	return s.s.lessEqScalar(ctx, rhs)
}

func (s *Series) GreaterScalar(ctx context.Context, rhs any) (*Series, error) {
	result, err := s.s.greaterScalar(ctx, rhs)
	if err != nil {
		return nil, err
	}

	return &Series{s: result}, nil
}

func (s *Series) GreaterEqScalar(ctx context.Context, rhs any) (*Series, error) {
	result, err := s.s.greaterEqScalar(ctx, rhs)
	if err != nil {
		return nil, err
	}

	return &Series{s: result}, nil
}

func (s *Series) EqualScalar(ctx context.Context, rhs any) (*chunked.Bool, error) {
	return s.s.equalScalar(ctx, rhs)
}

func (s *Series) NotEqualScalar(ctx context.Context, rhs any) (*chunked.Bool, error) {
	return s.s.notEqualScalar(ctx, rhs)
}

func Optional[T badgers.SimpleType](v T) *T { return &v }

func ToSeriesBinary(c *chunked.Binary) *Series {
	return &Series{s: &baseWrap[[]byte]{Chunked: c, eq: byteEqual{}}}
}

func ToSeriesSimple[T badgers.SimpleType](c *chunked.Chunked[T]) *Series {
	return &Series{s: &baseWrap[T]{Chunked: c, eq: primEqual[T]{}}}
}

func FromArrowChunked(name string, c *arrow.Chunked) *Series {
	if c == nil {
		panic("nil chunked array passed")
	}
	for _, chunk := range c.Chunks() {
		chunk.Retain()
	}

	field := badgers.Field{Name: name, Type: badgers.FromArrowType(c.DataType())}
	return fromArrowSimple(field, c.Chunks())
}

func fromArrowSimple(field badgers.Field, chunks []arrow.Array) *Series {
	switch field.Type.(type) {
	case badgers.Int8:
		return ToSeriesSimple(chunked.FromArrowChunkedNoCheck[int8](field, chunks))
	case badgers.Uint8:
		return ToSeriesSimple(chunked.FromArrowChunkedNoCheck[uint8](field, chunks))
	case badgers.Int16:
		return ToSeriesSimple(chunked.FromArrowChunkedNoCheck[int16](field, chunks))
	case badgers.Uint16:
		return ToSeriesSimple(chunked.FromArrowChunkedNoCheck[uint16](field, chunks))
	case badgers.Int32:
		return ToSeriesSimple(chunked.FromArrowChunkedNoCheck[int32](field, chunks))
	case badgers.Uint32:
		return ToSeriesSimple(chunked.FromArrowChunkedNoCheck[uint32](field, chunks))
	case badgers.Int64:
		return ToSeriesSimple(chunked.FromArrowChunkedNoCheck[int64](field, chunks))
	case badgers.Uint64:
		return ToSeriesSimple(chunked.FromArrowChunkedNoCheck[uint64](field, chunks))
	case badgers.Float32:
		return ToSeriesSimple(chunked.FromArrowChunkedNoCheck[float32](field, chunks))
	case badgers.Float64:
		return ToSeriesSimple(chunked.FromArrowChunkedNoCheck[float64](field, chunks))
	case badgers.String:
		return ToSeriesSimple(chunked.FromArrowChunkedNoCheck[string](field, chunks))
	case badgers.Binary:
		return ToSeriesBinary(chunked.FromArrowChunkedNoCheck[[]byte](field, chunks))
	default:
		panic("unimplemented type")
	}
}
