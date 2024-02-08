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
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/badgers"
	"github.com/apache/badgers/chunked"
)

type privateSeriesNumeric interface {
	bitReprIsLarge() bool
	bitReprLarge() *chunked.Uint64
	bitReprSmall() *chunked.Uint32
}

type privateSeries interface {
	retain()
	release()
	len() uint64
	rename(string)
	chunks() *[]arrow.Array
	field() badgers.Field
	// getListBuilder(name string, valuesCap, listCap uint) ListBuilder
	dtype() badgers.DataType
	slice(offset int64, length uint64) privateSeries
	append(privateSeries) error
	// explodeByOffsets(offsets []int64) Series
	equalElement(idxSelf, idxOther uint, other *Series) bool
	get(idx uint) (any, error)
	getUnchecked(idx uint) any
	filter(context.Context, *chunked.Bool) (privateSeries, error)
	lessScalar(ctx context.Context, rhs any) (*chunked.Bool, error)
	lessEqScalar(ctx context.Context, rhs any) (*chunked.Bool, error)
	greaterScalar(ctx context.Context, rhs any) (privateSeries, error)
	greaterEqScalar(ctx context.Context, rhs any) (privateSeries, error)
	equalScalar(ctx context.Context, rhs any) (*chunked.Bool, error)
	notEqualScalar(ctx context.Context, rhs any) (*chunked.Bool, error)

	// addTo(rhs Series) (Series, error)
	// subtract(rhs Series) (Series, error)
}

type equaler[T badgers.SimpleType | []byte] interface {
	equalElem(self, other T) bool
}

type primEqual[T comparable] struct{}

func (primEqual[T]) equalElem(self, other T) bool {
	return self == other
}

type byteEqual struct{}

func (byteEqual) equalElem(self, other []byte) bool {
	return bytes.Equal(self, other)
}

type baseWrap[T badgers.SimpleType | []byte] struct {
	*chunked.Chunked[T]

	eq equaler[T]
}

func (b *baseWrap[T]) retain()  { b.Retain() }
func (b *baseWrap[T]) release() { b.Release() }

func (b *baseWrap[T]) len() uint64 { return b.Len() }

func (b *baseWrap[T]) chunks() *[]arrow.Array { return b.Chunks() }

func (b *baseWrap[T]) rename(n string) { b.Rename(n) }

func (b *baseWrap[T]) field() badgers.Field { return b.Field() }

func (p *baseWrap[T]) get(idx uint) (any, error) {
	return p.Get(idx)
}
func (p *baseWrap[T]) getUnchecked(idx uint) any {
	return p.GetUnchecked(idx)
}

func (p *baseWrap[T]) dtype() badgers.DataType { return p.Field().Type }
func (p *baseWrap[T]) equalElement(idxSelf, idxOther uint, other *Series) bool {
	rhs, ok := other.s.(*baseWrap[T])
	if !ok {
		return false
	}

	switch {
	case idxSelf >= uint(p.Len()):
		return false
	case idxOther >= uint(rhs.Len()):
		return false
	}

	return p.eq.equalElem(p.GetUnchecked(idxSelf), rhs.GetUnchecked(idxOther))
}

func (p *baseWrap[T]) append(other privateSeries) error {
	rhs, ok := other.(*baseWrap[T])
	if !ok {
		return fmt.Errorf("incompatible types")
	}

	p.Append(rhs.Chunked)
	return nil
}

func (p *baseWrap[T]) slice(offset int64, length uint64) privateSeries {
	return &baseWrap[T]{
		Chunked: p.Chunked.Slice(offset, length),
		eq:      p.eq,
	}
}

func (p *baseWrap[T]) filter(ctx context.Context, mask *chunked.Bool) (privateSeries, error) {
	result, err := p.Filter(ctx, mask)
	if err != nil {
		return nil, err
	}

	return &baseWrap[T]{Chunked: result, eq: p.eq}, nil
}

func (p *baseWrap[T]) lessScalar(ctx context.Context, rhs any) (*chunked.Bool, error) {
	return p.LtScalar(ctx, rhs)
}

func (p *baseWrap[T]) lessEqScalar(ctx context.Context, rhs any) (*chunked.Bool, error) {
	return p.LtEqScalar(ctx, rhs)
}

func (p *baseWrap[T]) greaterScalar(ctx context.Context, rhs any) (privateSeries, error) {
	result, err := p.GtScalar(ctx, rhs)
	if err != nil {
		return nil, err
	}

	return &baseWrap[bool]{Chunked: result, eq: primEqual[bool]{}}, nil
}

func (p *baseWrap[T]) greaterEqScalar(ctx context.Context, rhs any) (privateSeries, error) {
	result, err := p.GtEqScalar(ctx, rhs)
	if err != nil {
		return nil, err
	}

	return &baseWrap[bool]{Chunked: result, eq: primEqual[bool]{}}, nil
}

func (p *baseWrap[T]) equalScalar(ctx context.Context, rhs any) (*chunked.Bool, error) {
	return p.EqScalar(ctx, rhs)
}

func (p *baseWrap[T]) notEqualScalar(ctx context.Context, rhs any) (*chunked.Bool, error) {
	return p.NotEqScalar(ctx, rhs)
}
