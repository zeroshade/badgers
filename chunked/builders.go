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
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/badgers"
)

type arrowPrimitiveBldr[T badgers.BadgersDataType] interface {
	array.Builder
	Append(T)
	AppendValues([]T, []bool)
}

// type ChunkedBuilder[T badgers.BadgersDataType] interface {
// 	AppendValue(T)
// 	AppendNull()
// 	AppendOptional(*T)
// 	AppendValues([]T, []bool)
// 	ShrinkToFit()
// 	Finish() Array
// }

type primitiveChunkedBuilder[T badgers.SimpleType | []byte] struct {
	bldr  arrowPrimitiveBldr[T]
	field badgers.Field
}

func (p *primitiveChunkedBuilder[T]) AppendValue(val T) {
	p.bldr.Append(val)
}

func (p *primitiveChunkedBuilder[T]) AppendNull() {
	p.bldr.AppendNull()
}

func (p *primitiveChunkedBuilder[T]) AppendOptional(val *T) {
	if val == nil {
		p.bldr.AppendNull()
	} else {
		p.bldr.Append(*val)
	}

}

func (p *primitiveChunkedBuilder[T]) AppendValues(vals []T, valid []bool) {
	p.bldr.AppendValues(vals, valid)
}

func (p *primitiveChunkedBuilder[T]) ShrinkToFit() {
	p.bldr.Resize(p.bldr.Len())
}

func (p *primitiveChunkedBuilder[T]) Finish() *Chunked[T] {
	arr := p.bldr.NewArray().(typedArray[T])
	p.bldr.Release()

	return &Chunked[T]{
		chunks:    []arrow.Array{arr},
		field:     p.field,
		length:    uint64(arr.Len()),
		nullCount: uint64(arr.NullN()),
	}
}
