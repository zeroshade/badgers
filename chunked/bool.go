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

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/compute"
	"github.com/apache/badgers"
)

func foldReduceBool(ctx context.Context, fn string, lhs, rhs *Bool, extra ...*Bool) (*Bool, error) {
	arg1 := arrow.NewChunked(lhs.chunks[0].DataType(), lhs.chunks)
	defer arg1.Release()

	arg2 := arrow.NewChunked(rhs.chunks[0].DataType(), rhs.chunks)
	defer arg2.Release()

	if len(extra) > 0 {
		args := make([]compute.Datum, len(extra))
		for i, e := range extra {
			arg := arrow.NewChunked(e.chunks[0].DataType(), e.chunks)
			defer arg.Release()
			args[i] = compute.NewDatumWithoutOwning(arg)
		}
		return callFuncBinaryReduce[bool](ctx, badgers.Field{Type: badgers.Boolean{}}, fn, nil,
			compute.NewDatumWithoutOwning(arg1),
			compute.NewDatumWithoutOwning(arg2), args...)
	}

	return callFunc[bool](ctx, badgers.Field{Type: badgers.Boolean{}}, fn, nil,
		compute.NewDatumWithoutOwning(arg1), compute.NewDatumWithoutOwning(arg2))
}

func And(ctx context.Context, lhs, rhs *Bool, extra ...*Bool) (*Bool, error) {
	return foldReduceBool(ctx, "and", lhs, rhs, extra...)
}

func Or(ctx context.Context, lhs, rhs *Bool, extra ...*Bool) (*Bool, error) {
	return foldReduceBool(ctx, "or", lhs, rhs, extra...)
}
