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

package frame_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/badgers/chunked"
	"github.com/apache/badgers/frame"
	"github.com/apache/badgers/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataframe(t *testing.T) {
	s1 := series.NewSimple("Fruit", []string{"Apple", "Apple", "Pear"})
	s2 := series.NewSimple("Color", []string{"Red", "Yellow", "Green"})

	df, err := frame.New(s1, s2)
	assert.NoError(t, err)
	defer df.Release()

	assert.Equal(t, `shape: (3, 2)
+-------+--------+
| Fruit | Color  |
| --    | --     |
| str   | str    |
+-------+--------+
| Apple | Red    |
| Apple | Yellow |
| Pear  | Green  |
+-------+--------+`, df.String())
}

func TestFilter(t *testing.T) {
	df, err := frame.New(
		series.NewSimple("Fruit", []string{"Apple", "Apple", "Pear"}),
		series.NewSimple("Color", []string{"Red", "Yellow", "Green"}),
	)
	assert.NoError(t, err)
	defer df.Release()

	f, err := df.Filter(context.Background(), chunked.NewSimple("", []bool{false, true, false}))
	assert.NoError(t, err)
	defer f.Release()

	assert.EqualValues(t, 2, f.NumCols())
	assert.EqualValues(t, 1, f.NumRows())

	assert.Equal(t, "Apple", f.Column(0).GetUnchecked(0))
	assert.Equal(t, "Yellow", f.Column(1).GetUnchecked(0))
}

func TestFromCsv(t *testing.T) {
	require.FileExists(t, "../testdata/flights.csv")
	f, _ := os.Open("../testdata/flights.csv")
	defer f.Close()

	df, err := frame.FromCsv(f)
	require.NoError(t, err)
	defer df.Release()

	fmt.Printf("shape(%d, %d)\n", df.NumCols(), df.NumRows())
	ctx := context.Background()
	must := func(s *chunked.Bool, err error) *chunked.Bool {
		require.NoError(t, err)
		return s
	}

	filtered, err := df.Filter(ctx, must(chunked.And(ctx,
		must(df.Col("MONTH").EqualScalar(ctx, 12)),
		must(df.Col("ORIGIN_AIRPORT").EqualScalar(ctx, "SEA")),
		must(df.Col("DESTINATION_AIRPORT").EqualScalar(ctx, "DFW")),
	)))
	require.NoError(t, err)
	defer filtered.Release()

	fmt.Println(filtered)
}

func TestFromParquet(t *testing.T) {
	require.FileExists(t, "../testdata/flights.parquet")
	f, _ := os.Open("../testdata/flights.parquet")
	defer f.Close()

	df, err := frame.FromParquet(context.Background(), f, memory.DefaultAllocator)
	require.NoError(t, err)
	defer df.Release()

	fmt.Printf("shape(%d, %d)\n", df.NumCols(), df.NumRows())
	ctx := context.Background()
	must := func(s *chunked.Bool, err error) *chunked.Bool {
		require.NoError(t, err)
		return s
	}

	filtered, err := df.Filter(ctx, must(chunked.And(ctx,
		must(df.Col("MONTH").EqualScalar(ctx, 12)),
		must(df.Col("ORIGIN_AIRPORT").EqualScalar(ctx, "SEA")),
		must(df.Col("DESTINATION_AIRPORT").EqualScalar(ctx, "DFW")),
	)))
	require.NoError(t, err)
	defer filtered.Release()

	fmt.Printf("shape(%d, %d)\n", filtered.NumCols(), filtered.NumRows())
}
