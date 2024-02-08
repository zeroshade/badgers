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

package series_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/apache/badgers"
	"github.com/apache/badgers/chunked"
	"github.com/apache/badgers/series"
	"github.com/stretchr/testify/assert"
)

func TestBasicSeries(t *testing.T) {
	s := series.NewSimple("a", []int32{1, 2, 3})
	defer s.Release()

	assert.Equal(t, "a", s.Name())
	assert.Equal(t, `shape: (3,)
Series: 'a' [int32]
[
	1
	2
	3
]`, s.String())

	s2 := series.NewNullable("b", []*float32{series.Optional[float32](1), nil, series.Optional[float32](2)})
	defer s2.Release()

	assert.Equal(t, "b", s2.Name())
	assert.Equal(t, `shape: (3,)
Series: 'b' [float32]
[
	1
	(null)
	2
]`, s2.String())
}

func TestFilterSeries(t *testing.T) {
	s := series.NewSimple("a", []int32{1, 2, 3})
	defer s.Release()

	b, err := s.Filter(context.Background(),
		chunked.NewSimple("filter", []bool{true, false, false}))
	assert.NoError(t, err)
	defer b.Release()

	assert.EqualValues(t, 3, s.Len())
	assert.EqualValues(t, 1, b.Len())
	assert.Equal(t, int32(1), b.GetUnchecked(0))
}

func TestSeriesAppend(t *testing.T) {
	s1 := series.NewSimple("a", []int32{1, 2})
	s2 := series.NewSimple("b", []int32{3})

	defer s1.Release()
	defer s2.Release()

	assert.EqualValues(t, 2, s1.Len())
	assert.NoError(t, s1.Append(s2))
	assert.EqualValues(t, 3, s1.Len())

	s3 := series.NewSimple("c", []float32{3.0})
	defer s3.Release()

	assert.Error(t, s1.Append(s3))
}

func TestSeriesCompareScalar(t *testing.T) {
	s1 := series.NewSimple("a", []int16{1, 2, 3, 4, 5})
	defer s1.Release()

	s2, err := s1.LessScalar(context.Background(), 3)
	assert.NoError(t, err)
	defer s2.Release()

	for i := 0; i < 5; i++ {
		assert.Equal(t, i < 2, s2.GetUnchecked(uint(i)))
	}

	s2, err = s1.LessEqScalar(context.Background(), 3)
	assert.NoError(t, err)
	defer s2.Release()

	for i := 0; i < 5; i++ {
		assert.Equal(t, i <= 2, s2.GetUnchecked(uint(i)))
	}

	s3, err := s1.GreaterScalar(context.Background(), 3)
	assert.NoError(t, err)
	defer s3.Release()

	for i := 0; i < 5; i++ {
		assert.Equal(t, i > 2, s3.GetUnchecked(uint(i)))
	}

	s3, err = s1.GreaterEqScalar(context.Background(), 3)
	assert.NoError(t, err)
	defer s3.Release()

	for i := 0; i < 5; i++ {
		assert.Equal(t, i >= 2, s3.GetUnchecked(uint(i)))
	}

	s4, err := s1.EqualScalar(context.Background(), 3)
	assert.NoError(t, err)
	defer s4.Release()

	for i := 0; i < 5; i++ {
		assert.Equal(t, i == 2, s4.GetUnchecked(uint(i)))
	}

	s4, err = s1.NotEqualScalar(context.Background(), 3)
	assert.NoError(t, err)
	defer s4.Release()

	for i := 0; i < 5; i++ {
		assert.Equal(t, i != 2, s4.GetUnchecked(uint(i)))
	}
}

func TestSeriesSlice(t *testing.T) {
	s := series.NewSimple("a", []int64{1, 2, 3, 4, 5})
	defer s.Release()

	slice1 := s.Slice(-3, 3)
	defer slice1.Release()

	slice2 := s.Slice(-5, 5)
	defer slice2.Release()

	slice3 := s.Slice(0, 5)
	defer slice3.Release()

	v, err := slice1.Get(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), v)

	v, err = slice2.Get(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), v)

	v, err = slice3.Get(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), v)
}

func TestOutOfRangeSlice(t *testing.T) {
	s := series.NewSimple("a", []int64{1, 2, 3, 4, 5})
	defer s.Release()

	assert.NotPanics(t, func() {
		_ = s.Slice(-3, 4)
	})
	assert.NotPanics(t, func() {
		_ = s.Slice(-6, 2)
	})
	assert.NotPanics(t, func() {
		_ = s.Slice(4, 2)
	})
}

func generateInts(r *rand.Rand, n int) (data []int32) {
	for i := 0; i < n; i++ {
		data = append(data, r.Int31())
	}
	return
}

func generateFloats(r *rand.Rand, n int) (data []float64) {
	for i := 0; i < n; i++ {
		data = append(data, rand.NormFloat64())
	}
	return
}

func benchNew[T badgers.PrimitiveType](b *testing.B, n string, data []T) {
	var s *series.Series
	for i := 0; i < b.N; i++ {
		s = series.NewSimple(n, data)
	}
	b.Cleanup(func() {
		s.Rename("foo")
	})
}

func BenchmarkSeries_New(b *testing.B) {
	r := rand.New(rand.NewSource(1337))
	tests := []struct {
		name string
		data interface{}
	}{
		{
			"[]int32(10000)",
			generateInts(r, 10000),
		},
		{
			"[]float64(10000)",
			generateFloats(r, 10000),
		},
	}

	for _, test := range tests {
		switch d := test.data.(type) {
		case []int32:
			b.Run(test.name, func(b *testing.B) {
				benchNew(b, test.name, d)
			})
		case []float64:
			b.Run(test.name, func(b *testing.B) {
				benchNew(b, test.name, d)
			})
		}
	}
}
