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
	"math/bits"
	"unsafe"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/bitutil"
	"github.com/apache/badgers"
	"golang.org/x/exp/constraints"
)

func bytesToUint64(b []byte) []uint64 {
	if len(b) < 8 {
		return nil
	}

	ptr := unsafe.Pointer(unsafe.SliceData(b))
	return unsafe.Slice((*uint64)(ptr), len(b)/8)
}

func findFirstNotNull(buf []byte, offset, n int64) int64 {
	if offset > 0 {
		return findFirstNotNullWithOffset(buf, offset, n)
	}

	start, uint64Bytes := int64(0), n/64
	for _, v := range bytesToUint64(buf[:uint64Bytes]) {
		if v == 0 {
			start += 64
		} else {
			return start + int64(bits.LeadingZeros64(v))
		}
	}

	for _, v := range buf[uint64Bytes : n/8] {
		if v == 0 {
			start += 8
		} else {
			return start + int64(bits.LeadingZeros8(v))
		}
	}

	for i := n &^ 0x7; i < n; i++ {
		if bitutil.BitIsSet(buf, int(i)) {
			return i
		}
	}

	return -1
}

func roundUp(v, f int64) int64 {
	return (v + (f - 1)) / f * f
}

func findFirstNotNullWithOffset(buf []byte, offset, n int64) int64 {
	beg, end := offset, offset+n

	begU8 := roundUp(beg, 64)
	init := min(n, begU8-beg)
	for i := offset; i < beg+init; i++ {
		if bitutil.BitIsSet(buf, int(i)) {
			return i - beg
		}
	}

	start := init
	nU64 := (n - init) / 64
	begU64 := begU8 / 64
	endU64 := begU64 + nU64
	bufU64 := bytesToUint64(buf)
	if begU64 < int64(len(bufU64)) {
		for _, v := range bufU64[begU64:endU64] {
			if v == 0 {
				start += 64
			} else {
				return start + int64(bits.LeadingZeros64(v))
			}
		}
	}

	tail := beg + init + nU64*64
	for i := tail; i < end; i++ {
		if bitutil.BitIsSet(buf, int(i)) {
			return i - offset
		}
	}

	return -1
}

func indexToChunkedIndex[I []T, T constraints.Integer](chunkLens I, idx T) (current, remainder T) {
	remainder = idx
	for _, l := range chunkLens {
		if l > remainder {
			break
		}

		remainder -= l
		current++
	}
	return
}

func sliceOffsets(offset int64, length, arrayLen uint64) (uint64, uint64) {
	// offset counts from the start of the array
	// or from the end of the array if negative
	if offset < 0 {
		absOffset := uint64(-offset)
		if absOffset <= arrayLen {
			return arrayLen - absOffset, min(length, absOffset)
		}
		return 0, min(length, arrayLen)
	}
	absOffset := uint64(offset)
	if absOffset <= arrayLen {
		return absOffset, min(length, arrayLen-absOffset)
	}

	return arrayLen, 0
}

func slice(chunks []arrow.Array, offset int64, sliceLength, ownLength uint64) ([]arrow.Array, uint64) {
	rawOffset, sliceLen := sliceOffsets(offset, sliceLength, ownLength)

	remainingLength, remainingOffset := sliceLen, rawOffset
	newLen := uint64(0)

	newChunks := make([]arrow.Array, 0, 1)
	for _, c := range chunks {
		chunkLen := uint64(c.Len())
		if remainingOffset > 0 && remainingOffset >= chunkLen {
			remainingOffset -= chunkLen
			continue
		}

		var takeLen uint64
		if remainingLength+remainingOffset > chunkLen {
			takeLen = chunkLen - remainingOffset
		} else {
			takeLen = remainingLength
		}

		newLen += takeLen

		newChunks = append(newChunks, array.NewSlice(c, int64(remainingOffset), int64(remainingOffset+takeLen)))
		remainingLength -= takeLen
		remainingOffset = 0

		if remainingLength == 0 {
			break
		}
	}

	if len(newChunks) == 0 {
		newChunks = append(newChunks, array.NewSlice(chunks[0], 0, 0))
	}
	return newChunks, newLen
}

func asTyped[T badgers.SimpleType | []byte](chunks []arrow.Array) []typedArray[T] {
	out := make([]typedArray[T], len(chunks))
	for i, c := range chunks {
		out[i] = c.(typedArray[T])
	}
	return out
}
