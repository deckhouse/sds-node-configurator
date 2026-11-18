/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"encoding/binary"
	"slices"
	"testing"
)

const fuzzRangeBytes = 3

func encodeFuzzRanges(ranges ...Range) []byte {
	out := make([]byte, 0, len(ranges)*fuzzRangeBytes)
	for _, r := range ranges {
		out = binary.BigEndian.AppendUint16(out, uint16(r.Start))
		out = append(out, byte(r.Count-1))
	}
	return out
}

func decodeFuzzRanges(data []byte) RangeCover {
	cover := make(RangeCover, 0, len(data)/fuzzRangeBytes)
	for len(data) >= fuzzRangeBytes {
		cover = append(cover, Range{
			Start: int64(binary.BigEndian.Uint16(data)),
			Count: int64(data[2]) + 1,
		})
		data = data[fuzzRangeBytes:]
	}
	return cover
}

func coverRuns(cover RangeCover) (runs RangeCover, overlapped bool) {
	var end int64
	for _, r := range cover {
		end = max(end, r.Start+r.Count)
	}
	covered := make([]bool, end)
	for _, r := range cover {
		for block := r.Start; block < r.Start+r.Count; block++ {
			if covered[block] {
				overlapped = true
			}
			covered[block] = true
		}
	}

	runs = RangeCover{}
	for block := 0; block < len(covered); block++ {
		if !covered[block] {
			continue
		}
		start := block
		for block < len(covered) && covered[block] {
			block++
		}
		runs = append(runs, Range{Start: int64(start), Count: int64(block - start)})
	}
	return runs, overlapped
}

func FuzzRangeCoverMerged(f *testing.F) {
	f.Add([]byte{})
	f.Add(encodeFuzzRanges(Range{Start: 0, Count: 2}, Range{Start: 1, Count: 1}))
	f.Add(encodeFuzzRanges(Range{Start: 0, Count: 2}, Range{Start: 7, Count: 1}))
	f.Add(encodeFuzzRanges(Range{Start: 0, Count: 2}, Range{Start: 2, Count: 1}))
	f.Add(encodeFuzzRanges(Range{Start: 9, Count: 1}, Range{Start: 3, Count: 3}, Range{Start: 6, Count: 3}, Range{Start: 0, Count: 1}))
	f.Add(encodeFuzzRanges(Range{Start: 65535, Count: 256}, Range{Start: 0, Count: 256}, Range{Start: 256, Count: 1}))

	f.Fuzz(func(t *testing.T, data []byte) {
		cover := decodeFuzzRanges(data)
		wantRuns, overlapped := coverRuns(cover)

		merged, err := slices.Clone(cover).Merged()
		if overlapped {
			if err == nil {
				t.Fatalf("Merged(%v) = %v, want the self overlapped range error", cover, merged)
			}
			return
		}
		if err != nil {
			t.Fatalf("Merged(%v) failed on ranges that do not overlap: %v", cover, err)
		}

		if !slices.Equal(merged, wantRuns) {
			t.Fatalf("Merged(%v) = %v, want the maximal runs %v", cover, merged, wantRuns)
		}

		var inTotal, outTotal int64
		for _, r := range cover {
			inTotal += r.Count
		}
		for i, r := range merged {
			outTotal += r.Count
			if r.Count <= 0 {
				t.Fatalf("Merged(%v) = %v: range %d is empty", cover, merged, i)
			}
			if i > 0 && merged[i-1].Start+merged[i-1].Count >= r.Start {
				t.Fatalf("Merged(%v) = %v: ranges %d and %d are not sorted, overlap or touch", cover, merged, i-1, i)
			}
		}
		if inTotal != outTotal {
			t.Fatalf("Merged(%v) = %v covers %d blocks, want %d", cover, merged, outTotal, inTotal)
		}

		again, err := slices.Clone(merged).Merged()
		if err != nil || !slices.Equal(again, merged) {
			t.Fatalf("Merged is not idempotent on %v: got %v, %v", merged, again, err)
		}
	})
}
