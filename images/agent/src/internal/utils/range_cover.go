/*
Copyright 2023 Flant JSC

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
	"fmt"
	"sort"
)

type Range struct {
	Start, Count int64
}

type RangeCover []Range

func (rc RangeCover) Merged() (RangeCover, error) {
	rcLen := len(rc)
	if rcLen <= 1 {
		return rc, nil
	}

	sort.Slice(rc, func(i, j int) bool {
		return rc[i].Start < rc[j].Start
	})

	for i, d := range rc[0 : rcLen-1] {
		if d.Start+d.Count > rc[i+1].Start {
			return nil, fmt.Errorf("self overlapped range")
		}
	}

	last := Range{Start: 0, Count: 0}
	reduced := make(RangeCover, 0, len(rc))
	for _, d := range rc {
		if last.Count == 0 {
			last = d
		} else if d.Start == last.Start+last.Count {
			last.Count += d.Count
		} else {
			reduced = append(reduced, last)
			last = d
		}
	}

	if last.Count > 0 {
		reduced = append(reduced, last)
	}

	return reduced, nil
}

func (value Range) Multiplied(multiplier int64) Range {
	return Range{Start: value.Start * multiplier, Count: value.Count * multiplier}
}

func (cover RangeCover) Multiplied(multiplier int64) RangeCover {
	multiplied := make([]Range, len(cover))
	for i, value := range cover {
		multiplied[i] = value.Multiplied(multiplier)
	}
	return multiplied
}
