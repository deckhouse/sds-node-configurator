/*
Copyright 2025 Flant JSC

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

func (cover RangeCover) Merged() (RangeCover, error) {
	rcLen := len(cover)
	if rcLen <= 1 {
		return cover, nil
	}

	sort.Slice(cover, func(i, j int) bool {
		return cover[i].Start < cover[j].Start
	})

	for i, d := range cover[0 : rcLen-1] {
		if d.Start+d.Count > cover[i+1].Start {
			return nil, fmt.Errorf("self overlapped range")
		}
	}

	last := Range{Start: 0, Count: 0}
	reduced := make(RangeCover, 0, len(cover))
	for _, item := range cover {
		switch last.Count {
		case 0: // Special case for first range
			last = item
		case item.Start - last.Start: // Touching, merge to one range
			last.Count += item.Count
		default: // Add previously merged ranges to cover
			reduced = append(reduced, last)
			last = item
		}
	}

	if last.Count > 0 {
		reduced = append(reduced, last)
	}

	return reduced, nil
}

func (cover Range) Multiplied(multiplier int64) Range {
	return Range{Start: cover.Start * multiplier, Count: cover.Count * multiplier}
}

func (cover RangeCover) Multiplied(multiplier int64) RangeCover {
	multiplied := make([]Range, len(cover))
	for i, value := range cover {
		multiplied[i] = value.Multiplied(multiplier)
	}
	return multiplied
}
