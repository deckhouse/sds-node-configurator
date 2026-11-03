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

package v1alpha1

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// The size fields below are int-or-string in the CRDs, so the API server stores
// `size: 1073741824` as a JSON number. A plain string field cannot take a number,
// and the decoding error is not limited to the object that carries it: a list
// that contains one such object fails as a whole, and with it the informers of
// every component that reads the resource. These decoders keep the field a
// string and take a number as its literal text.

// UnmarshalJSON accepts spec.thinPools[].size as a string or a number.
func (s *LVMVolumeGroupThinPoolSpec) UnmarshalJSON(data []byte) error {
	type plain LVMVolumeGroupThinPoolSpec
	aux := struct {
		*plain
		Size json.RawMessage `json:"size"`
	}{plain: (*plain)(s)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	return setSizeFromJSON(&s.Size, aux.Size)
}

// UnmarshalJSON accepts spec.size as a string or a number.
func (s *LVMLogicalVolumeSpec) UnmarshalJSON(data []byte) error {
	type plain LVMLogicalVolumeSpec
	aux := struct {
		*plain
		Size json.RawMessage `json:"size"`
	}{plain: (*plain)(s)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	return setSizeFromJSON(&s.Size, aux.Size)
}

// setSizeFromJSON leaves size alone when the field is absent or null, as the
// standard decoder does.
func setSizeFromJSON(size *string, raw json.RawMessage) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	if raw[0] == '"' {
		return json.Unmarshal(raw, size)
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return fmt.Errorf("size must be a string or a number: %w", err)
	}
	*size = n.String()
	return nil
}
