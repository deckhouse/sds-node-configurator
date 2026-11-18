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
	"bytes"
	"encoding/json"
	"testing"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

const fuzzLVMAdvisory = "  Consider pruning vg-1 VG archive with more then 1032 MiB in 11272 files (check archiving is needed in lvm.conf).\n"

var lvmReportSeeds = []string{
	``,
	`{}`,
	`{"report":[]}`,
	`{"report":{}}`,
	`{"report":[{"pv":[{"pv_name":"/dev/vdb", "vg_name":"vgtest", "pv_fmt":"lvm2", "pv_attr":"a--", "pv_size":"10G",
"pv_free":"1020.00m", "pv_used":"0 ", "pv_uuid":"BmuLLu-9ZSf-eqpf-qR3H-23rQ-fIl7-Ouyl5X", "vg_tags":"",
"vg_uuid":"JnCFQZ-TTfE-Ed2C-nKoH-yzPH-4fMA-CKwIv7"}]}]}`,
	`{"report":[{"vg":[{"vg_name":"test-vg", "pv_count":"1", "lv_count":"0", "snap_count":"0", "vg_attr":"wz--n-",
"vg_size":"2G", "vg_free":"2G", "vg_uuid":"P14t8J-nfUE-hryT-LiTv-JdFD-Wqxg-R8taCa",
"vg_tags":"test-tag", "vg_shared":"test-shared"}]}]}`,
	`{"report":[{"vg":[{"vg_size":"<2.00g"}]}]}`,
	`  {
      "report": [
  Consider pruning ceph-vg VG archive with more then 12 MiB in 8929 files (check archiving is needed in lvm.conf).
          {
              "vg": [
                  {"vg_name":"vg-1", "pv_count":"2", "lv_count":"329", "snap_count":"0", "vg_attr":"wz--n-", "vg_size":"11522252210176", "vg_free":"1322367582208", "vg_uuid":"GzKqS7-TW1a-W21I-yZK2-FlTu-WC7c-Iz24OW", "vg_tags":"storage.deckhouse.io/enabled=true", "vg_shared":"", "vg_extent_size":"4194304"}
              ]
          }
      ]
  }
`,
	`{"report":[{"lv":[{"lv_name":"thin-1", "vg_name":"vg-1", "lv_attr":"twi-a-tz--", "lv_size":"104857600", "pool_lv":"", "origin":"", "data_percent":"0.00", "metadata_percent":"10.84", "move_pv":"", "mirror_log":"", "copy_percent":"", "convert_lv":"", "vg_uuid":"QVh4uj-O6Wa-6TT8-XdU7-xCQu-M4gR-x9IY36", "lv_tags":"", "thin_id":"", "metadata_lv":"[thin-1_tmeta]", "lv_dm_path":"/dev/mapper/vg--1-thin--1"}]}],
      "log": [
      ]
  }
`,
	"\x00\x01 not a report at all",
}

func fuzzLVMReport[T any](f *testing.F, unmarshal func([]byte) ([]T, error), report func([]T) any) {
	for _, seed := range lvmReportSeeds {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, out []byte) {
		rows, err := unmarshal(out)

		at := bytes.IndexByte(out, '\n') + 1
		polluted := append(append(append([]byte{}, out[:at]...), fuzzLVMAdvisory...), out[at:]...)
		pollutedRows, pollutedErr := unmarshal(polluted)
		if (err == nil) != (pollutedErr == nil) {
			t.Fatalf("an advisory line changed the outcome: %v without it, %v with it", err, pollutedErr)
		}
		if err != nil {
			return
		}

		canonical, err := json.Marshal(report(rows))
		if err != nil {
			t.Fatalf("marshaling %v: %v", rows, err)
		}
		pollutedCanonical, err := json.Marshal(report(pollutedRows))
		if err != nil {
			t.Fatalf("marshaling %v: %v", pollutedRows, err)
		}
		if !bytes.Equal(canonical, pollutedCanonical) {
			t.Fatalf("an advisory line changed the rows: %s without it, %s with it", canonical, pollutedCanonical)
		}

		again, err := unmarshal(canonical)
		if err != nil {
			t.Fatalf("the report re-encoded from the parsed rows does not parse: %v; report: %s", err, canonical)
		}
		if len(again) != len(rows) {
			t.Fatalf("got %d rows back from the re-encoded report, want %d", len(again), len(rows))
		}
		againCanonical, err := json.Marshal(report(again))
		if err != nil {
			t.Fatalf("marshaling %v: %v", again, err)
		}
		if !bytes.Equal(canonical, againCanonical) {
			t.Fatalf("the rows changed on a round trip: %s, then %s", canonical, againCanonical)
		}
	})
}

func FuzzUnmarshalPVs(f *testing.F) {
	fuzzLVMReport(f, unmarshalPVs, func(rows []internal.PVData) any {
		return internal.PVReport{Report: []internal.PV{{PV: rows}}}
	})
}

func FuzzUnmarshalVGs(f *testing.F) {
	fuzzLVMReport(f, unmarshalVGs, func(rows []internal.VGData) any {
		return internal.VGReport{Report: []internal.VG{{VG: rows}}}
	})
}

func FuzzUnmarshalLVs(f *testing.F) {
	fuzzLVMReport(f, unmarshalLVs, func(rows []internal.LVData) any {
		return internal.LVReport{Report: []internal.LV{{LV: rows}}}
	})
}
