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

package lvm

type VolumeGroup interface {
	Name() string
	LogicalVolume(name string) (LogicalVolume, error)
}

type volumeGroup struct {
	name string
	lvs  map[string]logicalVolume
}

func (vg *volumeGroup) Name() string {
	return vg.name
}

func (vg *volumeGroup) LogicalVolume(name string) (LogicalVolume, error) {
	lv, ok := vg.lvs[name]

	if !ok {
		return nil, ErrNotFound
	}

	return &lv, nil
}

func newVolumeGroup(name string) *volumeGroup {
	return &volumeGroup{name: name}
}

func (vg *volumeGroup) newLogicalVolume(name string) *logicalVolume {
	return &logicalVolume{name: name, vg: vg}
}
