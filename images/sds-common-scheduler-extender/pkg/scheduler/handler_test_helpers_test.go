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

package scheduler

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func init() {
	_ = snc.AddToScheme(scheme.Scheme)
}

func newTestScheduler(cl client.Client, c *cache.Cache) *scheduler {
	log, _ := logger.NewLogger("0")
	return &scheduler{
		client:         cl,
		ctx:            context.Background(),
		log:            log,
		cache:          c,
		defaultDivisor: 1.0,
	}
}

func readyLVG(name string, vgSize, vgFree int64) *snc.LVMVolumeGroup {
	return &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: snc.LVMVolumeGroupStatus{
			Phase:  snc.PhaseReady,
			VGSize: *resource.NewQuantity(vgSize, resource.BinarySI),
			VGFree: *resource.NewQuantity(vgFree, resource.BinarySI),
		},
	}
}

func readyLVGWithThinPool(name string, vgSize int64, tpName string, tpAvailable int64) *snc.LVMVolumeGroup {
	return &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: snc.LVMVolumeGroupStatus{
			Phase:  snc.PhaseReady,
			VGSize: *resource.NewQuantity(vgSize, resource.BinarySI),
			VGFree: *resource.NewQuantity(0, resource.BinarySI),
			ThinPools: []snc.LVMVolumeGroupThinPoolStatus{
				{
					Name:           tpName,
					AvailableSpace: *resource.NewQuantity(tpAvailable, resource.BinarySI),
				},
			},
		},
	}
}

func notReadyLVG(name string, phase string) *snc.LVMVolumeGroup {
	return &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: snc.LVMVolumeGroupStatus{
			Phase:  phase,
			VGSize: *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI),
			VGFree: *resource.NewQuantity(50*1024*1024*1024, resource.BinarySI),
		},
	}
}

func newTestCache() *cache.Cache {
	log, _ := logger.NewLogger("0")
	return cache.NewCache(log, time.Hour)
}

func newFakeClient(objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objects...).
		Build()
}
