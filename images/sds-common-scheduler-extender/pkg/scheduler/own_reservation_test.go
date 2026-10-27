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

package scheduler

// A request replaces its own reservation, so the space check must not count it: a pool
// with free space between the size and twice the size would be rejected, and the score
// would subtract the volume's own size twice.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
)

// A 100Gi pool with 15Gi free and a 10Gi local PVC.
func ownReservationSetup(t *testing.T) (*scheduler, *cache.Cache, map[string]*corev1.PersistentVolumeClaim,
	map[string]*storagev1.StorageClass, map[string]PVCRequest) {
	t.Helper()
	sc := testLocalSC("sc-local", "lvg1")
	pvc := pendingPVCWithSize("pvc", "default", sc.Name, 10*oneGiB)
	cl := newFakeClient(sc, pvc, readyLVGOnNode("lvg1", "node-1", hundredGiB, 15*oneGiB))
	c := newTestCache()
	return newTestScheduler(cl, c), c,
		map[string]*corev1.PersistentVolumeClaim{pvc.Name: pvc},
		map[string]*storagev1.StorageClass{sc.Name: sc},
		map[string]PVCRequest{pvc.Name: {DeviceType: consts.Thick, RequestedSize: 10 * oneGiB}}
}

func TestOwnReservation_FilterRetryKeepsNode(t *testing.T) {
	log, _ := logger.NewLogger("0")
	pool := []cache.StoragePoolKey{{LVGName: "lvg1"}}
	nodes := map[string]*corev1.Node{"node-1": {ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}

	for _, tc := range []struct {
		name          string
		reservationID string
		wantNodes     []string
	}{
		// The previous scheduling cycle of the same pod reserved 10Gi: 15Gi free is still enough.
		{"own reservation", "default/pvc", []string{"node-1"}},
		// Another PVC's 10Gi leaves 5Gi.
		{"other reservation", "default/other", []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c, pvcs, scs, reqs := ownReservationSetup(t)
			c.AddReservation(tc.reservationID, time.Minute, 10*oneGiB, pool)
			nodeNames := []string{"node-1"}
			res, err := filterNodes(context.Background(), log, s.client, c, &nodeNames, nodes, pvcs, scs, reqs)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.wantNodes, *res.NodeNames)
		})
	}
}

// Free space left after the request: (15-10)/100 = 5% -> round(log2(5)) = 2.
// Counting the own reservation too gives -5% -> 1.
func TestOwnReservation_ScoreSubtractsRequestOnce(t *testing.T) {
	log, _ := logger.NewLogger("0")
	s, c, pvcs, scs, reqs := ownReservationSetup(t)
	c.AddReservation("default/pvc", time.Minute, 10*oneGiB, []cache.StoragePoolKey{{LVGName: "lvg1"}})

	nodeNames := []string{"node-1"}
	scores, err := scoreNodes(context.Background(), log, s.client, c, &nodeNames, pvcs, scs, reqs, map[string][]string{}, 1.0)
	require.NoError(t, err)
	require.Len(t, scores, 1)
	require.Equal(t, 2, scores[0].Score)
}

func TestOwnReservation_FilterAndScoreRetry(t *testing.T) {
	cl := newFakeClient(readyLVG("lvg1", hundredGiB, 15*oneGiB))
	c := newTestCache()
	s := newTestScheduler(cl, c)
	body := `{"reservationID":"ns/pvc","reservationTTL":"60s","size":10737418240,"lvgs":[{"name":"lvg1"}]}`

	for i := range 2 {
		w := httptest.NewRecorder()
		s.filterAndScore(w, httptest.NewRequest(http.MethodPost, "/v1/lvg/filter-and-score", bytes.NewReader([]byte(body))))
		require.Equal(t, http.StatusOK, w.Code)
		var resp FilterAndScoreResponse
		require.NoError(t, decodeJSON(w.Body.Bytes(), &resp))
		require.Len(t, resp.LVGS, 1, "call %d", i+1)
		require.Equal(t, 2, resp.LVGS[0].Score, "call %d", i+1)
	}
	require.Equal(t, 10*oneGiB, c.GetReservedSpace(cache.StoragePoolKey{LVGName: "lvg1"}))
}
