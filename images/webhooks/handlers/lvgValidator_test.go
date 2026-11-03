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

package handlers

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/slok/kubewebhook/v2/pkg/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/sds-common-lib/logger"
	cn "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func testLVG(name, node, vg string) *cn.LVMVolumeGroup {
	return &cn.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: cn.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: vg,
			Local:                 cn.LVMVolumeGroupLocalSpec{NodeName: node},
		},
	}
}

func newLVGFakeClient(t *testing.T, funcs *interceptor.Funcs, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := cn.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	b := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...)
	if funcs != nil {
		b = b.WithInterceptorFuncs(*funcs)
	}
	return b.Build()
}

func TestLVGValidate(t *testing.T) {
	existing := testLVG("vg-data-worker-1", "worker-1", "vg-data")

	tests := []struct {
		name        string
		op          model.AdmissionReviewOp
		lvg         *cn.LVMVolumeGroup
		listFails   bool
		wantValid   bool
		wantMessage string
	}{
		{
			name:        "a second resource for the same Volume Group of the node is rejected",
			op:          model.OperationCreate,
			lvg:         testLVG("vg-data-copy", "worker-1", "vg-data"),
			wantValid:   false,
			wantMessage: "already described by the LVMVolumeGroup vg-data-worker-1",
		},
		{
			name:      "the same Volume Group name on another node is allowed",
			op:        model.OperationCreate,
			lvg:       testLVG("vg-data-worker-2", "worker-2", "vg-data"),
			wantValid: true,
		},
		{
			name:      "another Volume Group on the same node is allowed",
			op:        model.OperationCreate,
			lvg:       testLVG("vg-logs-worker-1", "worker-1", "vg-logs"),
			wantValid: true,
		},
		{
			name:      "an update of the existing resource is not checked",
			op:        model.OperationUpdate,
			lvg:       testLVG("vg-data-worker-1", "worker-1", "vg-data"),
			wantValid: true,
		},
		{
			name:      "a failed list lets the request through",
			op:        model.OperationCreate,
			lvg:       testLVG("vg-data-copy", "worker-1", "vg-data"),
			listFails: true,
			wantValid: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var funcs *interceptor.Funcs
			if tc.listFails {
				funcs = &interceptor.Funcs{
					List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
						return errors.New("apiserver unavailable")
					},
				}
			}
			cl := newLVGFakeClient(t, funcs, existing.DeepCopy())

			res, err := LVGValidate(cl, logger.NewNop())(context.Background(), &model.AdmissionReview{Operation: tc.op}, tc.lvg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Valid != tc.wantValid {
				t.Fatalf("Valid = %v, want %v (message %q)", res.Valid, tc.wantValid, res.Message)
			}
			if tc.wantMessage != "" && !strings.Contains(res.Message, tc.wantMessage) {
				t.Fatalf("message %q does not contain %q", res.Message, tc.wantMessage)
			}
		})
	}
}
