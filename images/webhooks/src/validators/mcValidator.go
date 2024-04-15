/*
Copyright 2024 Flant JSC

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

package validators

import (
	"context"
	"encoding/json"
	dhctl "github.com/deckhouse/deckhouse/deckhouse-controller/pkg/apis/deckhouse.io/v1alpha1"
	"github.com/go-logr/logr"
	"k8s.io/api/admission/v1beta1"
	"k8s.io/klog/v2"
	"net/http"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"webhooks/funcs"
)

type patchBoolValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value bool   `json:"value"`
}

type mc struct {
	Spec     mcSpec   `json:"spec"`
	Metadata Metadata `json:"metadata"`
}

type Metadata struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
}

type mcSpec struct {
	Settings mcSettings `json:"settings"`
}

type mcSettings struct {
	EnableThinProvisioning bool `json:"enableThinProvisioning"`
}

func MCValidate(w http.ResponseWriter, r *http.Request) {
	logf.SetLogger(logr.Logger{})

	arReview := v1beta1.AdmissionReview{}
	if err := json.NewDecoder(r.Body).Decode(&arReview); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	} else if arReview.Request == nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	raw := arReview.Request.Object.Raw

	arReview.Response = &v1beta1.AdmissionResponse{
		UID:     arReview.Request.UID,
		Allowed: true,
	}

	mcJson := mc{}
	klog.Infof("Retrieving MC object %v", json.Unmarshal(raw, &mcJson))

	if (mcJson.Metadata.Name == "sds-replicated-volume" || mcJson.Metadata.Name == "sds-local-volume") &&
		mcJson.Spec.Settings.EnableThinProvisioning == true {
		klog.Infof("in module %s enabled thin provisioning", mcJson.Metadata.Name)

		ctx := context.Background()
		cl, err := funcs.NewKubeClient()

		if err != nil {
			klog.Fatal(err.Error())
		}

		objs := dhctl.ModuleConfigList{}

		err = cl.List(ctx, &objs)

		for _, obj := range objs.Items {
			if obj.Name == "sds-node-configurator" {
				if obj.Spec.Settings == nil {
					obj.Spec.Settings = map[string]interface{}{"enableThinProvisioning": true}
				} else {
					obj.Spec.Settings["enableThinProvisioning"] = true
				}
				err = cl.Update(ctx, &obj)

				if err != nil {
					klog.Fatal(err.Error())
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	klog.Infof("Returning answer %v", json.NewEncoder(w).Encode(&arReview))
}
