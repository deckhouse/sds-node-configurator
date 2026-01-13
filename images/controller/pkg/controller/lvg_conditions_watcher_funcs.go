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

package controller

import (
	"context"

	"gopkg.in/yaml.v3"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func getTargetConditionsCount(lvgCrd *v1.CustomResourceDefinition) (int, error) {
	type item struct {
		Type       string `json:"type"`
		Properties struct {
			LastTransitionTime struct {
				Type string `json:"type"`
			} `json:"lastTransitionTime"`
			Message struct {
				Type string `json:"type"`
			} `json:"message"`
			ObservedGeneration struct {
				Type string `json:"type"`
			} `json:"observedGeneration"`
			Reason struct {
				Type string `json:"type"`
			} `json:"reason"`
			Status struct {
				Type string `json:"type"`
			} `json:"status"`
			Type struct {
				Type string   `json:"type"`
				Enum []string `json:"enum"`
			} `json:"type"`
		} `json:"properties"`
	}
	i := item{}
	json, err := lvgCrd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["status"].Properties["conditions"].Items.MarshalJSON()
	if err != nil {
		return 0, err
	}

	err = yaml.Unmarshal(json, &i)
	if err != nil {
		return 0, err
	}

	return len(i.Properties.Type.Enum), nil
}

func getCRD(ctx context.Context, cl client.Client, crdName string) (*v1.CustomResourceDefinition, error) {
	crd := &v1.CustomResourceDefinition{}
	err := cl.Get(ctx, client.ObjectKey{
		Name: crdName,
	}, crd)

	return crd, err
}

func updateLVMVolumeGroupPhaseIfNeeded(ctx context.Context, cl client.Client, lvg *v1alpha1.LVMVolumeGroup, phase string) error {
	if lvg.Status.Phase == phase {
		return nil
	}

	lvg.Status.Phase = phase

	return cl.Status().Update(ctx, lvg)
}
