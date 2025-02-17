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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLVGConditionsWatcher(t *testing.T) {
	cl := NewFakeClient()
	ctx := context.Background()

	t.Run("getCRD", func(t *testing.T) {
		targetName := "target"
		crds := []v1.CustomResourceDefinition{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: targetName,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "other-name",
				},
			},
		}

		for _, crd := range crds {
			err := cl.Create(ctx, &crd)
			if err != nil {
				t.Error(err)
			}
		}

		crd, err := getCRD(ctx, cl, targetName)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, targetName, crd.Name)
	})

	t.Run("getTargetConditionsCount", func(t *testing.T) {
		first, err := json.Marshal("first")
		if err != nil {
			t.Error(err)
		}
		second, err := json.Marshal("second")
		if err != nil {
			t.Error(err)
		}
		third, err := json.Marshal("third")
		if err != nil {
			t.Error(err)
		}
		crd := &v1.CustomResourceDefinition{
			Spec: v1.CustomResourceDefinitionSpec{
				Versions: []v1.CustomResourceDefinitionVersion{
					{
						Schema: &v1.CustomResourceValidation{
							OpenAPIV3Schema: &v1.JSONSchemaProps{
								Properties: map[string]v1.JSONSchemaProps{
									"status": {
										Properties: map[string]v1.JSONSchemaProps{
											"conditions": {
												Items: &v1.JSONSchemaPropsOrArray{
													Schema: &v1.JSONSchemaProps{
														Properties: map[string]v1.JSONSchemaProps{
															"type": {
																Enum: []v1.JSON{
																	{
																		Raw: first,
																	},
																	{
																		Raw: second,
																	},
																	{
																		Raw: third,
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		count, err := getTargetConditionsCount(crd)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, 3, count)
	})
}
