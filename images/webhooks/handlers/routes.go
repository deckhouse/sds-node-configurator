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
	"fmt"
	"net/http"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-common-lib/logger/kwhlogger"
	cn "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

const (
	LLVSValidatorID = "LLVSValidator"
	LVGValidatorID  = "LVGValidator"
)

func Handlers(log logger.Logger, cl client.Reader) (map[string]http.Handler, error) {
	kwhLog := kwhlogger.New(log)

	llvs, err := GetValidatingWebhookHandler(LLVSValidate, LLVSValidatorID, &cn.LVMLogicalVolumeSnapshot{}, kwhLog)
	if err != nil {
		return nil, fmt.Errorf("creating the LVMLogicalVolumeSnapshot validator: %w", err)
	}
	lvg, err := GetValidatingWebhookHandler(LVGValidate(cl, log.Named("lvg-validate")), LVGValidatorID, &cn.LVMVolumeGroup{}, kwhLog)
	if err != nil {
		return nil, fmt.Errorf("creating the LVMVolumeGroup validator: %w", err)
	}
	return map[string]http.Handler{
		"/llvs-validate": llvs,
		"/lvg-validate":  lvg,
	}, nil
}
