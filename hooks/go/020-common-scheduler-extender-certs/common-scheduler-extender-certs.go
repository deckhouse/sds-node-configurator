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

package hooks_common

import (
	"fmt"

	tlscertificate "github.com/deckhouse/module-sdk/common-hooks/tls-certificate"
	consts "github.com/deckhouse/sds-node-configurator/hooks/go/consts"
)

var _ = tlscertificate.RegisterInternalTLSHookEM(tlscertificate.GenSelfSignedTLSHookConf{
	CommonCACanonicalName: fmt.Sprintf("%s-%s", consts.ModulePluralName, consts.SdsCommonSchedulerExtenderCertCn),
	CN:                    consts.SdsCommonSchedulerExtenderCertCn,
	TLSSecretName:         consts.CommonSchedulerExtenderSecretName,
	Namespace:             consts.ModuleNamespace,
	SANs: tlscertificate.DefaultSANs([]string{
		"localhost",
		"127.0.0.1",
		consts.SdsCommonSchedulerExtenderCertCn,
		fmt.Sprintf("%s.%s", consts.SdsCommonSchedulerExtenderCertCn, consts.ModuleNamespace),
		fmt.Sprintf("%s.%s.svc", consts.SdsCommonSchedulerExtenderCertCn, consts.ModuleNamespace),
		// %CLUSTER_DOMAIN%:// is a special value to generate SAN like 'svc_name.svc_namespace.svc.cluster.local'
		fmt.Sprintf("%%CLUSTER_DOMAIN%%://%s.%s.svc", consts.SdsCommonSchedulerExtenderCertCn, consts.ModuleNamespace),
	}),
	FullValuesPathPrefix: fmt.Sprintf("%s.internal.customSchedulerExtenderCert", consts.ModuleName),
})
