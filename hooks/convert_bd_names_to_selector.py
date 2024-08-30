#!/usr/bin/env python3
#
# Copyright 2024 Flant JSC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, List

from deckhouse import hook

import kubernetes

config = """
configVersion: v1
onStartup: 5
"""

group = 'storage.deckhouse.io'
plural = 'lvmvolumegroups'
version = 'v1alpha1'


# This webhook ensures the migration of LVMVolumeGroup resources from the old CRD version to the new one:
# - Removes field spec.blockDeviceNames
# - Adds spec.Local field and fills its value 'nodeName' with the resource's node.
# - Adds spec.blockDeviceSelector field and fills it with the LVMVolumeGroup nodeName and blockDeviceNames

# add some

def main(ctx: hook.Context):
    kubernetes.config.load_incluster_config()

    lvg_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group=group,
                                                                                    plural=plural,
                                                                                    version=version)

    for lvg in lvg_list['items']:
        print(f"lvg {lvg['metadata']['name']} is going to be updated")
        print(f"full lvg {lvg}")
        bd_names: List[str] = lvg['spec']['blockDeviceNames']
        print(f"blockDeviceNames: {bd_names}")
        del lvg['spec']['blockDeviceNames']
        lvg['spec']['local']['nodeName'] = lvg['status']['nodes'][0]['name']
        lvg['spec']['blockDeviceSelector']['matchLabels']['kubernetes.io/hostname'] = lvg['spec']['local']['nodeName']
        lvg['spec']['blockDeviceSelector']['matchExpressions'][0]['key'] = 'kubernetes.io/metadata.name'
        lvg['spec']['blockDeviceSelector']['matchExpressions'][0]['operator'] = 'in'
        lvg['spec']['blockDeviceSelector']['matchExpressions'][0]['values'] = bd_names

        kubernetes.client.CustomObjectsApi.patch_cluster_custom_object(group=group, plural=plural, version=version)


if __name__ == "__main__":
    hook.run(main, config=config)
