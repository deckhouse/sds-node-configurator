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
import os
from typing import Any, List

import yaml
from deckhouse import hook

import kubernetes

config = """
configVersion: v1
onStartup: 10
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
    print("starts to migrate LVMVolumeGroups")
    kubernetes.config.load_incluster_config()

    # get current lvgs (which are LvmVolumeGroup)
    lvg_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group=group,
                                                                                    plural=plural,
                                                                                    version=version)

    # we create backup crd for lvmvolumegroups
    ctx.kubernetes.delete(kind='CustomResourceDefinition', apiVersion='apiextensions.k8s.io/v1', name='LvmVolumeGroup')
    for dirpath, _, filenames in os.walk(top=find_crds_root(__file__)):
        for filename in filenames:
            if filename == 'lvmvolumegroupbackup.yaml':
                crd_path = os.path.join(dirpath, filename)
                with open(crd_path, "r", encoding="utf-8") as f:
                    for manifest in yaml.safe_load_all(f):
                        if manifest is None:
                            continue
                        ctx.kubernetes.create(manifest)
                        print(f"{filename} was successfully created")
                break

    for lvg in lvg_list.get('items', []):
        lvg['kind'] = 'LVMVolumeGroupBackup'
        ctx.kubernetes.create(lvg)
        print(f"{lvg['metadata']['name']} backup was created")


    # TODO: запатчить kind у ресурсов с LvmVolumegroup -> LVMVolumeGroup
    # пересоздать CRD LVMVolumeGroup
    # for lvg in lvg_list.get('items', []):
    #     lvg_name = lvg['metadata']['name']
    #     print(f"LVMVolumeGroup {lvg_name} is going to be updated")
    #     print(f"LVMVolumeGroup {lvg_name} spec before update: {lvg['spec']}")
    #     bd_names: List[str] = lvg['spec']['blockDeviceNames']
    #     if len(bd_names) > 0:
    #         print(f"extracted BlockDevice names: {bd_names} from LVMVolumeGroup {lvg_name}")
    #         del lvg['spec']['blockDeviceNames']
    #         lvg['spec']['local'] = {'nodeName': lvg['status']['nodes'][0]['name']}
    #         print(f"LVMVolumeGroup {lvg_name} spec after adding the Local field: {lvg['spec']}")
    #         lvg['spec']['blockDeviceSelector'] = {
    #             'matchLabels': {'kubernetes.io/hostname': lvg['spec']['local']['nodeName']},
    #             'matchExpressions': [
    #                 {'key': 'kubernetes.io/metadata.name', 'operator': 'in', 'values': bd_names}]
    #         }
    #         print(f"LVMVolumeGroup {lvg_name} spec after adding the Selector: {lvg['spec']}")
    #
    #         ctx.kubernetes.create_or_update(lvg)

            # kubernetes.client.CustomObjectsApi().replace_cluster_custom_object(group=group,
            #                                                                    plural=plural,
            #                                                                    version=version,
            #                                                                    name=lvg_name,
            #                                                                    body=lvg)
            # print(f"LVMVolumeGroup {lvg_name} was successfully updated")



def find_crds_root(hookpath):
    hooks_root = os.path.dirname(hookpath)
    module_root = os.path.dirname(hooks_root)
    crds_root = os.path.join(module_root, "crds")
    return crds_root

if __name__ == "__main__":
    hook.run(main, config=config)
