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
import time
from typing import Any, List

import yaml
from deckhouse import hook

import kubernetes

config = """
configVersion: v1
onStartup: 1
"""
migrate_script = '[lvg_migration]'

group = 'storage.deckhouse.io'
lvmvolumegroup_plural = 'lvmvolumegroups'
version = 'v1alpha1'


# This webhook ensures the migration of LVMVolumeGroup resources from the old CRD version to the new one:
# - Removes field spec.blockDeviceNames
# - Adds spec.Local field and fills its value 'nodeName' with the resource's node.
# - Adds spec.blockDeviceSelector field and fills it with the LVMVolumeGroup nodeName and blockDeviceNames

# add some

def main(ctx: hook.Context):
    print(f"{migrate_script} starts to migrate LvmVolumeGroup kind to LVMVolumeGroup")
    kubernetes.config.load_incluster_config()

    ds_name = 'sds-node-configurator'
    ds_ns = 'd8-sds-node-configurator'
    print(f"{migrate_script} tries to scale down the sds-node-configurator daemon set")
    daemonset = kubernetes.client.AppsV1Api().read_namespaced_daemon_set(name=ds_name, namespace=ds_ns)
    daemonset.spec.template.spec.node_selector = {'exclude': 'true'}
    kubernetes.client.AppsV1Api().patch_namespaced_daemon_set(name=ds_name, namespace=ds_ns, body=daemonset)
    print(f"{migrate_script} daemon set has been successfully scaled down")




    # get current lvgs with old CRD (which is LvmVolumeGroup)
    # try:
    #     # this list might be LvmVolumeGroups as LVMVolumeGroups
    #     lvg_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group=group,
    #                                                                                     plural=lvmvolumegroup_plural,
    #                                                                                     version=version)
    #
    #     kubernetes.client.ApiextensionsV1Api().read_custom_resource_definition('lvmvolumegroups.storage.deckhouse.io')
    #
    #     # no need to back up anything
    #     if len(lvg_list.get('items', [])) == 0:
    #         try:
    #             ctx.kubernetes.delete(kind='CustomResourceDefinition', apiVersion='apiextensions.k8s.io/v1', name='LvmVolumeGroup')
    #         except Exception as e:
    #             if str(e.status) == '404':
    #                 # that means we found LVMVolumeGroup CRD, so the migration has been already done
    #                 print(f"{migrate_script} no LvmVolumeGroup CRD was found, migration has been already done")
    #                 return
    #
    #         print(f"{migrate_script} creates the new LVMVolumeGroup CRD")
    #         for dirpath, _, filenames in os.walk(top=find_crds_root(__file__)):
    #             for filename in filenames:
    #                 if filename == 'lvmvolumegroup.yaml':
    #                     crd_path = os.path.join(dirpath, filename)
    #                     with open(crd_path, "r", encoding="utf-8") as f:
    #                         for manifest in yaml.safe_load_all(f):
    #                             if manifest is None:
    #                                 continue
    #                             ctx.kubernetes.create_or_update(manifest)
    #                             print(f"{migrate_script} CRD {filename} was successfully created")
    #                             return
    #
    #     # as we found some LVG resources, we need to check if they are new or not
    #     if lvg_list.get('items', [])[0]['kind'] == 'LVMVolumeGroup':
    #         print(f"{migrate_script} LVMVolumeGroups have been already migrated")
    #         return
    #
    #     print(f"{migrate_script} needs to migrate. Start it.")
    #     # this is regular flow, or the retry when old LvmVolumeGroup CRD still exists
    #     # first we create backup crd for lvmvolumegroups
    #     print(f"{migrate_script} tries to create backup manifest")
    #     for dirpath, _, filenames in os.walk(top=find_crds_root(__file__)):
    #         for filename in filenames:
    #             if filename == 'lvmvolumegroupbackup.yaml':
    #                 crd_path = os.path.join(dirpath, filename)
    #                 with open(crd_path, "r", encoding="utf-8") as f:
    #                     for manifest in yaml.safe_load_all(f):
    #                         if manifest is None:
    #                             continue
    #                         ctx.kubernetes.create_or_update(manifest)
    #                         print(f"{migrate_script} {filename} was successfully created")
    #                 break
    #
    #     print(f"{migrate_script} starts to create backups and add 'kubernetes.io/hostname' to store the node name")
    #     backuped_lvg = {}
    #     for lvg in lvg_list.get('items', []):
    #         lvg_backup = lvg
    #         lvg_backup['kind'] = 'LvmVolumeGroupBackup'
    #         lvg_backup['metadata']['labels']['kubernetes.io/hostname'] = lvg_backup['status']['nodes'][0]['name']
    #         ctx.kubernetes.create_or_update(lvg_backup)
    #         print(f"{migrate_script} {lvg_backup['metadata']['name']} backup was created")
    #         backuped_lvg[lvg['metadata']['name']] = ''
    #
    #     # check if every backup has been successfully created
    #     print(f"{migrate_script} starts to check if every LvmVolumeGroup backup was successfully created")
    #     lvg_backup_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group=group,
    #                                                                                            plural='lvmvolumegroupbackups',
    #                                                                                            version=version)
    #
    #     for backup in lvg_backup_list.get('items', []):
    #         if backup['metadata']['name'] not in backuped_lvg:
    #             print(
    #                 f"{migrate_script} ERROR: backup {backup['metadata']['name']} was not created to the corresponding LvmVolumeGroup")
    #             raise ValueError(f"{backup['metadata']['name']} was not created")
    #
    #     print(f"{migrate_script} every backup was successfully created for lvmvolumegroups")
    #
    #     # we do it before CRD removal to prevent version conflicts with deletion timestamps
    #     print(f"{migrate_script} remove finalizers from old LvmVolumeGroup CRs")
    #     for lvg in lvg_list.get('items', []):
    #         del lvg['metadata']['finalizers']
    #         ctx.kubernetes.create_or_update(lvg)
    #
    #     print(f"{migrate_script} successfully removed finalizers from old LvmVolumeGroup CRs")
    #     # TODO: выключаем daemon set
    #     print(f"{migrate_script} delete the old LvmVolumeGroup CRD")
    #     # we delete the LvmVolumeGroup CRD (old one)
    #     ctx.kubernetes.delete(kind='CustomResourceDefinition', apiVersion='apiextensions.k8s.io/v1', name='LvmVolumeGroup')
    #     print(f"{migrate_script} successfully deleted old LvmVolumeGroup CRD")
    #     print(f"{migrate_script} creates the new LVMVolumeGroup CRD")
    #     for dirpath, _, filenames in os.walk(top=find_crds_root(__file__)):
    #         for filename in filenames:
    #             if filename == 'lvmvolumegroup.yaml':
    #                 crd_path = os.path.join(dirpath, filename)
    #                 with open(crd_path, "r", encoding="utf-8") as f:
    #                     for manifest in yaml.safe_load_all(f):
    #                         if manifest is None:
    #                             continue
    #                         ctx.kubernetes.create_or_update(manifest)
    #                         print(f"{migrate_script} {filename} was successfully created")
    #                         break
    #
    #     print(f"{migrate_script} successfully created the new LVMVolumeGroup CRD")
    #
    #     print(f"{migrate_script} create new LVMVolumeGroup CRs")
    #     for lvg_backup in lvg_backup_list.get('items', []):
    #         lvg = lvg_backup
    #         lvg_name = lvg['metadata']['name']
    #         lvg['kind'] = 'LVMVolumeGroup'
    #         bd_names: List[str] = lvg['spec']['blockDeviceNames']
    #         if len(bd_names) > 0:
    #             print(f"{migrate_script} extracted BlockDevice names: {bd_names} from LVMVolumeGroup {lvg_name}")
    #             del lvg['spec']['blockDeviceNames']
    #             lvg['spec']['local'] = {'nodeName': lvg_nodename[lvg_name]}
    #             print(f"{migrate_script} LVMVolumeGroup {lvg_name} spec after adding the Local field: {lvg['spec']}")
    #             lvg['spec']['blockDeviceSelector'] = {
    #                 'matchLabels': {'kubernetes.io/hostname': lvg['spec']['local']['nodeName']},
    #                 'matchExpressions': [
    #                     {'key': 'kubernetes.io/metadata.name', 'operator': 'in', 'values': bd_names}]
    #             }
    #             print(f"{migrate_script} LVMVolumeGroup {lvg_name} spec after adding the Selector: {lvg['spec']}")
    #         ctx.kubernetes.create(lvg)
    #         print(f"{migrate_script} LVMVolumeGroup {lvg_name} was created")
    #     print(f"{migrate_script} successfully created every LVMVolumeGroup CR from backup")
    #
    #     print(f"{migrate_script} wait for LVMVolumeGroup got configured")
    #     lvg_configured = False
    #     attempts = 0
    #     while True:
    #         if attempts == 5:
    #             print(
    #                 f"{migrate_script} maximum attempts reached while waiting for the LVMVolumeGroups to be configured. Stopping retries.")
    #             break
    #
    #         attempts += 1
    #
    #         new_lvg_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group=group,
    #                                                                                             plural=lvmvolumegroup_plural,
    #                                                                                             version=version)
    #
    #         is_ready = True
    #         for new_lvg in new_lvg_list.get('items', []):
    #             if new_lvg['status']['phase'] is not 'Ready':
    #                 print(
    #                     f"{migrate_script} the LVMVolumeGroup {new_lvg['metadata']['name']} is not Ready yet. Retry in 3s")
    #                 is_ready = False
    #                 break
    #
    #         if is_ready:
    #             print(f"{migrate_script} all LVMVolumeGroups status.phase are Ready")
    #             lvg_configured = True
    #             break
    #
    #         time.sleep(3)
    #
    #     if not lvg_configured:
    #         print(
    #             f"{migrate_script} ATTENTION: some LVMVolumeGroups are not in Ready state. Check more information about it in LVMVolumeGroup status. The backups will not be deleted automatically, you will have to do it MANUALLY.")
    #         return
    #
    #     print(f"{migrate_script} removes the backup CRD")
    #     ctx.kubernetes.delete(kind='CustomResourceDefinition', apiVersion='apiextensions.k8s.io/v1',
    #                           name='LvmVolumeGroupBackup')
    #     print(f"{migrate_script} successfully removed the backup CRD")
    #
    #     # now every LvmVolumeGroup CR has deletion timestamp and is waiting for its finalizer removal
    #     # TODO: не удаляем backup
    #
    #     print(f"{migrate_script} removes finalizers from backup LvmVolumeGroup CRs")
    #     for lvg_backup in lvg_backup_list.get('items', []):
    #         del lvg_backup['metadata']['finalizers']
    #         ctx.kubernetes.create_or_update(lvg_backup)
    #         print(f"{migrate_script} removed finalizer from backup LvmVolumeGroup {lvg_backup['metadata']['name']}")
    #     except Exception as e:
    #         if str(e.status) == '404':
    #             # this case might be triggered if we start in a "fresh" cluster, or we retry after down
    #             print(f"{migrate_script} no old LvmVolumeGroup CRD was found, try to find the backups")
    #             try:
    #                 lvg_backup_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group=group,
    #                                                                                                        plural='lvmvolumegroupbackups',
    #                                                                                                        version=version)
    #
    #                 print(f"{migrate_script} LvmVolumeGroup backups were found. Check if every backup has node name")
    #                 for lvg_backup in lvg_backup_list.get('items', []):
    #                     if lvg_backup['metadata']['labels']['kubernetes.io/hostname'] == '':
    #                         raise ValueError(
    #                             f"{lvg_backup['metadata']['name']} has no 'kubernetes.io/hostname' specified in labels. Specify node name manually into the labels with key 'kubernetes.io/hostname'")
    #
    #                 print(f"{migrate_script} creates the new LVMVolumeGroup CRD")
    #                 for dirpath, _, filenames in os.walk(top=find_crds_root(__file__)):
    #                     for filename in filenames:
    #                         if filename == 'lvmvolumegroup.yaml':
    #                             crd_path = os.path.join(dirpath, filename)
    #                             with open(crd_path, "r", encoding="utf-8") as f:
    #                                 for manifest in yaml.safe_load_all(f):
    #                                     if manifest is None:
    #                                         continue
    #                                     ctx.kubernetes.create_or_update(manifest)
    #                                     print(f"{migrate_script} CRD {filename} was successfully created")
    #                             break
    #
    #                 print(f"{migrate_script} create new LVMVolumeGroup CRs")
    #                 for lvg_backup in lvg_backup_list.get('items', []):
    #                     lvg = lvg_backup
    #                     lvg_name = lvg['metadata']['name']
    #                     lvg['kind'] = 'LVMVolumeGroup'
    #                     bd_names: List[str] = lvg['spec']['blockDeviceNames']
    #                     print(f"{migrate_script} extracted BlockDevice names: {bd_names} from LVMVolumeGroup {lvg_name}")
    #                     del lvg['spec']['blockDeviceNames']
    #                     lvg['spec']['local'] = {'nodeName': lvg['metadata']['labels']['kubernetes.io/hostname']}
    #                     del lvg['metadata']['labels']['kubernetes.io/hostname']
    #                     print(
    #                         f"{migrate_script} LVMVolumeGroup {lvg_name} spec after adding the Local field: {lvg['spec']}")
    #                     lvg['spec']['blockDeviceSelector'] = {
    #                         'matchLabels': {'kubernetes.io/hostname': lvg['spec']['local']['nodeName']},
    #                         'matchExpressions': [
    #                             {'key': 'kubernetes.io/metadata.name', 'operator': 'in', 'values': bd_names}]
    #                     }
    #                     print(f"{migrate_script} LVMVolumeGroup {lvg_name} spec after adding the Selector: {lvg['spec']}")
    #                     ctx.kubernetes.create_or_update(lvg)
    #                     print(f"{migrate_script} LVMVolumeGroup {lvg_name} was created")
    #                 print(f"{migrate_script} successfully created every LVMVolumeGroup CR from backup")
    #
    #             except Exception as ex:
    #                 if str(ex.status) == '404':
    #                     # this means we start in a "fresh" cluster
    #                     print(f"{migrate_script} no LVMVolumeGroup or BackUp CRD was found. No need to migrate")
    #                     return
    #                 else:
    #                     print(f"{migrate_script} exception occurred during the migration process: {e}")
    #     else:
    #         print(f"{migrate_script} exception occurred during the migration process: {e}")

def delete_old_lvg_crd():
    kubernetes.client.CustomObjectsApi().delete_cluster_custom_object()

def find_crds_root(hookpath):
    hooks_root = os.path.dirname(hookpath)
    module_root = os.path.dirname(hooks_root)
    crds_root = os.path.join(module_root, "crds")
    return crds_root


if __name__ == "__main__":
    hook.run(main, config=config)
