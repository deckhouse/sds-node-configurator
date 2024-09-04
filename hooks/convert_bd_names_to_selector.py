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
import copy
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

migration_completed_label = 'migration-completed'


# This webhook ensures the migration of LVMVolumeGroup resources from the old CRD version to the new one:
# - Removes field spec.blockDeviceNames
# - Adds spec.Local field and fills its value 'nodeName' with the resource's node.
# - Adds spec.blockDeviceSelector field and fills it with the LVMVolumeGroup nodeName and blockDeviceNames

# add some

def main(ctx: hook.Context):
    print(f"{migrate_script} starts to migrate LvmVolumeGroup kind to LVMVolumeGroup")
    kubernetes.config.load_incluster_config()

    api_v1 = kubernetes.client.AppsV1Api()
    custom_api = kubernetes.client.CustomObjectsApi()
    api_extenstion = kubernetes.client.ApiextensionsV1Api()

    ds_name = 'sds-node-configurator'
    ds_ns = 'd8-sds-node-configurator'
    print(f"{migrate_script} tries to scale down the sds-node-configurator daemon set")
    daemonset = api_v1.read_namespaced_daemon_set(name=ds_name, namespace=ds_ns)
    if daemonset.spec.template.spec.node_selector is None:
        daemonset.spec.template.spec.node_selector = {}
    daemonset.spec.template.spec.node_selector = {'exclude': 'true'}
    api_v1.patch_namespaced_daemon_set(name=ds_name, namespace=ds_ns, body=daemonset)
    print(f"{migrate_script} daemon set has been successfully scaled down")

    # get current lvgs with old CRD (which is LvmVolumeGroup)
    try:
        # this list might be LvmVolumeGroups as LVMVolumeGroups
        lvg_list: Any = custom_api.list_cluster_custom_object(group=group,
                                                              plural=lvmvolumegroup_plural,
                                                              version=version)

        # so we might have LvmVolumeGroup list or LVMVolumeGroup list
        if len(lvg_list.get('items', [])) == 0:
            try:
                # ctx.kubernetes.delete(kind='CustomResourceDefinition',
                #                       apiVersion='apiextensions.k8s.io/v1',
                #                       name='LvmVolumeGroup',
                #                       namespace='')

                api_extenstion.delete_custom_resource_definition('lvmvolumegroups.storage.deckhouse.io')
                print(f"{migrate_script} successfully delete the LvmVolumeGroup CRD")
                # that means we deleted old kind (LvmVolumeGroup)
                create_new_lvg_crd(ctx)
                turn_on_daemonset(api_v1, ds_name, ds_ns, daemonset)
                return
            except kubernetes.client.exceptions.ApiException as e:
                if e.status == '404':
                    # that means we found LVMVolumeGroup CRD
                    lvg_backup_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group=group,
                                                                                                           plural='lvmvolumegroupbackups',
                                                                                                           version=version)
                    for backup in lvg_backup_list.get('items', []):
                        if backup['metadata']['labels'][migration_completed_label] == 'true':
                            print(
                                f"{migrate_script} the LvmVolumeGroup {backup['metadata']['name']} was already migrated")
                            continue

                        lvg = configure_new_lvg(backup)
                        ctx.kubernetes.create_or_update(lvg)
                        print(
                            f"{migrate_script} the LVMVolumeGroup {lvg['metadata']['name']} has been successfully created")
                        backup['metadata']['labels'][migration_completed_label] = 'true'
                        ctx.kubernetes.create_or_update(backup)
                        print(
                            f"{migrate_script} the LVMVolumeGroupBackup {lvg['metadata']['name']} {migration_completed_label} was updated to true")

                    turn_on_daemonset(api_v1, ds_name, ds_ns, daemonset)
                    return
            except Exception as e:
                print(f"{migrate_script} ERROR occurred while deleting the LvmVolumeGroup CRD: {e}")
                raise e

            print(f"{migrate_script} creates the new LVMVolumeGroup CRD")
            for dirpath, _, filenames in os.walk(top=find_crds_root(__file__)):
                for filename in filenames:
                    if filename == 'lvmvolumegroup.yaml':
                        crd_path = os.path.join(dirpath, filename)
                        with open(crd_path, "r", encoding="utf-8") as f:
                            for manifest in yaml.safe_load_all(f):
                                if manifest is None:
                                    continue
                                ctx.kubernetes.create_or_update(manifest)
                                print(f"{migrate_script} CRD {filename} was successfully created")
                                break

        # as we found some LVG resources, we need to check if they are new or not
        if lvg_list.get('items', [])[0]['kind'] == 'LVMVolumeGroup':
            print(f"{migrate_script} found LVMVolumeGroup kind")
            actual_lvgs = {}
            for lvg in lvg_list.get('items', []):
                actual_lvgs[lvg['metadata']['name']] = ''

            try:
                lvg_backup_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group=group,
                                                                                                       plural='lvmvolumegroupbackups',
                                                                                                       version=version)
                for backup in lvg_backup_list.get('items', []):
                    if backup['metadata']['name'] in actual_lvgs:
                        print(f"{migrate_script} LVMVolumeGroup {backup['metadata']['name']} has been already migrated")
                        continue

                    if backup['metadata']['labels'][migration_completed_label] == 'true':
                        print(f"{migrate_script} no need to migrate LVMVolumeGroup {backup['metadata']['name']}")
                        continue

                    lvg = configure_new_lvg(backup)
                    ctx.kubernetes.create_or_update(lvg)
                    print(f"{migrate_script} LVMVolumeGroup {backup['metadata']['name']} has been successfully created")

                    backup['metadata']['labels'][migration_completed_label] = 'true'
                    ctx.kubernetes.create_or_update(backup)
                    print(
                        f"{migrate_script} LVMVolumeGroupBackup {backup['metadata']['name']} {migration_completed_label} has been updated to true")

                turn_on_daemonset(api_v1, ds_name, ds_ns, daemonset)
                return
            except kubernetes.client.exceptions.ApiException as e:
                if e.status == '404':
                    print(f"{migrate_script} no migration needed")
                    return
            except Exception as e:
                print(f"{migrate_script} ERROR occurred: {e}")
                raise e

        # so we have old CRD (LvmVolumeGroup) and we need to full cycle migration
        print(f"{migrate_script} starts the full cycle migration")

        # first we create backup crd for lvmvolumegroups
        print(f"{migrate_script} tries to create backup manifest")
        for dirpath, _, filenames in os.walk(top=find_crds_root(__file__)):
            for filename in filenames:
                if filename == 'lvmvolumegroupbackup.yaml':
                    crd_path = os.path.join(dirpath, filename)
                    with open(crd_path, "r", encoding="utf-8") as f:
                        for manifest in yaml.safe_load_all(f):
                            if manifest is None:
                                continue
                            ctx.kubernetes.create_or_update(manifest)
                            print(f"{migrate_script} {filename} was successfully created")
                            break

        print(f"{migrate_script} starts to create backups and add 'kubernetes.io/hostname' to store the node name")
        for lvg in lvg_list.get('items', []):
            lvg_backup = copy.deepcopy(lvg)
            lvg_backup['kind'] = 'LvmVolumeGroupBackup'
            lvg_backup['metadata']['labels']['kubernetes.io/hostname'] = lvg_backup['status']['nodes'][0]['name']
            lvg_backup['metadata']['labels'][migration_completed_label] = 'false'
            ctx.kubernetes.create_or_update(lvg_backup)
            print(f"{migrate_script} {lvg_backup['metadata']['name']} backup was created")

        print(f"{migrate_script} every backup was successfully created for lvmvolumegroups")

        # we do it before CRD removal to prevent deletion timestamps
        print(f"{migrate_script} remove finalizers from old LvmVolumeGroup CRs")
        for lvg in lvg_list.get('items', []):
            del lvg['metadata']['finalizers']
            print(f"{migrate_script} metadata of LvmVolumeGroup {lvg['metadata']['name']}: {lvg['metadata']}")
            ctx.kubernetes.create_or_update(lvg)
            print(f"{migrate_script} removed finalizer from LvmVolumeGroup {lvg['metadata']['name']}")
        print(f"{migrate_script} successfully removed finalizers from old LvmVolumeGroup CRs")

        # we delete the LvmVolumeGroup CRD (old one)
        print(f"{migrate_script} delete the old LvmVolumeGroup CRD")
        # ctx.kubernetes.delete(kind='CustomResourceDefinition',
        #                       apiVersion='apiextensions.k8s.io/v1',
        #                       name='LvmVolumeGroup',
        #                       namespace='')
        api_extenstion.delete_custom_resource_definition('lvmvolumegroups.storage.deckhouse.io')
        print(f"{migrate_script} successfully deleted old LvmVolumeGroup CRD")

        # we create new LVMVolumeGroup CRD (new one)
        create_new_lvg_crd(ctx)
        print(f"{migrate_script} successfully created the new LVMVolumeGroup CRD")

        print(f"{migrate_script} get LvmVolumeGroupBackups")
        lvg_backup_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group=group,
                                                                                               plural='lvmvolumegroupbackups',
                                                                                               version=version)
        print(f"{migrate_script} successfully got LvmVolumeGroupBackups")
        print(f"{migrate_script} create new LVMVolumeGroup CRs")
        for lvg_backup in lvg_backup_list.get('items', []):
            lvg = configure_new_lvg(backup)
            ctx.kubernetes.create(lvg)
            print(f"{migrate_script} LVMVolumeGroup {lvg['metadata']['name']} was created")

            lvg_backup['metadata']['labels'][migration_completed_label] = 'true'
            ctx.kubernetes.create_or_update(lvg_backup)
            print(f"{migrate_script} the LVMVolumeGroupBackup label {migration_completed_label} was updated to true")
        print(f"{migrate_script} successfully created every LVMVolumeGroup CR from backup")

        turn_on_daemonset(api_v1, ds_name, ds_ns, daemonset)
        return
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == '404':
            # this means we have no CRD with LVMVolumeGroup or LvmVolumeGroup kind

            print(f"{migrate_script} creates the new LVMVolumeGroup CRD")
            create_new_lvg_crd(ctx)

            print(f"{migrate_script} no old LvmVolumeGroup CRD was found, try to find the backups")
            try:
                lvg_backup_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group=group,
                                                                                                       plural='lvmvolumegroupbackups',
                                                                                                       version=version)

                print(f"{migrate_script} LvmVolumeGroup backups were found. Check if every backup has node name")
                for lvg_backup in lvg_backup_list.get('items', []):
                    if lvg_backup['metadata']['labels']['kubernetes.io/hostname'] == '':
                        raise ValueError(
                            f"{lvg_backup['metadata']['name']} has no 'kubernetes.io/hostname' specified in labels. Specify node name manually into the labels with key 'kubernetes.io/hostname'")

                print(f"{migrate_script} create new LVMVolumeGroup CRs")
                for lvg_backup in lvg_backup_list.get('items', []):
                    if lvg_backup['metadata']['labels'][migration_completed_label] == 'true':
                        print(f"{migrate_script} no need to create LVMVolumeGroup {lvg_backup['metadata']['name']}")
                        continue

                    lvg = configure_new_lvg(lvg_backup)
                    ctx.kubernetes.create_or_update(lvg)
                    print(f"{migrate_script} LVMVolumeGroup {lvg['metadata']['name']} was created")

                    lvg_backup['metadata']['labels'][migration_completed_label] = 'true'
                    ctx.kubernetes.create_or_update(lvg_backup)
                    print(
                        f"{migrate_script} updated LvmVolumeGroupBackup {lvg['metadata']['name']} migration completed to true")
                print(f"{migrate_script} successfully created every LVMVolumeGroup CR from backup")
                turn_on_daemonset(api_v1, ds_name, ds_ns, daemonset)
                return
            except kubernetes.client.exceptions.ApiException as ex:
                if ex.status == '404':
                    # this means we start in a "fresh" cluster
                    print(f"{migrate_script} no LVMVolumeGroup or BackUp CRD was found. No need to migrate")
                    turn_on_daemonset(api_v1, ds_name, ds_ns, daemonset)
                    return
            except Exception as e:
                print(f"{migrate_script} exception occurred during the migration process: {e}")
                raise e
    except Exception as e:
        print(f"{migrate_script} exception occurred during the migration process: {e}")
        raise e


def create_new_lvg_crd(ctx):
    print(f"{migrate_script} creates the new LVMVolumeGroup CRD")
    for dirpath, _, filenames in os.walk(top=find_crds_root(__file__)):
        for filename in filenames:
            if filename == 'lvmvolumegroup.yaml':
                crd_path = os.path.join(dirpath, filename)
                with open(crd_path, "r", encoding="utf-8") as f:
                    for manifest in yaml.safe_load_all(f):
                        if manifest is None:
                            continue
                        ctx.kubernetes.create_or_update(manifest)
                        print(f"{migrate_script} {filename} was successfully created")
                        break


def configure_new_lvg(backup):
    lvg = copy.deepcopy(backup)
    lvg_name = lvg['metadata']['name']
    lvg['kind'] = 'LVMVolumeGroup'
    bd_names: List[str] = lvg['spec']['blockDeviceNames']
    print(f"{migrate_script} extracted BlockDevice names: {bd_names} from LVMVolumeGroup {lvg_name}")
    del lvg['spec']['blockDeviceNames']
    lvg['spec']['local'] = {'nodeName': lvg['metadata']['labels']['kubernetes.io/hostname']}
    del lvg['metadata']['labels']['kubernetes.io/hostname']
    del lvg['metadata']['labels'][migration_completed_label]
    print(f"{migrate_script} LVMVolumeGroup {lvg_name} spec after adding the Local field: {lvg['spec']}")
    lvg['spec']['blockDeviceSelector'] = {
        'matchLabels': {'kubernetes.io/hostname': lvg['spec']['local']['nodeName']},
        'matchExpressions': [
            {'key': 'kubernetes.io/metadata.name', 'operator': 'in', 'values': bd_names}]
    }
    print(f"{migrate_script} LVMVolumeGroup {lvg_name} spec after adding the Selector: {lvg['spec']}")
    return lvg


def turn_on_daemonset(api_v1, name, namespace, daemonset):
    del daemonset['spec']['template']['spec']['nodeSelector']['exclude']
    print(f"{migrate_script} successfully migrated LvmVolumeGroup kind to LVMVolumeGroup")
    api_v1.patch_namespaced_daemon_set(name=name, namespace=namespace, body=daemonset)


def find_crds_root(hookpath):
    hooks_root = os.path.dirname(hookpath)
    module_root = os.path.dirname(hooks_root)
    crds_root = os.path.join(module_root, "crds")
    return crds_root


if __name__ == "__main__":
    hook.run(main, config=config)
