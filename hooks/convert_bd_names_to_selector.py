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
from typing import Any, List

import kubernetes
import yaml
from deckhouse import hook

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
    print(f"{migrate_script} starts to migrate LvmVolumeGroup kind to LVMVolumeGroup new version")
    kubernetes.config.load_incluster_config()

    api_v1 = kubernetes.client.AppsV1Api()
    custom_api = kubernetes.client.CustomObjectsApi()
    api_extension = kubernetes.client.ApiextensionsV1Api()

    ds_name = 'sds-node-configurator'
    ds_ns = 'd8-sds-node-configurator'
    crd_name = "lvmvolumegroups.storage.deckhouse.io"

    print(f"{migrate_script} tries to scale down the sds-node-configurator daemon set")
    try:
        api_v1.delete_namespaced_daemon_set(name=ds_name, namespace=ds_ns)
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            pass
    except Exception as e:
        raise e
    print(f"{migrate_script} daemon set has been successfully scaled down")

    try:
        print(f"{migrate_script} tries to find lvmvolumegroup CRD")
        crd = api_extension.read_custom_resource_definition(crd_name)
        print(f"{migrate_script} successfully found lvmvolumegroup CRD")

        ### LvmVolumeGroup CRD flow
        if crd.spec.names.kind == 'LvmVolumeGroup':
            print(f"{migrate_script} found LvmVolumeGroup CRD")
            print(f"{migrate_script} tries to list lvmvolumegroup resources")
            lvg_list = None
            try:
                lvg_list: Any = custom_api.list_cluster_custom_object(group=group,
                                                                      plural=lvmvolumegroup_plural,
                                                                      version=version)
                print(f"{migrate_script} successfully listed lvmvolumegroup resources")
            except kubernetes.client.exceptions.ApiException as e:
                # means no lvmvolumegroup resources found
                if e.status == 404:
                    print(f"{migrate_script} no lvmvolumegroup resources found, tries to delete LvmVolumeGroup CRD")
                    try:
                        api_extension.delete_custom_resource_definition(crd_name)
                    except Exception as e:
                        print(f"{migrate_script} unable to delete LvmVolumeGroup CRD, error: {e}")

                    print(f"{migrate_script} successfully deleted the LvmVolumeGroup CRD")

                    print(f"{migrate_script} tries to create LVMVolumeGroup CRD")
                    create_new_lvg_crd(ctx)
                    print(f"{migrate_script} successfully created LVMVolumeGroup CRD")
                    return
            except Exception as e:
                raise e

            print(f"{migrate_script} some lvmvolumegroup resource were found, tries to create LvmVolumeGroupBackup CRD")
            for dirpath, _, filenames in os.walk(top=find_crds_root(__file__)):
                found = False
                for filename in filenames:
                    if filename == 'lvmvolumegroupbackup.yaml':
                        crd_path = os.path.join(dirpath, filename)
                        print(f"{migrate_script} CRD path: {crd_path}")
                        with open(crd_path, "r", encoding="utf-8") as f:
                            for manifest in yaml.safe_load_all(f):
                                if manifest is None:
                                    print(f"{migrate_script} LvmVolumeGroupBackup manifest is None, skip it")
                                    continue
                                try:
                                    found = True
                                    print(f"{migrate_script} LvmVolumeGroupBackup manifest found, tries to create it")
                                    print(f"{migrate_script} manifest: {manifest}")
                                    ctx.kubernetes.create_or_update(manifest)
                                    break
                                except Exception as e:
                                    print(f"{migrate_script} unable to create LVMVolumeGroupBackup CRD, error: {e}")
                                    raise e
                    if found:
                        break

            print(f"{migrate_script} successfully created LvmVolumeGroupBackup CRD")

            print(f"{migrate_script} starts to create backups and add 'kubernetes.io/hostname' to store the node name")
            for lvg in lvg_list.get('items', []):
                lvg_backup = copy.deepcopy(lvg)
                lvg_backup['kind'] = 'LvmVolumeGroupBackup'
                lvg_backup['metadata']['labels']['kubernetes.io/hostname'] = lvg_backup['status']['nodes'][0]['name']
                lvg_backup['metadata']['labels'][migration_completed_label] = 'false'
                try:
                    create_or_update_custom_resource(group=group,
                                                     plural='lvmvolumegroupbackups',
                                                     version=version,
                                                     resource=lvg_backup)

                except Exception as e:
                    print(f"{migrate_script} unable to create backup, error: {e}")
                    raise e
                print(f"{migrate_script} {lvg_backup['metadata']['name']} backup was created")
            print(f"{migrate_script} every backup was successfully created for lvmvolumegroups")

            print(f"{migrate_script} remove finalizers from old LvmVolumeGroup CRs")
            for lvg in lvg_list.get('items', []):
                try:
                    custom_api.patch_cluster_custom_object(group=group,
                                                           plural='lvmvolumegroups',
                                                           version=version,
                                                           name=lvg['metadata']['name'],
                                                           body={'metadata': {'finalizers': []}})
                except Exception as e:
                    print(f"{migrate_script} unable to remove finalizers from LvmVolumeGroups, error: {e}")
                    raise e
                print(f"{migrate_script} removed finalizer from LvmVolumeGroup {lvg['metadata']['name']}")
            print(f"{migrate_script} successfully removed finalizers from old LvmVolumeGroup CRs")
    except Exception as e:
        print(f"{migrate_script} error occurred")
        raise e
    #
    #         print(f"{migrate_script} tries to delete LvmVolumeGroup CRD")
    #         try:
    #             api_extension.delete_custom_resource_definition(crd_name)
    #         except kubernetes.client.exceptions.ApiException as e:
    #             if e.status == 404:
    #                 print(f"{migrate_script} the LvmVolumeGroup CRD has been already deleted")
    #                 pass
    #         except Exception as e:
    #             print(f"{migrate_script} unable to delete LvmVolumeGroup CRD")
    #             raise e
    #         print(f"{migrate_script} successfully removed LvmVolumeGroup CRD")
    #
    #         print(f"{migrate_script} tries to create LVMVolumeGroup CRD")
    #         create_new_lvg_crd(ctx)
    #         print(f"{migrate_script} successfully created LVMVolumeGroup CRD")
    #
    #         print(f"{migrate_script} tries to get LvmVolumeGroupBackups")
    #         try:
    #             lvg_backup_list: Any = custom_api.list_cluster_custom_object(group=group,
    #                                                                          plural='lvmvolumegroupbackups',
    #                                                                          version=version)
    #         except Exception as e:
    #             print(f"{migrate_script} unable to list lvmvolumegroupbackups, error: {e}")
    #             raise e
    #
    #         print(f"{migrate_script} successfully got LvmVolumeGroupBackups")
    #         print(f"{migrate_script} create new LVMVolumeGroup CRs")
    #         for lvg_backup in lvg_backup_list.get('items', []):
    #             lvg = configure_new_lvg(lvg_backup)
    #             try:
    #                 create_or_update_custom_resource(group=group,
    #                                                  plural='lvmvolumegroups',
    #                                                  version=version,
    #                                                  resource=lvg)
    #                 print(f"{migrate_script} LVMVolumeGroup {lvg['metadata']['name']} was created")
    #             except Exception as e:
    #                 print(f"{migrate_script} unable to create LVMVolumeGroup {lvg['metadata']['name']}, error: {e}")
    #                 raise e
    #             try:
    #                 custom_api.patch_cluster_custom_object(group=group,
    #                                                        version=version,
    #                                                        plural='lvmvolumegroupbackups',
    #                                                        name=lvg_backup['metadata']['name'],
    #                                                        body={
    #                                                            'metadata': {
    #                                                                'labels': {migration_completed_label: 'true'}}})
    #                 print(
    #                     f"{migrate_script} the LVMVolumeGroupBackup label {migration_completed_label} was updated to true")
    #             except Exception as e:
    #                 print(
    #                     f"{migrate_script} unable to update LvmVolumeGroupBackup {lvg_backup['metadata']['name']}, error: {e}")
    #                 raise e
    #             print(f"{migrate_script} successfully created every LVMVolumeGroup CR from backup")
    #             return
    #     ### End of LvmVolumeGroup CRD flow
    #
    #     ### LVMVolumeGroup CRD flow
    #     print(f"{migrate_script} found LVMVolumeGroup CRD")
    #
    #     print(f"{migrate_script} tries to list LVMVolumeGroup resources")
    #     try:
    #         lvg_list: Any = custom_api.list_cluster_custom_object(group=group,
    #                                                               plural='lvmvolumegroups',
    #                                                               version=version)
    #     except Exception as e:
    #         print(f"{migrate_script} unable to list LVMVolumeGroup resources")
    #         raise e
    #     print(f"{migrate_script} successfully listed LVMVolumeGroup resources")
    #
    #     actual_lvgs = {}
    #     for lvg in lvg_list.get('items', []):
    #         actual_lvgs[lvg['metadata']['name']] = ''
    #
    #     print(f"{migrate_script} tries to list lvmvolumegroupbackups")
    #     try:
    #         lvg_backup_list: Any = custom_api.list_cluster_custom_object(group=group,
    #                                                                      plural='lvmvolumegroupbackups',
    #                                                                      version=version)
    #     except Exception as e:
    #         print(f"{migrate_script} unable to list lvmvolumegroupbackups, error: {e}")
    #         raise e
    #     print(f"{migrate_script} successfully listed lvmvolumegroupbackups")
    #
    #     for backup in lvg_backup_list.get('items', []):
    #         if backup['metadata']['name'] in actual_lvgs:
    #             print(f"{migrate_script} LVMVolumeGroup {backup['metadata']['name']} has been already migrated")
    #             continue
    #
    #         if backup['metadata']['labels'][migration_completed_label] == 'true':
    #             print(f"{migrate_script} the LvmVolumeGroup {backup['metadata']['name']} was already migrated")
    #             continue
    #
    #         print(f"{migrate_script} tries to create LVMVolumeGroup {backup['metadata']['name']}")
    #         lvg = configure_new_lvg(backup)
    #         try:
    #             create_or_update_custom_resource(group=group,
    #                                              plural='lvmvolumegroups',
    #                                              version=version,
    #                                              resource=lvg)
    #         except Exception as e:
    #             print(f"{migrate_script} unable to create LVMVolumeGroup {lvg['metadata']['name']}, error: {e}")
    #             raise e
    #         print(
    #             f"{migrate_script} the LVMVolumeGroup {lvg['metadata']['name']} has been successfully created")
    #
    #         print(f"{migrate_script} tries to update LvmVolumeGroupBackup {backup['metadata']['name']}")
    #         try:
    #             custom_api.patch_cluster_custom_object(group=group,
    #                                                    version=version,
    #                                                    plural='lvmvolumegroupbackups',
    #                                                    name=backup['metadata']['name'],
    #                                                    body={
    #                                                        'metadata': {'labels': {migration_completed_label: 'true'}}})
    #             print(
    #                 f"{migrate_script} the LVMVolumeGroupBackup {backup['metadata']['name']} {migration_completed_label} was updated to true")
    #         except Exception as e:
    #             print(
    #                 f"{migrate_script} unable to update LVMVolumeGroupBackup {backup['metadata']['name']}, error: {e}")
    #             raise e
    #
    #     print(f"{migrate_script} every LVMVolumeGroup resources has been migrated")
    #     return
    #     ### End of LVMVolumeGroup CRD flow
    # except kubernetes.client.exceptions.ApiException as e:
    #     ### If we do not find any lvmvolumegroup CRD flow
    #     if e.status == 404:
    #         # ничего нет, просто создаем новую CRD и создаем по бэкапам
    #         print(f"{migrate_script} no lvmvolumegroup CRD was found")
    #
    #         print(f"{migrate_script} tries to create LVMVolumeGroup CRD")
    #         create_new_lvg_crd(ctx)
    #         print(f"{migrate_script} successfully created LVMVolumeGroup CRD")
    #
    #         print(f"{migrate_script} tries to list lvmvolumegroupbackups")
    #         try:
    #             lvg_backup_list: Any = custom_api.list_cluster_custom_object(group=group,
    #                                                                          plural='lvmvolumegroupbackups',
    #                                                                          version=version)
    #         except Exception as e:
    #             print(f"{migrate_script} unable to list lvmvolumegroupbackups, error: {e}")
    #             raise e
    #         print(f"{migrate_script} successfully listed lvmvolumegroupbackups")
    #
    #         for backup in lvg_backup_list.get('items', []):
    #             if backup['metadata']['labels'][migration_completed_label] == 'true':
    #                 print(f"{migrate_script} the LvmVolumeGroup {backup['metadata']['name']} was already migrated")
    #                 continue
    #
    #             print(f"{migrate_script} tries to create LVMVolumeGroup {backup['metadata']['name']}")
    #             lvg = configure_new_lvg(backup)
    #             try:
    #                 create_or_update_custom_resource(group=group,
    #                                                  version=version,
    #                                                  plural='lvmvolumegroups',
    #                                                  resource=lvg)
    #             except Exception as e:
    #                 print(f"{migrate_script} unable to create LVMVolumeGroup {lvg['metadata']['name']}, error: {e}")
    #                 raise e
    #             print(
    #                 f"{migrate_script} the LVMVolumeGroup {lvg['metadata']['name']} has been successfully created")
    #             backup['metadata']['labels'][migration_completed_label] = 'true'
    #
    #             print(f"{migrate_script} tries to update LvmVolumeGroupBackup {backup['metadata']['name']}")
    #             try:
    #                 custom_api.patch_cluster_custom_object(group=group,
    #                                                        version=version,
    #                                                        plural='lvmvolumegroupbackups',
    #                                                        name=backup['metadata']['name'],
    #                                                        body=backup)
    #             except Exception as e:
    #                 print(
    #                     f"{migrate_script} unable to create LVMVolumeGroupBackup {backup['metadata']['name']}, error: {e}")
    #                 raise e
    #             print(
    #                 f"{migrate_script} the LVMVolumeGroupBackup {backup['metadata']['name']} {migration_completed_label} was updated to true")
    #     print(f"{migrate_script} every LVMVolumeGroup resources has been migrated")
    #     ### End of if we do not find any lvmvolumegroup CRD flow
    # except Exception as e:
    #     print(f"{migrate_script} unable to get lvmvolumegroup CRD, error: {e}")
    #     raise e


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
                        try:
                            ctx.kubernetes.create_or_update(manifest)
                            print(f"{migrate_script} {filename} was successfully created")
                        except Exception as e:
                            print(f"{migrate_script} unable to create LVMVolumeGroup CRD, error: {e}")
                            raise e
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


def find_crds_root(hookpath):
    hooks_root = os.path.dirname(hookpath)
    module_root = os.path.dirname(hooks_root)
    crds_root = os.path.join(module_root, "crds")
    return crds_root


def create_or_update_custom_resource(group, plural, version, resource):
    max_attempts = 3
    delay_between_attempts = 10

    for attempt in range(max_attempts):
        try:
            kubernetes.client.CustomObjectsApi().replace_cluster_custom_object(group=group,
                                                                               plural=plural,
                                                                               version=version,
                                                                               name=resource['metadata']['name'],
                                                                               body={
                                                                                   'apiVersion': f'{group}/{version}',
                                                                                   'kind': resource['kind'],
                                                                                   'metadata': {
                                                                                       'name': resource['metadata'][
                                                                                           'name'],
                                                                                       'labels': resource['metadata'][
                                                                                           'labels'],
                                                                                       'finalizers': resource['metadata'][
                                                                                           'finalizers']},
                                                                                   'spec': resource['spec']})
            print(f"{migrate_script} {resource['kind']} {resource['metadata']['name']} updated")
            return True
        except kubernetes.client.exceptions.ApiException as e:
            print(f"{migrate_script} {resource['kind']} {resource['metadata']['name']} was not found, try to create it")
            if e.status == 404:
                try:
                    kubernetes.client.CustomObjectsApi().create_cluster_custom_object(group=group,
                                                                                      plural=plural,
                                                                                      version=version,
                                                                                      body={
                                                                                          'apiVersion': f'{group}/{version}',
                                                                                          'kind': resource['kind'],
                                                                                          'metadata': {
                                                                                              'name':
                                                                                                  resource['metadata'][
                                                                                                      'name'],
                                                                                              'labels':
                                                                                                  resource['metadata'][
                                                                                                      'labels'],
                                                                                              'finalizers':
                                                                                                  resource['metadata'][
                                                                                                      'finalizers']},
                                                                                          'spec': resource['spec']})
                    print(f"{migrate_script} {resource['kind']} {resource['metadata']['name']} created")
                except Exception as e:
                    print(
                        f"{migrate_script} failed to create {resource['kind']} {resource['metadata']['name']} after {max_attempts} attempts, error: {e}")
                    return False
        except Exception as e:
            print(
                f"{migrate_script} attempt {attempt + 1} failed for {resource['kind']} {resource['metadata']['name']} with message: {e}")
            if attempt < max_attempts - 1:
                print(f"{migrate_script} retrying in {delay_between_attempts} seconds...")
                # time.sleep(delay_between_attempts)
            else:
                print(
                    f"{migrate_script} failed to create {resource['kind']} {resource['metadata']['name']} after {max_attempts} attempts")
                return False


if __name__ == "__main__":
    hook.run(main, config=config)
