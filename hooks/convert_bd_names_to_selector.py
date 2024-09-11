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
ds_name = 'sds-node-configurator'
ds_ns = 'd8-sds-node-configurator'
lvg_crd_name = "lvmvolumegroups.storage.deckhouse.io"
secret_name = 'lvg-migration'

migration_completed_label = 'migration-completed'


# This webhook ensures the migration of LVMVolumeGroup resources from the old CRD version to the new one:
# - Removes field spec.blockDeviceNames
# - Adds spec.Local field and fills its value 'nodeName' with the resource's node.
# - Adds spec.blockDeviceSelector field and fills it with the LVMVolumeGroup nodeName and blockDeviceNames

# need the param as it is given from a running context above
def main(ctx: hook.Context):
    kubernetes.config.load_incluster_config()
    api_v1 = kubernetes.client.AppsV1Api()
    custom_api = kubernetes.client.CustomObjectsApi()
    api_extension = kubernetes.client.ApiextensionsV1Api()

    print(f"{migrate_script} tries to check if LvmVolumeGroup migration has been completed")
    try:
        kubernetes.client.CoreV1Api().read_namespaced_secret(secret_name, 'd8-sds-node-configurator')
        print(f"{migrate_script} secret {secret_name} was found, no need to run the migration")
        return
    except kubernetes.client.exceptions.ApiException as ae:
        if ae.status == 404:
            pass
        else:
            print(f"{migrate_script} unable to get the secret {secret_name}, error: {ae}")
            raise ae

    print(
        f"{migrate_script} no migration has been completed, starts to migrate LvmVolumeGroup kind to LVMVolumeGroup new version")

    print(f"{migrate_script} tries to scale down the sds-node-configurator daemon set")
    try:
        api_v1.delete_namespaced_daemon_set(name=ds_name, namespace=ds_ns)
    except kubernetes.client.exceptions.ApiException as e:
        # if we are retrying the ds is already deleted
        if e.status == 404:
            pass
    except Exception as e:
        raise e
    print(f"{migrate_script} daemon set has been successfully scaled down")

    print(f"{migrate_script} tries to find lvmvolumegroup CRD")
    try:
        lvg_crd = api_extension.read_custom_resource_definition(lvg_crd_name)
        print(f"{migrate_script} successfully found lvmvolumegroup CRD")
    except kubernetes.client.exceptions.ApiException as ae:
        # that means no LvmVolumeGroup or LVMVolumeGroup CRD was found, so just create LVMVolumeGroup CRD
        if ae.status == 404:
            print(f"{migrate_script} no {lvg_crd_name} was found, creates LVMVolumeGroup CRD")
            try:
                create_new_lvg_crd()
            except Exception as e:
                print(f"{migrate_script} unable to create LVMVolumeGroup CRD, error: {e}")
                raise e

            # check if we have some backups
            print(f"{migrate_script} tries to list LvmVolumeGroupBackups")
            lvg_backup_list = None
            try:
                lvg_backup_list = custom_api.list_cluster_custom_object(group=group,
                                                                        version=version,
                                                                        plural='lvmvolumegroupbackups')
            except kubernetes.client.exceptions.ApiException as ae2:
                if ae2.status == 404:
                    print(f"{migrate_script} lvmvolumegroupbackups CRD was not found")
                    pass
            except Exception as e:
                print(f"{migrate_script} unable to list LvmVolumeGroupBackups, error: {e}")
                raise e

            # if we do not have any of them, we are good, so just end the migration
            if lvg_backup_list is None or len(lvg_backup_list.get('items', [])) == 0:
                print(f"{migrate_script} no LvmVolumeGroup backups were found")
                create_migration_secret()
                print(f"{migrate_script} successfully migrated LvmVolumeGroup to LVMVolumeGroup CRD")
                return

            # if there are some backups, we just create a LVMVolumeGroup for each of them
            print(
                f"{migrate_script} some LvmVolumeGroup backups were found, tries to create LVMVolumeGroups from backups")
            for backup in lvg_backup_list.get('items', []):
                if backup['metadata']['labels']['migration-completed'] == 'true':
                    print(f"{migrate_script} backup {backup['metadata']['name']} has been already migrated")
                    continue

                lvg = configure_new_lvg_from_backup(backup)
                try:
                    create_or_update_custom_resource(
                        group=group,
                        version=version,
                        plural='lvmvolumegroups',
                        resource=lvg)
                except Exception as e:
                    print(
                        f"{migrate_script} unable to create or update the LVMVolumeGroup {lvg['metadata']['name']}, error: {e}")
                    raise e

                try:
                    patch_backup_as_migrated(backup)
                except Exception as e:
                    print(
                        f"{migrate_script} unable to patch the LvmVolumeGroupBackup {backup['metadata']['name']}, error: {e}")
                    raise e

            print(f"{migrate_script} all LVMVolumeGroup were created from the backups")
            create_migration_secret()
            print(f"{migrate_script} successfully migrated LvmVolumeGroup to LVMVolumeGroup CRD")
            return
    except Exception as e:
        print(f"{migrate_script} error occurred, error: {e}")
        raise e

    # LvmVolumeGroup CRD flow
    if lvg_crd.spec.names.kind == 'LvmVolumeGroup':
        print(f"{migrate_script} found LvmVolumeGroup CRD")
        print(f"{migrate_script} tries to list lvmvolumegroup resources")
        lvg_list = {}
        try:
            lvg_list = custom_api.list_cluster_custom_object(group=group,
                                                             plural=lvmvolumegroup_plural,
                                                             version=version)
            print(f"{migrate_script} successfully listed lvmvolumegroup resources")

            # as we have no LvmVolumeGroup resources, we do not need to back up anything, so just delete old CRD and create a new one
            if len(lvg_list.get('items', [])) == 0:
                print(f"{migrate_script} no lvmvolumegroup resources found, tries to delete LvmVolumeGroup CRD")
                try:
                    api_extension.delete_custom_resource_definition(lvg_crd_name)
                except Exception as e:
                    print(f"{migrate_script} unable to delete LvmVolumeGroup CRD, error: {e}")
                    raise e

                print(f"{migrate_script} successfully deleted the LvmVolumeGroup CRD")

                print(f"{migrate_script} tries to create LVMVolumeGroup CRD")
                create_new_lvg_crd()
                print(f"{migrate_script} successfully created LVMVolumeGroup CRD")
                create_migration_secret()
                print(f"{migrate_script} successfully migrated LvmVolumeGroup to LVMVolumeGroup CRD")
                return
        except Exception as e:
            raise e

        # as we have some LvmVolumeGroup resources, we need to back up them and then delete old CRD and create a new one
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
                                api_extension.create_custom_resource_definition(manifest)

                                # as the next step is creating the LvmVolumeGroupBackup resources, we need to be sure, that
                                # LvmVolumeGroupBackup CRD has been appeared in a cluster
                                for count in range(3):
                                    try:
                                        api_extension.read_custom_resource_definition(
                                            "lvmvolumegroupbackups.storage.deckhouse.io")
                                        break
                                    except kubernetes.client.exceptions.ApiException as ae:
                                        if ae.status == 404:
                                            if count == 2:
                                                return ae

                                            time.sleep(1)
                                            pass
                                        else:
                                            print(f"{migrate_script} unable to read LvmVolumeGroupBackup CRD")
                                print(f"{migrate_script} successfully created LvmVolumeGroupBackup CRD")
                                break
                            except kubernetes.client.exceptions.ApiException as ae2:
                                if ae2.status == 409:
                                    print(f"{migrate_script} LvmVolumeGroupBackup CRD has been already created")
                                    pass
                            except Exception as e:
                                print(f"{migrate_script} unable to create LVMVolumeGroupBackup CRD, error: {e}")
                                raise e
                if found:
                    break

        # we store the using node name as a label due to we will lose the status for the backup resource
        print(f"{migrate_script} starts to create backups and add 'kubernetes.io/hostname' to store the node name")
        for lvg in lvg_list.get('items', []):
            lvg_backup = {'apiVersion': lvg['apiVersion'],
                          'kind': 'LvmVolumeGroupBackup',
                          'metadata': {
                              'name':
                                  lvg['metadata'][
                                      'name'],
                              'labels':
                                  lvg['metadata'][
                                      'labels'],
                              'finalizers':
                                  lvg['metadata'][
                                      'finalizers']},
                          'spec': lvg['spec']}
            lvg_backup['metadata']['labels']['kubernetes.io/hostname'] = lvg['status']['nodes'][0]['name']
            lvg_backup['metadata']['labels'][migration_completed_label] = 'false'
            try:
                create_or_update_custom_resource(group=group,
                                                 version=version,
                                                 plural='lvmvolumegroupbackups',
                                                 resource=lvg_backup)
            except Exception as e:
                print(f"{migrate_script} unable to create or update, error {e}")
                raise e
            print(f"{migrate_script} {lvg_backup['metadata']['name']} backup was created")
        print(f"{migrate_script} every backup was successfully created for lvmvolumegroups")

        # before we are going to remove finalizers from LvmVolumeGroup resources and delete LvmVolumeGroup CRD, we need
        # to be sure there are all backups created in the cluster
        print(f"{migrate_script} check if every LvmVolumeGroupBackup is ready to be used")
        lvg_backup_list = {}
        try:
            for count in range(3):
                lvg_backup_list = custom_api.list_cluster_custom_object(group=group,
                                                                        plural='lvmvolumegroupbackups',
                                                                        version=version)
                print(f"{migrate_script} successfully got LvmVolumeGroupBackups")

                if len(lvg_backup_list.get('items', [])) < len(lvg_list.get('items', [])):
                    if count == 2:
                        raise Exception('unable to find some LvmVolumeGroup backups')

                    print(f"{migrate_script} some backups were not ready yet, retry in 1s")
                    time.sleep(1)
                else:
                    print(f"{migrate_script} every backup is ready, continue")
                    break
        except Exception as e:
            print(f"{migrate_script} unable to list lvmvolumegroupbackups, error: {e}")
            raise e

        print(f"{migrate_script} remove finalizers from old LvmVolumeGroup CRs")
        for lvg in lvg_list.get('items', []):
            try:
                custom_api.patch_cluster_custom_object(group=group,
                                                       plural='lvmvolumegroups',
                                                       version=version,
                                                       name=lvg['metadata']['name'],
                                                       body={'metadata': {'finalizers': []}})
                print(f"{migrate_script} successfully removed finalizer from LvmVolumeGroup {lvg['metadata']['name']}")
            except kubernetes.client.exceptions.ApiException as ae:
                print(f"{migrate_script} unable to patch LvmVolumeGroup {lvg['metadata']['name']}, error: {ae}")
                raise ae
            except Exception as e:
                print(f"{migrate_script} unable to remove finalizers from LvmVolumeGroups, error: {e}")
                raise e
        print(f"{migrate_script} successfully removed finalizers from all old LvmVolumeGroup resources")

        print(f"{migrate_script} tries to delete LvmVolumeGroup CRD")
        try:
            delete_old_lvg_crd()
        except Exception as e:
            print(f"{migrate_script} unable to delete LvmVolumeGroup CRD")
            raise e
        print(f"{migrate_script} successfully removed LvmVolumeGroup CRD")

        print(f"{migrate_script} tries to create LVMVolumeGroup CRD")
        create_new_lvg_crd()
        print(f"{migrate_script} successfully created LVMVolumeGroup CRD")

        print(f"{migrate_script} create new LVMVolumeGroup CRs from backups")
        for lvg_backup in lvg_backup_list.get('items', []):
            lvg = configure_new_lvg_from_backup(lvg_backup)
            try:
                create_or_update_custom_resource(group=group,
                                                 plural='lvmvolumegroups',
                                                 version=version,
                                                 resource=lvg)
                print(f"{migrate_script} LVMVolumeGroup {lvg['metadata']['name']} was created")
            except Exception as e:
                print(f"{migrate_script} unable to create LVMVolumeGroup {lvg['metadata']['name']}, error: {e}")
                raise e
            try:
                patch_backup_as_migrated(lvg_backup)
            except Exception as e:
                print(
                    f"{migrate_script} unable to update LvmVolumeGroupBackup {lvg_backup['metadata']['name']}, error: {e}")
                raise e

        print(f"{migrate_script} successfully created every LVMVolumeGroup CR from backup")
        create_migration_secret()
        print(f"{migrate_script} successfully migrated LvmVolumeGroup to LVMVolumeGroup CRD")
        return
    # End of LvmVolumeGroup CRD flow

    # LVMVolumeGroup CRD flow
    print(f"{migrate_script} found LVMVolumeGroup CRD")
    print(f"{migrate_script} tries to list lvmvolumegroupbackups")
    lvg_backup_list = None
    try:
        lvg_backup_list = custom_api.list_cluster_custom_object(group=group,
                                                                plural='lvmvolumegroupbackups',
                                                                version=version)
    except kubernetes.client.exceptions.ApiException as ae:
        if ae.status == 404:
            print(f"{migrate_script} lvmvolumegroupbackups CRD was not found")
            pass
    except Exception as e:
        print(f"{migrate_script} unable to list lvmvolumegroupbackups, error: {e}")
        raise e
    print(f"{migrate_script} successfully listed lvmvolumegroupbackups")

    # if we already have the new LVMVolumeGroup CRD and there is no any backup, we are good and just end the migration
    if lvg_backup_list is None or len(lvg_backup_list.get('items', [])) == 0:
        print(f"{migrate_script} no LvmVolumeGroupBackups found")
        create_migration_secret()
        print(f"{migrate_script} successfully migrated LvmVolumeGroup to LVMVolumeGroup CRD")
        return

    # as we found some backups, we need to check for each of them if new LVMVolumeGroup resources should be created
    print(f"{migrate_script} some LvmVolumeGroupBackups were found")
    print(f"{migrate_script} tries to list LVMVolumeGroup resources")
    try:
        lvg_list: Any = custom_api.list_cluster_custom_object(group=group,
                                                              plural='lvmvolumegroups',
                                                              version=version)
    except Exception as e:
        print(f"{migrate_script} unable to list LVMVolumeGroup resources")
        raise e
    print(f"{migrate_script} successfully listed LVMVolumeGroup resources")

    # we store already created LVMVolumeGroups as ones which should not be migrated
    actual_lvgs = {}
    for lvg in lvg_list.get('items', []):
        actual_lvgs[lvg['metadata']['name']] = ''

    for backup in lvg_backup_list.get('items', []):
        if backup['metadata']['name'] in actual_lvgs:
            print(f"{migrate_script} LVMVolumeGroup {backup['metadata']['name']} has been already migrated")
            continue

        # this means that the corresponding LVMVolumeGroup has been already created from the backup, but also got
        # deleted by a user.
        if backup['metadata']['labels'][migration_completed_label] == 'true':
            print(f"{migrate_script} the LvmVolumeGroup {backup['metadata']['name']} has been already migrated")
            continue

        print(f"{migrate_script} tries to create LVMVolumeGroup {backup['metadata']['name']} from backup")
        lvg = configure_new_lvg_from_backup(backup)
        try:
            create_or_update_custom_resource(group=group,
                                             plural='lvmvolumegroups',
                                             version=version,
                                             resource=lvg)
        except Exception as e:
            print(f"{migrate_script} unable to create LVMVolumeGroup {lvg['metadata']['name']}, error: {e}")
            raise e
        print(
            f"{migrate_script} the LVMVolumeGroup {lvg['metadata']['name']} has been successfully created")

        print(f"{migrate_script} tries to update LvmVolumeGroupBackup {backup['metadata']['name']}")
        try:
            patch_backup_as_migrated(backup)
        except Exception as e:
            print(
                f"{migrate_script} unable to update LVMVolumeGroupBackup {backup['metadata']['name']}, error: {e}")
            raise e

    print(f"{migrate_script} every LVMVolumeGroup resources has been migrated")
    create_migration_secret()
    print(f"{migrate_script} successfully migrated LvmVolumeGroup to LVMVolumeGroup CRD")
    return
    ### End of LVMVolumeGroup CRD flow


def patch_backup_as_migrated(backup):
    try:
        kubernetes.client.CustomObjectsApi().patch_cluster_custom_object(group=group,
                                                                         version=version,
                                                                         plural='lvmvolumegroupbackups',
                                                                         name=backup['metadata']['name'],
                                                                         body={
                                                                             'metadata': {'labels': {
                                                                                 migration_completed_label: 'true'}}})
        print(
            f"{migrate_script} the LVMVolumeGroupBackup {backup['metadata']['name']} {migration_completed_label} was updated to true")
    except Exception as e:
        print(
            f"{migrate_script} unable to update LVMVolumeGroupBackup {backup['metadata']['name']}, error: {e}")
        raise e


def delete_old_lvg_crd():
    try:
        kubernetes.client.ApiextensionsV1Api().delete_custom_resource_definition(lvg_crd_name)

        # we need to be sure that old CRD was really removed before creating the new one
        for count in range(3):
            try:
                kubernetes.client.ApiextensionsV1Api().read_custom_resource_definition(lvg_crd_name)

                if count == 2:
                    raise Exception('LvmVolumeGroup CRD still exists in a cluster')
            except kubernetes.client.exceptions.ApiException as ae:
                if ae.status == 404:
                    return
            except Exception as e:
                print(f"{migrate_script} unable to read {lvg_crd_name}, error: {e}")
                raise e
    except Exception as e2:
        print(f"{migrate_script} unable to delete {lvg_crd_name}, error:{e2}")
        raise e2


def create_migration_secret():
    try:
        kubernetes.client.CoreV1Api().create_namespaced_secret(namespace='d8-sds-node-configurator',
                                                               body={'apiVersion': 'v1',
                                                                     'kind': 'Secret',
                                                                     'metadata': {
                                                                         'name': secret_name}})
    except kubernetes.client.api_client as ae:
        print(f"{migrate_script} unable to create migration secret, error: {ae}")
        raise ae


def create_new_lvg_crd():
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
                            kubernetes.client.ApiextensionsV1Api().create_custom_resource_definition(manifest)

                            # we need to be sure that the new LVMVolumeGroup CRD was created before creating LVMVolumeGroup resources
                            for count in range(3):
                                try:
                                    kubernetes.client.ApiextensionsV1Api().read_custom_resource_definition(lvg_crd_name)
                                    break
                                except kubernetes.client.exceptions.ApiException as ae:
                                    if ae.status == 404:
                                        if count == 2:
                                            raise ae

                                        time.sleep(1)
                                        pass
                                    else:
                                        print(f"{migrate_script} unable to read LvmVolumeGroupBackup CRD")
                                        raise ae

                            print(f"{migrate_script} {filename} was successfully created")
                            break
                        except Exception as e:
                            print(f"{migrate_script} unable to create LVMVolumeGroup CRD, error: {e}")
                            raise e


def configure_new_lvg_from_backup(backup):
    lvg = {'apiVersion': 'storage.deckhouse.io/v1alpha1',
           'kind': 'LVMVolumeGroup',
           'metadata': {
               'name':
                   backup['metadata'][
                       'name'],
               'labels':
                   backup['metadata'][
                       'labels'],
               'finalizers':
                   backup['metadata'][
                       'finalizers']},
           'spec': backup['spec']}

    lvg_name = lvg['metadata']['name']
    bd_names: List[str] = backup['spec']['blockDeviceNames']
    print(f"{migrate_script} extracted BlockDevice names: {bd_names} from LVMVolumeGroup {lvg_name}")
    del lvg['spec']['blockDeviceNames']
    lvg['spec']['local'] = {'nodeName': lvg['metadata']['labels']['kubernetes.io/hostname']}
    del lvg['metadata']['labels']['kubernetes.io/hostname']
    del lvg['metadata']['labels'][migration_completed_label]
    print(f"{migrate_script} LVMVolumeGroup {lvg_name} spec after adding the Local field: {lvg['spec']}")
    lvg['spec']['blockDeviceSelector'] = {
        'matchLabels': {'kubernetes.io/hostname': lvg['spec']['local']['nodeName']},
        'matchExpressions': [
            {'key': 'kubernetes.io/metadata.name', 'operator': 'In', 'values': bd_names}]
    }
    print(f"{migrate_script} LVMVolumeGroup {lvg_name} spec after adding the Selector: {lvg['spec']}")
    return lvg


def find_crds_root(hookpath):
    hooks_root = os.path.dirname(hookpath)
    module_root = os.path.dirname(hooks_root)
    crds_root = os.path.join(module_root, "crds")
    return crds_root


def create_or_update_custom_resource(group, plural, version, resource):
    try:
        kubernetes.client.CustomObjectsApi().create_cluster_custom_object(group=group,
                                                                          plural=plural,
                                                                          version=version,
                                                                          body={
                                                                              'apiVersion': f'{group}/{version}',
                                                                              'kind': resource['kind'],
                                                                              'metadata':
                                                                                  {
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
    except kubernetes.client.exceptions.ApiException as ae:
        # todo: перепроверить статус код ошибки
        if ae.status == 409:
            print(
                f"{migrate_script} the {resource['kind']} {resource['metadata']['name']} has been already created, update it")
            try:
                kubernetes.client.CustomObjectsApi().patch_cluster_custom_object(group=group,
                                                                                 plural=plural,
                                                                                 version=version,
                                                                                 name=resource['metadata']['name'],
                                                                                 body={
                                                                                     'metadata':
                                                                                         {
                                                                                             'name':
                                                                                                 resource['metadata'][
                                                                                                     'name'],
                                                                                             'labels':
                                                                                                 resource['metadata'][
                                                                                                     'labels'],
                                                                                             'finalizers':
                                                                                                 resource['metadata'][
                                                                                                     'finalizers'],
                                                                                         },
                                                                                     'spec': resource['spec']})
                print(f"{migrate_script} successfully updated LvmVolumeGroupBackup {resource['metadata']['name']}")
            except kubernetes.client.exceptions.ApiException as ae2:
                print(
                    f"{migrate_script} ApiException occurred while trying to update {resource['kind']} {resource['metadata']['name']}, error: {ae2}")
            except Exception as e:
                print(
                    f"{migrate_script} Exception occurred while trying to update {resource['kind']} {resource['metadata']['name']}, error: {e}")
        else:
            print(
                f"{migrate_script} unexpected error has been occurred while trying to create the {resource['kind']} {resource['metadata']['name']}, error: {ae}")
            raise ae
    except Exception as ex:
        print(
            f"{migrate_script} failed to create {resource['kind']} {resource['metadata']['name']}")
        raise ex


if __name__ == "__main__":
    hook.run(main, config=config)
