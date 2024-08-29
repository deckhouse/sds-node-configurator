from typing import Any, List

from deckhouse import hook

import kubernetes

config = """
configVersion: v1
afterHelm: 10
"""

group = "storage.deckhouse.io"
plural = "lvmvolumegroups"
version = "v1alpha1"


def main(ctx: hook.Context):
    kubernetes.config.load_incluster_config()

    lvg_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group=group,
                                                                                    plural=plural,
                                                                                    version=version)

    for lvg in lvg_list["items"]:
        bd_names: List[str] = lvg["spec"]["blockDeviceNames"]
        del lvg["spec"]["blockDeviceNames"]
        lvg["spec"]["local"]["nodeName"] = lvg["status"]["nodes"][0]["name"]
        lvg["spec"]["blockDeviceSelector"]["matchLabels"]["kubernetes.io/hostname"] = lvg["spec"]["local"]["nodeName"]
        lvg["spec"]["blockDeviceSelector"]["matchExpressions"][0]["key"] = "kubernetes.io/metadata.name"
        lvg["spec"]["blockDeviceSelector"]["matchExpressions"][0]["operator"] = "in"
        lvg["spec"]["blockDeviceSelector"]["matchExpressions"][0]["values"] = bd_names

        kubernetes.client.CustomObjectsApi.patch_cluster_custom_object(group=group, plural=plural, version=version)


if __name__ == "__main__":
    hook.run(main, config=config)
