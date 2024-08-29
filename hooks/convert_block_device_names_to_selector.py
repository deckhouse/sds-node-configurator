from typing import Any, List

from deckhouse import hook

import kubernetes


def main(ctx: hook.Context):
    kubernetes.config.load_incluster_config()

    lvg_list: Any = kubernetes.client.CustomObjectsApi().list_cluster_custom_object(group="storage.deckhouse.io",
                                                                              plural="lvmvolumegroups",
                                                                              version="v1alpha1")

    for lvg in lvg_list["items"]:
        bdNames: List[str] = lvg["spec"]["blockDeviceNames"]
        lvg["spec"].pop("blockDeviceNames")
        lvg["spec"]["local"]["nodeName"] = lvg["status"]["nodes"][0]["name"]
        lvg["spec"]["blockDeviceSelector"]["matchLabels"] = {""}

