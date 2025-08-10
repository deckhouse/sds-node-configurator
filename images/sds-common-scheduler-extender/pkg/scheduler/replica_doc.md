# Replica endpoints

This document describes the endpoints to evaluate and cleanup DRBD replica reservations.

## POST /replica/evaluate
- Purpose: filter candidate LVGs (and thin pools) and return priorities for placing a replica; optionally reserve space in cache.
- Request body:
  - `candidatesThick` ([]string, optional): list of LVG names for thick provisioning.
  - `candidatesThin` ([{lvgName, thinPool}], optional): list of LVG+ThinPool pairs for thin provisioning.
  - `requestedBytes` (int64, required): size to reserve per candidate.
  - `replicaKey` (string, required): unique identifier of replica reservation.
  - `reserve` (bool, optional): if true, reserves space for all passed candidates.
- Response body:
  - `nodenames` ([]string): list of passed candidates. For thin entries the format is `LVG/ThinPool`.
  - `failedNodes` (map[string]string): reasons for failed candidates (key is LVG or `LVG/ThinPool`).
  - `priorities` ([{"host": string, "score": int}]): scoring where host is LVG or `LVG/ThinPool`.

Examples:

- Thick evaluation and reserve on two LVGs:
```json
{
  "candidatesThick": ["lvg-a", "lvg-b"],
  "requestedBytes": 10737418240,
  "replicaKey": "ns/rr-uid-1",
  "reserve": true
}
```

- Thin evaluation without reservation on two pools:
```json
{
  "candidatesThin": [
    {"lvgName": "lvg-a", "thinPool": "tp"},
    {"lvgName": "lvg-b", "thinPool": "tp"}
  ],
  "requestedBytes": 10737418240,
  "replicaKey": "ns/rr-uid-2",
  "reserve": false
}
```

## POST /replica/cleanup
- Purpose: clear reservations for a replica in cache after selecting a final target, or after successful creation.
- Request body:
  - `replicaKey` (string, required)
  - `selectedThickLVG` (string, optional): thick selection. Used with `clearNonSelected` to keep only this LVG, or with `clearSelected` to remove this LVG.
  - `selectedThinLVG` (string, optional) and `selectedThinPool` (string, optional): thin selection. Used with `clearNonSelected` to keep only this pair, or with `clearSelected` to remove this pair.
  - `clearNonSelected` (bool, optional): remove reservations on all candidates except the selected one.
  - `clearSelected` (bool, optional): remove reservation on the selected candidate (after successful creation).

Examples:

- Thick: keep only selected LVG (drop others):
```json
{
  "replicaKey": "ns/rr-uid-1",
  "selectedThickLVG": "lvg-b",
  "clearNonSelected": true
}
```

- Thick: remove reservation on selected LVG after creation:
```json
{
  "replicaKey": "ns/rr-uid-1",
  "selectedThickLVG": "lvg-b",
  "clearSelected": true
}
```

- Thin: keep only selected LVG/Pool (drop others):
```json
{
  "replicaKey": "ns/rr-uid-2",
  "selectedThinLVG": "lvg-a",
  "selectedThinPool": "tp",
  "clearNonSelected": true
}
```

- Thin: remove reservation on selected LVG/Pool after creation:
```json
{
  "replicaKey": "ns/rr-uid-2",
  "selectedThinLVG": "lvg-a",
  "selectedThinPool": "tp",
  "clearSelected": true
}
```


