# Design: Multicluster Storage Identity for Kubernetes API Server

## Status

Implemented — March 2026

## Authors

kplane-dev team

---

## Table of Contents

1. [Context and Motivation](#context-and-motivation)
2. [Background: What kplane Does Today](#background-what-kplane-does-today)
3. [Problems With the Current Model](#problems-with-the-current-model)
4. [Design Goals](#design-goals)
5. [Non-Goals](#non-goals)
6. [Architecture](#architecture)
7. [Implementation: kplane-dev/kubernetes (Fork)](#implementation-kplane-devkubernetes-fork)
8. [Implementation: kplane-dev/storage (New Repo)](#implementation-kplane-devstorage-new-repo)
9. [What Stays in the kplane apiserver Repo](#what-stays-in-the-kplane-apiserver-repo)
10. [Migration Path](#migration-path)
11. [Testing Strategy](#testing-strategy)
12. [Risks and Mitigations](#risks-and-mitigations)
13. [Future: Backend Swappability](#future-backend-swappability)

---

## Context and Motivation

kplane is a multicluster Kubernetes API server. A single kplane apiserver
process serves multiple virtual clusters ("virtual control planes" or VCPs),
each identified by a cluster ID. Clients connect to a particular cluster
via URL path prefix (`/clusters/<clusterID>/...`) and the server routes
requests, storage, RBAC, admission, and watch fan-out to the correct cluster
keyspace.

The core scalability insight is about **where cluster identity sits in
the etcd key path** and how that interacts with etcd's native prefix-watch
semantics.

### The Key Layout Trick

A naive multicluster model would be: create a separate watch per cluster
per resource type. With C clusters and R resource types, that is **C × R
watches**. At 200 clusters and 50 resource types, that is 10,000 etcd watch
streams — each with its own goroutine, gRPC stream, and memory overhead.

kplane avoids this entirely by pushing cluster identity **below** the
resource prefix in the etcd key hierarchy:

```
/<prefix>/<resource>/clusters/<clusterID>/[<namespace>/]<name>
```

This means a single prefix-watch on `/<prefix>/<resource>/clusters/`
naturally receives events for **all clusters** of that resource type,
because etcd prefix watches are recursive. The watch count drops from
**C × R** to just **R** — one watch per resource type, regardless of how
many clusters exist.

---

## Background: What kplane Does Today

### Etcd Key Layout

```
/<etcdPrefix>/<resourcePrefix>/clusters/<clusterID>/[<namespace>/]<name>
```

Examples:
```
/registry/pods/clusters/c-abc123/default/nginx
/registry/apiextensions.k8s.io/customresourcedefinitions/clusters/c-abc123/widgets.example.com
```

### The Identity Problem

The critical question: "given an object from a shared watch, which cluster
does it belong to?"

**Previously**, kplane worked around the upstream Cacher's lack of identity by:
- Wrapping `storage.Interface` to inject `InternalEntry` into watch events
- Maintaining etcd-backed revision→cluster lookup maps
- Using per-resource allowlists for storage-key lookups
- Retry loops for CRD bootstrap timing issues

This created a "hope the identity makes it through" model. These
workarounds have been replaced by the design described below.

---

## Problems With the Current Model

1. **No durable internal identity**: Objects lose cluster identity after
   cache eviction/relist. Every code path needs its own resolution strategy.

2. **Fragile watch/cache identity propagation**: The upstream `store.Element`
   has no place for cluster ID. Identity is bolted on externally.

3. **Growing resource allowlists**: Manual per-resource allowlists control
   which types get etcd fallback lookups. Inherently fragile.

4. **Status update misrouting**: Internal controllers running via loopback
   client require etcd queries to determine which cluster to update.

---

## Design Goals

1. **Durable internal identity**: every object in the cacher pipeline
   carries its cluster ID at all times.

2. **Zero client-visible metadata**: no labels, annotations, or new fields
   on objects returned to API clients.

3. **Minimal upstream fork surface**: changes are small, additive, and
   confined to the cacher's internal types and output boundaries.

4. **Preserve shared informer model**: one watch/cache per resource kind.

5. **Backend flexibility**: the fork does not couple to etcd. Backend
   swapping remains independent of identity propagation.

---

## Non-Goals

- Changing the external Kubernetes API schema.
- Introducing per-cluster watch caches or informer factories.
- Modifying client-go, kubectl, or other client tooling.
- Upstream acceptance of these changes (this is a focused fork).

---

## Architecture

### Three-Layer Architecture

The identity system is split across three repos with clear responsibilities:

```
┌──────────────────────────────────────────────────────┐
│  kplane-dev/apiserver                                │
│  - RESTOptionsDecorator wires storage hooks          │
│  - InternalEntry unwrapping at API boundary          │
│  - Multicluster routing, RBAC, admission             │
└─────────────┬────────────────────────────────────────┘
              │ uses StorageDecorator from ↓
┌─────────────▼────────────────────────────────────────┐
│  kplane-dev/storage                                  │
│  - ObjectWithClusterIdentity envelope type           │
│  - KeyLayout / cluster extraction from keys          │
│  - StorageDecorator factory (configures all hooks)   │
│  - UnwrapClusterIdentity() helper                    │
└─────────────┬────────────────────────────────────────┘
              │ plugs into ↓
┌─────────────▼────────────────────────────────────────┐
│  kplane-dev/kubernetes (fork)                        │
│                                                      │
│  etcd3 layer:                                        │
│  - WrapDecodedObject hook on storagebackend.Config   │
│  - etcd3 store + watcher wrap decoded objects with   │
│    their storage key before the cacher sees them     │
│  - Decode callback for list identity resolution      │
│                                                      │
│  Cacher layer:                                       │
│  - ClusterID on store.Element + watchCacheEvent      │
│  - IdentityFromKey: key → clusterID extraction       │
│  - UnwrapObject: strips envelope before cache store  │
│  - processEvent + Replace populate ClusterID         │
│  - ListerWatcher annotates list items with identity  │
│                                                      │
│  Watch output boundary:                              │
│  - WrapWatchObject on cacheWatcher wraps objects     │
│    in ObjectWithClusterIdentity for consumers        │
└──────────────────────────────────────────────────────┘
```

### Data Flow: Watch Path

```
etcd watch event arrives
    │
    ▼
etcd3 watcher decodes bytes → runtime.Object
  └─ WrapDecodedObject(obj, storageKey) → wrapped object
     (attaches storage key to object before key is lost)
    │
    ▼
Reflector receives watch.Event
  (type check disabled — sees wrapper type)
    │
    ▼
watchCache.processEvent()
  ├─ keyFunc(object) → key
  ├─ IdentityFromKey(key) → clusterID
  ├─ UnwrapObject(object) → raw Kubernetes object
  ├─ store.Element{Key, Object, Labels, Fields, ClusterID}
  └─ watchCacheEvent{..., ClusterID}
    │
    ▼
Cacher dispatches to cacheWatchers
    │
    ▼
cacheWatcher.convertToWatchEvent()
  ├─ getMutableObject(event.Object) → obj
  ├─ WrapWatchObject(obj, key, clusterID) → ObjectWithClusterIdentity
  └─ watch.Event{Object: ObjectWithClusterIdentity{...}}
    │
    ▼
Consumer receives watch.Event
  ├─ UnwrapClusterIdentity(event.Object) → (innerObject, clusterID)
  └─ Route/filter by clusterID
```

### Data Flow: List Path

```
ListerWatcher.List() called by Reflector
    │
    ▼
etcd3 store.GetList() decodes items
  └─ Decode callback captures (obj, storageKey, modRevision)
     for each decoded item
    │
    ▼
ListerWatcher.annotateListItems()
  ├─ Matches decoded items to their storage keys
  ├─ IdentityFromKey(storageKey) → clusterID per item
  └─ WrapObject(item, storageKey, clusterID) → wrapped item
    │
    ▼
watchCache.Replace() receives wrapped list
  ├─ IdentityFromKey(key) → clusterID
  ├─ UnwrapObject(object) → raw Kubernetes object
  └─ store.Element{Key, Object, Labels, Fields, ClusterID}
```

### Identity for List vs Watch

**Watch**: Identity flows end-to-end. The etcd3 layer wraps decoded objects
with their storage key (`WrapDecodedObject`). The watchCache extracts the
cluster ID, unwraps the envelope, and stores raw objects with `ClusterID`
on the `store.Element`. At the output boundary, `WrapWatchObject` re-wraps
in `ObjectWithClusterIdentity` for consumers.

**List (initial cache population)**: The ListerWatcher uses a decode
callback to capture storage keys during `GetList`, then annotates each
item with its cluster identity before passing to `watchCache.Replace()`.
The watchCache unwraps and stores raw objects with `ClusterID`.

**List (per-cluster API request)**: Identity is known from the request
context. The key prefix (e.g., `/pods/clusters/c1/`) ensures all results
belong to one cluster. No wrapping needed.

**List (cross-cluster)**: The cacher's internal `store.Element` objects
have `ClusterID` populated. Cross-cluster consumers access identity via
the watch path or the cache's internal store.

---

## Implementation: kplane-dev/kubernetes (Fork)

The fork touches three layers of the apiserver storage stack. All changes
are additive and hook-based — when hooks are nil, behavior is identical
to upstream.

### Layer 1: etcd3 Storage Backend

The etcd3 store and watcher gain a `WrapDecodedObject` hook, configured
via `storagebackend.Config`. When set, the etcd3 layer wraps every decoded
object with its storage key before passing it up to the Reflector. This
is necessary because the upstream watch pipeline discards storage keys at
the etcd3→Reflector boundary, and multicluster identity depends on the key.

For list operations, a context-based decode callback allows the
ListerWatcher to capture the storage key for each decoded item during
`GetList`.

### Layer 2: Cacher Internals

The Cacher `Config` gains three hooks:

```go
type Config struct {
    // ... existing fields ...

    // Derives cluster ID from a storage key. nil for single-cluster.
    IdentityFromKey func(key string) string

    // Wraps objects with identity at the watch output boundary. nil for single-cluster.
    WrapWatchObject func(object runtime.Object, key string, clusterID string) runtime.Object

    // Strips the identity envelope before cache storage. nil for single-cluster.
    UnwrapObject func(obj runtime.Object) runtime.Object
}
```

**Internal types** carry identity:
- `store.Element` gains a `ClusterID` field, populated in `processEvent`
  and `Replace` via `IdentityFromKey`.
- `watchCacheEvent` gains a `ClusterID` field, propagated through the
  ring buffer and `watchCacheInterval`.

**Unwrap before storage**: The watchCache calls `UnwrapObject` before
storing, so that `Get`/`GetList` return raw Kubernetes objects. A
post-unwrap type check replaces the Reflector's type check (which is
disabled because the Reflector sees the wrapper type).

**ListerWatcher identity annotation**: For initial list population, the
ListerWatcher uses the decode callback to build a key map, then wraps
each item with its cluster identity before passing to `watchCache.Replace()`.

### Layer 3: Watch Output Boundary

The `cacheWatcher` applies `WrapWatchObject` to every outgoing watch event
(Added, Modified, Deleted), wrapping the raw object in
`ObjectWithClusterIdentity` with the key and cluster ID from the
`watchCacheEvent`. Bookmark events pass through unwrapped.

### Fork Surface

Changes span the etcd3 store/watcher, storagebackend config, cacher
config, watchCache, ListerWatcher, cacheWatcher, and watchCacheInterval.
All are additive fields and nil-guarded hooks — no upstream behavior is
altered when hooks are unconfigured.

---

## Implementation: kplane-dev/storage (New Repo)

### ObjectWithClusterIdentity

```go
type ObjectWithClusterIdentity struct {
    Object     runtime.Object
    ClusterID  string
    StorageKey string
}
```

Implements `runtime.Object`. Used as the watch.Event.Object type when
identity hooks are configured.

### KeyLayout

```go
type KeyLayout struct {
    ClusterSegment string  // default: "clusters"
}
```

Provides:
- `IdentityFromKey()` — returns a `func(key string) string` for the fork's Config
- `ClusterFromKey(key)` — one-shot extraction
- `KindRootPrefix(resourcePrefix)` — e.g., `/pods/clusters/`
- `PerClusterPrefix(resourcePrefix, clusterID)` — e.g., `/pods/clusters/c1/`

### StorageWithClusterIdentity

Drop-in replacement for `registry.StorageWithCacher()` that configures:
- `WrapDecodedObject` on the etcd3 backend (storage key attachment)
- `IdentityFromKey` from the KeyLayout
- `WrapWatchObject` that wraps in ObjectWithClusterIdentity
- `UnwrapObject` that strips the envelope before cache storage

### UnwrapClusterIdentity

```go
func UnwrapClusterIdentity(obj runtime.Object) (inner runtime.Object, clusterID string)
```

Primary API for consumers. Handles both wrapped and unwrapped objects.

---

## What Stays in the kplane apiserver Repo

| Concern | Where |
|---------|-------|
| URL routing and cluster context injection | `pkg/multicluster/options.go` |
| Storage key rewrite (`clusters/<cid>/...`) | `pkg/multicluster/storage.go` |
| Shared informer fan-out with per-cluster filtering | `pkg/multicluster/scopedinformer/` |
| RBAC projection and cluster-scoped auth | `pkg/multicluster/auth/` |
| CRD lifecycle management | `pkg/multicluster/bootstrap/` |

### What Gets Deleted From kplane apiserver

Once the fork provides durable identity, the following complexity is removed:

- `requiresStorageClusterLookup()` resource allowlist
- `lookupClusterFromStorage()` etcd query fallback
- `clusterHintFromDestination()` / `clusterHintFromKey()` update rewriting
- `storagekeyaware/backend.go` revision-cluster memory maps
- `revisionClusterIndex()` list priming
- All direct etcd client usage in the storage decorator

---

## Migration Path

### Phase 1: Fork + Storage Repo (Done)

1. Fork changes applied to `kplane-dev/kubernetes`.
2. `kplane-dev/storage` repo created with types, decorator, and tests.
3. All upstream cacher tests pass.
4. Fork identity unit tests pass.

### Phase 2: Wire Into kplane apiserver (Done)

1. Updated `kplane-dev/apiserver` go.mod to use forked kubernetes modules.
2. Replaced `StorageWithCacher()` with `storage.StorageWithClusterIdentity()`.
3. Updated shared informer event handlers to use `UnwrapClusterIdentity()`.
4. All 26 smoke tests pass with 1.79 MiB/VCP heap overhead.

### Phase 3: Remove Fallback Complexity (Done)

1. Deleted etcd lookup fallbacks, resource allowlist, revision-cluster
   memory maps, CRD-specific resolver retry loops (-3,699 lines).
2. Identity comes from cache natively — no etcd queries needed.
3. Validated as drop-in envtest replacement: cluster-api-exp full
   `./internal/...` test suite passes (36 packages, 0 failures).

### Phase 4: Harden and Validate

1. Add invariant assertions: identity must be non-empty at fan-out points.
2. Add targeted tests for duplicate-name cross-cluster objects.
3. Benchmark with 200+ VCPs to verify no performance regression.

---

## Testing Strategy

### Upstream Tests (Must Pass)

All existing tests in `kubernetes/kubernetes` pass without modification.
Fork changes are additive and default to no-ops.

### Fork Unit Tests (identity_test.go)

Tests in the fork's cacher package covering key extraction, identity
propagation through `processEvent` and `Replace`, single-cluster no-op
mode, watch wrapping for all event types, and cache element identity.

### kplane-dev/storage Tests

Unit tests for `ObjectWithClusterIdentity` type correctness, `UnwrapClusterIdentity`
helper, and `KeyLayout` key parsing. E2E tests verifying identity flows through
a real cacher with multi-cluster watch and all event types.

### Integration: kplanetest + cluster-api

The built kplane-apiserver binary passes:
- kplanetest conformance suite (2/2)
- cluster-api-exp `./internal/...` (36 packages, 0 failures)

### Apiserver Smoke Tests

26 smoke tests passing with 1.79 MiB/VCP heap overhead, covering CRUD,
watch, CRD lifecycle, RBAC, admission, and multicluster fan-out.

---

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Fork drift from upstream | Changes are additive hook fields on stable internal types (`store.Element`, `watchCacheEvent`, `Config`). All nil-guarded. Rebase on each upstream release tag. |
| Watch semantics regression | Full upstream test suite passes. Fork identity tests + cluster-api-exp integration suite (36 packages) validate end-to-end. |
| Performance from identity propagation | Identity is a string copy from key parsing. No allocation for single-cluster (hooks are nil). Validated at 1.79 MiB/VCP. |
| cachingObject wrapping | Watch consumers use `meta.Accessor` or `UnwrapClusterIdentity` which handle both raw objects and cachingObject wrappers. |

---

## Future: Backend Swappability

The identity hooks are split across two boundaries:

1. **`storagebackend.Config.WrapDecodedObject`** — implemented by the
   backend (today: etcd3 store and watcher). Any alternative backend
   would need to call this hook to attach storage keys to decoded objects.

2. **Cacher `Config` hooks** (`IdentityFromKey`, `WrapWatchObject`,
   `UnwrapObject`) — backend-agnostic. These operate on the objects
   the cacher receives from the Reflector, regardless of origin.

Backend swapping means replacing the `storage.Interface` implementation
while ensuring the new backend calls `WrapDecodedObject` on decoded
objects. The cacher and its identity hooks work identically regardless
of backend.

```go
// Today: etcd backend (via generic.NewRawStorage)
decorator := StorageWithClusterIdentity(cfg)

// Future: custom backend
decorator := StorageWithClusterIdentityAndBackend(cfg, myBackendFactory)
```

k3s/Kine operates at a different layer (etcd gRPC protocol), which is
useful for drop-in SQL replacement but doesn't help with identity. The
`storage.Interface` layer is the right boundary for kplane because it
gives control over key layout, identity semantics, and cluster-aware
operations that a protocol-level shim would lose.
