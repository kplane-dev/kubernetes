/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cacher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher/store"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/client-go/tools/cache"
)

// identityEnvelope wraps a runtime.Object with cluster identity metadata.
// This mirrors what kplane-dev/storage will provide as ObjectWithClusterIdentity.
type identityEnvelope struct {
	Object    runtime.Object
	ClusterID string
	Key       string
}

func (e *identityEnvelope) GetObjectKind() schema.ObjectKind {
	return e.Object.GetObjectKind()
}

func (e *identityEnvelope) DeepCopyObject() runtime.Object {
	return &identityEnvelope{
		Object:    e.Object.DeepCopyObject(),
		ClusterID: e.ClusterID,
		Key:       e.Key,
	}
}

// clusterFromKey extracts cluster ID from a key with layout:
// /pods/clusters/<clusterID>/<namespace>/<name>
func clusterFromKey(key string) string {
	const marker = "/clusters/"
	idx := strings.Index(key, marker)
	if idx < 0 {
		return ""
	}
	rest := key[idx+len(marker):]
	if slash := strings.Index(rest, "/"); slash >= 0 {
		return rest[:slash]
	}
	return rest
}

func wrapWithIdentity(object runtime.Object, key string, clusterID string) runtime.Object {
	return &identityEnvelope{
		Object:    object,
		ClusterID: clusterID,
		Key:       key,
	}
}

// podNameFromObject extracts the pod name from an object that might be
// wrapped in a cachingObject (the Cacher's internal serialization wrapper).
// In production, consumers use meta.Accessor which works with any runtime.Object.
func podNameFromObject(t *testing.T, obj runtime.Object) string {
	t.Helper()
	if pod, ok := obj.(*example.Pod); ok {
		return pod.Name
	}
	// cachingObject wraps the real object; use meta.Accessor which works
	// with any runtime.Object including cachingObject.
	accessor, err := meta.Accessor(obj)
	if err != nil {
		t.Fatalf("could not get accessor from %T: %v", obj, err)
	}
	return accessor.GetName()
}

// clusterKeyFunc computes a key that includes the cluster segment.
// Key format: /pods/clusters/<cluster>/<namespace>/<name>
// The cluster is embedded in the pod's labels["cluster"] for test purposes.
func clusterKeyFunc(obj runtime.Object) (string, error) {
	pod, ok := obj.(*example.Pod)
	if !ok {
		return "", fmt.Errorf("not a pod")
	}
	cluster := pod.Labels["cluster"]
	if cluster == "" {
		cluster = "default"
	}
	return fmt.Sprintf("/pods/clusters/%s/%s/%s", cluster, pod.Namespace, pod.Name), nil
}

// testSetupWithIdentity creates a Cacher configured with identity hooks.
func testSetupWithIdentity(t *testing.T) (context.Context, *CacheDelegator, storage.Interface, func()) {
	server, etcdStorage := newEtcdTestStorage(t, etcd3testing.PathPrefix())

	listErrors := 1
	if clientfeatures.FeatureGates().Enabled(clientfeatures.WatchListClient) {
		listErrors = 0
	}
	wrappedStorage := &storagetesting.StorageInjectingListErrors{
		Interface: etcdStorage,
		Errors:    listErrors,
	}

	prefix := "/pods/clusters/"
	config := Config{
		Storage:        wrappedStorage,
		Versioner:      storage.APIObjectVersioner{},
		GroupResource:  schema.GroupResource{Resource: "pods"},
		EventsHistoryWindow: DefaultEventFreshDuration,
		ResourcePrefix: prefix,
		KeyFunc: func(obj runtime.Object) (string, error) {
			return clusterKeyFunc(obj)
		},
		GetAttrsFunc: GetPodAttrs,
		NewFunc:      newPod,
		NewListFunc:  newPodList,
		Codec:        codecs.LegacyCodec(examplev1.SchemeGroupVersion),
		// Identity hooks:
		IdentityFromKey: clusterFromKey,
		WrapWatchObject: wrapWithIdentity,
	}

	cacher, err := NewCacherFromConfig(config)
	if err != nil {
		t.Fatalf("Failed to initialize cacher: %v", err)
	}
	ctx := context.Background()

	if err := wait.PollInfinite(100*time.Millisecond, wrappedStorage.ErrorsConsumed); err != nil {
		t.Fatalf("Failed to inject list errors: %v", err)
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.ResilientWatchCacheInitialization) {
		if err := cacher.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}

	delegator := NewCacheDelegator(cacher, wrappedStorage)
	terminate := func() {
		delegator.Stop()
		cacher.Stop()
		server.Terminate(t)
	}

	return ctx, delegator, etcdStorage, terminate
}

func makePodWithCluster(name, namespace, cluster string) *example.Pod {
	return &example.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"cluster": cluster},
		},
	}
}

// TestIdentityFromKey_ClusterExtraction verifies that clusterFromKey correctly
// extracts cluster ID from the key layout.
func TestIdentityFromKey_ClusterExtraction(t *testing.T) {
	tests := []struct {
		key      string
		expected string
	}{
		{"/pods/clusters/c1/default/nginx", "c1"},
		{"/pods/clusters/c2/kube-system/coredns", "c2"},
		{"/pods/clusters/my-cluster/default/app", "my-cluster"},
		{"/pods/default/nginx", ""},
		{"", ""},
	}
	for _, tt := range tests {
		got := clusterFromKey(tt.key)
		if got != tt.expected {
			t.Errorf("clusterFromKey(%q) = %q, want %q", tt.key, got, tt.expected)
		}
	}
}

// TestIdentityPropagation_ProcessEvent verifies that ClusterID is set on
// store.Element and watchCacheEvent when IdentityFromKey is configured.
func TestIdentityPropagation_ProcessEvent(t *testing.T) {
	var capturedEvent *watchCacheEvent
	handler := func(event *watchCacheEvent) {
		capturedEvent = event
	}

	wc := newTestWatchCache(10, DefaultEventFreshDuration, &cache.Indexers{})
	wc.eventHandler = handler
	wc.identityFromKey = clusterFromKey
	wc.keyFunc = clusterKeyFunc
	wc.getAttrsFunc = GetPodAttrs

	pod := makePodWithCluster("nginx", "default", "c1")
	pod.ResourceVersion = "1"

	err := wc.processEvent(watch.Event{
		Type:   watch.Added,
		Object: pod,
	}, 1, func(elem *store.Element) error {
		// Verify Element has ClusterID set
		if elem.ClusterID != "c1" {
			t.Errorf("Element.ClusterID = %q, want %q", elem.ClusterID, "c1")
		}
		if elem.Key != "/pods/clusters/c1/default/nginx" {
			t.Errorf("Element.Key = %q, want %q", elem.Key, "/pods/clusters/c1/default/nginx")
		}
		return wc.store.Update(elem)
	})
	if err != nil {
		t.Fatalf("processEvent failed: %v", err)
	}

	// Verify watchCacheEvent has ClusterID set
	if capturedEvent == nil {
		t.Fatal("event handler was not called")
	}
	if capturedEvent.ClusterID != "c1" {
		t.Errorf("watchCacheEvent.ClusterID = %q, want %q", capturedEvent.ClusterID, "c1")
	}
}

// TestIdentityPropagation_Replace verifies that ClusterID is set on
// store.Elements during Replace (initial list population).
func TestIdentityPropagation_Replace(t *testing.T) {
	wc := newTestWatchCache(10, DefaultEventFreshDuration, &cache.Indexers{})
	wc.identityFromKey = clusterFromKey
	wc.keyFunc = clusterKeyFunc
	wc.getAttrsFunc = GetPodAttrs

	pods := []interface{}{
		makePodWithCluster("nginx", "default", "c1"),
		makePodWithCluster("coredns", "kube-system", "c2"),
		makePodWithCluster("app", "default", "c1"),
	}

	err := wc.Replace(pods, "10")
	if err != nil {
		t.Fatalf("Replace failed: %v", err)
	}

	// Check all stored elements have correct ClusterID
	items := wc.store.List()
	clusterCounts := map[string]int{}
	for _, item := range items {
		elem := item.(*store.Element)
		clusterCounts[elem.ClusterID]++
		if elem.ClusterID == "" {
			t.Errorf("Element for key %q has empty ClusterID", elem.Key)
		}
		// Verify ClusterID matches what's in the key
		expected := clusterFromKey(elem.Key)
		if elem.ClusterID != expected {
			t.Errorf("Element.ClusterID = %q, want %q (key: %s)", elem.ClusterID, expected, elem.Key)
		}
	}

	if clusterCounts["c1"] != 2 {
		t.Errorf("expected 2 elements for c1, got %d", clusterCounts["c1"])
	}
	if clusterCounts["c2"] != 1 {
		t.Errorf("expected 1 element for c2, got %d", clusterCounts["c2"])
	}
}

// TestIdentityPropagation_NoIdentityHook verifies that when IdentityFromKey
// is nil (single-cluster mode), ClusterID remains empty and nothing breaks.
func TestIdentityPropagation_NoIdentityHook(t *testing.T) {
	wc := newTestWatchCache(10, DefaultEventFreshDuration, &cache.Indexers{})
	// identityFromKey is nil by default
	wc.keyFunc = clusterKeyFunc
	wc.getAttrsFunc = GetPodAttrs

	pod := makePodWithCluster("nginx", "default", "c1")
	pod.ResourceVersion = "1"

	err := wc.processEvent(watch.Event{
		Type:   watch.Added,
		Object: pod,
	}, 1, func(elem *store.Element) error {
		if elem.ClusterID != "" {
			t.Errorf("Element.ClusterID = %q, want empty (no identity hook)", elem.ClusterID)
		}
		return wc.store.Update(elem)
	})
	if err != nil {
		t.Fatalf("processEvent failed: %v", err)
	}
}

// TestIdentityPropagation_WatchWrapObject verifies that watch events are
// wrapped with identity envelopes when WrapWatchObject is configured.
func TestIdentityPropagation_WatchWrapObject(t *testing.T) {
	ctx, delegator, rawStorage, terminate := testSetupWithIdentity(t)
	defer terminate()

	// Create pods in two different clusters by writing directly to etcd
	// with cluster-prefixed keys.
	pod1 := makePodWithCluster("nginx", "default", "c1")
	pod2 := makePodWithCluster("coredns", "kube-system", "c2")

	out1 := &example.Pod{}
	err := rawStorage.Create(ctx, "/pods/clusters/c1/default/nginx", pod1, out1, 0)
	if err != nil {
		t.Fatalf("Failed to create pod1: %v", err)
	}
	out2 := &example.Pod{}
	err = rawStorage.Create(ctx, "/pods/clusters/c2/kube-system/coredns", pod2, out2, 0)
	if err != nil {
		t.Fatalf("Failed to create pod2: %v", err)
	}

	// Start watching from the beginning
	w, err := delegator.Watch(ctx, "/pods/clusters/", storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       storage.Everything,
		Recursive:       true,
	})
	if err != nil {
		t.Fatalf("Failed to start watch: %v", err)
	}
	defer w.Stop()

	// Collect events with a timeout
	events := make(map[string]*identityEnvelope)
	timeout := time.After(10 * time.Second)
	for len(events) < 2 {
		select {
		case evt, ok := <-w.ResultChan():
			if !ok {
				t.Fatal("watch channel closed unexpectedly")
			}
			if evt.Type == watch.Bookmark {
				continue
			}
			envelope, ok := evt.Object.(*identityEnvelope)
			if !ok {
				t.Fatalf("expected *identityEnvelope, got %T", evt.Object)
			}
			name := podNameFromObject(t, envelope.Object)
			events[name] = envelope
		case <-timeout:
			t.Fatalf("timed out waiting for watch events, got %d events", len(events))
		}
	}

	// Verify identity on each event
	if env, ok := events["nginx"]; ok {
		if env.ClusterID != "c1" {
			t.Errorf("nginx event ClusterID = %q, want %q", env.ClusterID, "c1")
		}
	} else {
		t.Error("missing watch event for nginx")
	}

	if env, ok := events["coredns"]; ok {
		if env.ClusterID != "c2" {
			t.Errorf("coredns event ClusterID = %q, want %q", env.ClusterID, "c2")
		}
	} else {
		t.Error("missing watch event for coredns")
	}
}

// TestIdentityPropagation_WatchModifyDelete verifies that Modified and Deleted
// watch events also carry identity.
func TestIdentityPropagation_WatchModifyDelete(t *testing.T) {
	ctx, delegator, rawStorage, terminate := testSetupWithIdentity(t)
	defer terminate()

	// Create a pod
	pod := makePodWithCluster("nginx", "default", "c1")
	out := &example.Pod{}
	err := rawStorage.Create(ctx, "/pods/clusters/c1/default/nginx", pod, out, 0)
	if err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Start watching
	w, err := delegator.Watch(ctx, "/pods/clusters/", storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       storage.Everything,
		Recursive:       true,
	})
	if err != nil {
		t.Fatalf("Failed to start watch: %v", err)
	}
	defer w.Stop()

	// Wait for the Added event
	waitForEvent := func(expectedType watch.EventType) *identityEnvelope {
		timeout := time.After(10 * time.Second)
		for {
			select {
			case evt, ok := <-w.ResultChan():
				if !ok {
					t.Fatal("watch channel closed")
				}
				if evt.Type == watch.Bookmark {
					continue
				}
				if evt.Type != expectedType {
					continue
				}
				envelope, ok := evt.Object.(*identityEnvelope)
				if !ok {
					t.Fatalf("expected *identityEnvelope for %v event, got %T", expectedType, evt.Object)
				}
				return envelope
			case <-timeout:
				t.Fatalf("timed out waiting for %v event", expectedType)
			}
		}
	}

	addEnv := waitForEvent(watch.Added)
	if addEnv.ClusterID != "c1" {
		t.Errorf("Added event ClusterID = %q, want c1", addEnv.ClusterID)
	}

	// Modify the pod
	err = rawStorage.GuaranteedUpdate(ctx, "/pods/clusters/c1/default/nginx", &example.Pod{}, false, nil,
		func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			p := input.(*example.Pod)
			p.Labels["modified"] = "true"
			return p, nil, nil
		}, out)
	if err != nil {
		t.Fatalf("Failed to update pod: %v", err)
	}

	modEnv := waitForEvent(watch.Modified)
	if modEnv.ClusterID != "c1" {
		t.Errorf("Modified event ClusterID = %q, want c1", modEnv.ClusterID)
	}

	// Delete the pod
	err = rawStorage.Delete(ctx, "/pods/clusters/c1/default/nginx", &example.Pod{}, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
	if err != nil {
		t.Fatalf("Failed to delete pod: %v", err)
	}

	delEnv := waitForEvent(watch.Deleted)
	if delEnv.ClusterID != "c1" {
		t.Errorf("Deleted event ClusterID = %q, want c1", delEnv.ClusterID)
	}
}

// TestIdentityPropagation_CacheElementsHaveClusterID verifies that after
// objects flow through the cacher, the internal cache Elements have ClusterID.
func TestIdentityPropagation_CacheElementsHaveClusterID(t *testing.T) {
	ctx, delegator, rawStorage, terminate := testSetupWithIdentity(t)
	defer terminate()

	// Create pods in two clusters
	for _, tc := range []struct {
		name, ns, cluster string
	}{
		{"nginx", "default", "c1"},
		{"app", "default", "c1"},
		{"coredns", "kube-system", "c2"},
	} {
		pod := makePodWithCluster(tc.name, tc.ns, tc.cluster)
		key := fmt.Sprintf("/pods/clusters/%s/%s/%s", tc.cluster, tc.ns, tc.name)
		if err := rawStorage.Create(ctx, key, pod, &example.Pod{}, 0); err != nil {
			t.Fatalf("Failed to create %s: %v", tc.name, err)
		}
	}

	// Wait for the cache to catch up by listing
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		list := &example.PodList{}
		if err := delegator.GetList(ctx, "/pods/clusters/", storage.ListOptions{
			ResourceVersion: "0",
			Predicate:       storage.Everything,
			Recursive:       true,
		}, list); err != nil {
			return false, nil
		}
		return len(list.Items) == 3, nil
	})
	if err != nil {
		t.Fatalf("Cache didn't populate: %v", err)
	}

	// Now check the internal cache store directly
	items := delegator.cacher.watchCache.store.List()
	if len(items) != 3 {
		t.Fatalf("expected 3 items in cache, got %d", len(items))
	}

	clusterMap := map[string][]string{} // clusterID -> list of pod names
	for _, item := range items {
		elem := item.(*store.Element)
		if elem.ClusterID == "" {
			t.Errorf("Element for key %q has empty ClusterID", elem.Key)
		}
		pod := elem.Object.(*example.Pod)
		clusterMap[elem.ClusterID] = append(clusterMap[elem.ClusterID], pod.Name)
	}

	if len(clusterMap["c1"]) != 2 {
		t.Errorf("expected 2 pods in c1, got %d: %v", len(clusterMap["c1"]), clusterMap["c1"])
	}
	if len(clusterMap["c2"]) != 1 {
		t.Errorf("expected 1 pod in c2, got %d: %v", len(clusterMap["c2"]), clusterMap["c2"])
	}
}
