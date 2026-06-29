/*
Copyright 2016 The Kubernetes Authors.

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

package factory

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

// DestroyFunc is to destroy any resources used by the storage returned in Create() together.
type DestroyFunc func()

// Backend is the dispatch contract used by the factory's Create / health /
// ready / prober / monitor entry points for non-etcd3 storage types. A
// process registers a Backend implementation under a name; that name is then
// selected at runtime via storagebackend.Config.Type.
//
// kplane addition (KPEP-0001): lets a single registration cover every
// factory.Create callsite in the apiserver — CR storage, master/peer endpoint
// leases, and the service IP/NodePort allocators — without per-callsite
// patches.
type Backend interface {
	Create(c storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object, resourcePrefix string) (storage.Interface, DestroyFunc, error)
	CreateHealthCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error)
	CreateReadyCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error)
	CreateProber(c storagebackend.Config) (Prober, error)
	CreateMonitor(c storagebackend.Config) (metrics.Monitor, error)
}

var (
	registryMu sync.RWMutex
	registry   = map[string]Backend{}
)

// Register installs b under name. Panics on duplicate or on use of a reserved
// name. Call from process init (e.g. apiserver main) before the first
// Create call.
func Register(name string, b Backend) {
	registryMu.Lock()
	defer registryMu.Unlock()
	switch name {
	case "", storagebackend.StorageTypeETCD2, storagebackend.StorageTypeETCD3:
		panic(fmt.Sprintf("storage backend name %q is reserved", name))
	}
	if _, dup := registry[name]; dup {
		panic(fmt.Sprintf("storage backend %q already registered", name))
	}
	registry[name] = b
}

func lookup(name string) (Backend, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	b, ok := registry[name]
	return b, ok
}

// IsRegistered reports whether name is a registered backend. Used by
// EtcdOptions.Validate so --storage-backend=<name> passes validation when
// the apiserver has registered <name> at startup.
func IsRegistered(name string) bool {
	_, ok := lookup(name)
	return ok
}

// Create creates a storage backend based on given config.
func Create(c storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object, resourcePrefix string) (storage.Interface, DestroyFunc, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3Storage(c, newFunc, newListFunc, resourcePrefix)
	default:
		if b, ok := lookup(c.Type); ok {
			return b.Create(c, newFunc, newListFunc, resourcePrefix)
		}
		return nil, nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// CreateHealthCheck creates a healthcheck function based on given config.
func CreateHealthCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3HealthCheck(c, stopCh)
	default:
		if b, ok := lookup(c.Type); ok {
			return b.CreateHealthCheck(c, stopCh)
		}
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

func CreateReadyCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ReadyCheck(c, stopCh)
	default:
		if b, ok := lookup(c.Type); ok {
			return b.CreateReadyCheck(c, stopCh)
		}
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

func CreateProber(c storagebackend.Config) (Prober, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ProberMonitor(c)
	default:
		if b, ok := lookup(c.Type); ok {
			return b.CreateProber(c)
		}
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

func CreateMonitor(c storagebackend.Config) (metrics.Monitor, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ProberMonitor(c)
	default:
		if b, ok := lookup(c.Type); ok {
			return b.CreateMonitor(c)
		}
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// Prober is an interface that defines the Probe function for doing etcd readiness/liveness checks.
type Prober interface {
	Probe(ctx context.Context) error
	Close() error
}
