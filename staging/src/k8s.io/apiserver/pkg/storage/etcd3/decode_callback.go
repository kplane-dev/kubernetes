/*
Copyright 2024 The Kubernetes Authors.

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

package etcd3

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
)

type decodeCallbackKeyType struct{}

// DecodeCallback is called for each item decoded during GetList,
// providing the decoded object, its storage-relative key (etcd prefix
// stripped), and the etcd mod revision.
type DecodeCallback func(obj runtime.Object, storageKey string, modRevision int64)

// WithDecodeCallback returns a context that carries a DecodeCallback.
// The callback will be invoked for each item decoded in GetList.
func WithDecodeCallback(ctx context.Context, cb DecodeCallback) context.Context {
	return context.WithValue(ctx, decodeCallbackKeyType{}, cb)
}

func decodeCallbackFromContext(ctx context.Context) DecodeCallback {
	cb, _ := ctx.Value(decodeCallbackKeyType{}).(DecodeCallback)
	return cb
}
