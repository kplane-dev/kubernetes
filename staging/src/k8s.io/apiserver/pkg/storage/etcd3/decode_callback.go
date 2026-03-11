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

	"k8s.io/apiserver/pkg/storage"
)

// DecodeCallback is an alias for storage.DecodeCallback.
// Deprecated: use storage.DecodeCallback directly.
type DecodeCallback = storage.DecodeCallback

// WithDecodeCallback is an alias for storage.WithDecodeCallback.
// Deprecated: use storage.WithDecodeCallback directly.
func WithDecodeCallback(ctx context.Context, cb DecodeCallback) context.Context {
	return storage.WithDecodeCallback(ctx, cb)
}

func decodeCallbackFromContext(ctx context.Context) DecodeCallback {
	return storage.DecodeCallbackFromContext(ctx)
}
