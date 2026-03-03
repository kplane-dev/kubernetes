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

package cacher

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// identityAnnotatedList is an internal list type used by the cacher's
// listerWatcher to carry identity-wrapped items through the reflector.
// It implements runtime.Object and provides Items/ListMeta for
// meta.ExtractList() and meta.ListAccessor() compatibility.
type identityAnnotatedList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []runtime.Object `json:"items"`
}

func (l *identityAnnotatedList) GetObjectKind() schema.ObjectKind { return &l.TypeMeta }
func (l *identityAnnotatedList) DeepCopyObject() runtime.Object   { return l }
