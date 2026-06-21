// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kube // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/k8sattributesprocessor/internal/kube"

import (
	"context"
	"sync"
	"time"

	api_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	clientmeta "k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"fmt"
)

type FakeInformer struct {
	*FakeController

	store         cache.Indexer
	namespace     string
	labelSelector labels.Selector
	fieldSelector fields.Selector
}

func NewFakeInformer(
	_ kubernetes.Interface,
	namespace string,
	labelSelector labels.Selector,
	fieldSelector fields.Selector,
) cache.SharedIndexInformer {
	keyFunc := func(obj any) (string, error) {
		switch p := obj.(type) {
		case *Pod:
			return p.Namespace + "/" + p.Name, nil
		case Pod:
			return p.Namespace + "/" + p.Name, nil
		case *api_v1.Pod:
			return p.Namespace + "/" + p.Name, nil
		case api_v1.Pod:
			return p.Namespace + "/" + p.Name, nil
		default:
			return "", fmt.Errorf("unknown type")
		}
	}
	return &FakeInformer{
		FakeController: &FakeController{},
		store:          cache.NewIndexer(keyFunc, cache.Indexers{}),
		namespace:      namespace,
		labelSelector:  labelSelector,
		fieldSelector:  fieldSelector,
	}
}

func (f *FakeInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return f.AddEventHandlerWithResyncPeriod(handler, time.Second)
}

func (f *FakeInformer) AddEventHandlerWithResyncPeriod(_ cache.ResourceEventHandler, _ time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return f, nil
}

func (f *FakeInformer) AddEventHandlerWithOptions(cache.ResourceEventHandler, cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return f, nil
}

func (*FakeInformer) RemoveEventHandler(cache.ResourceEventHandlerRegistration) error {
	return nil
}

func (*FakeInformer) IsStopped() bool {
	return false
}

func (*FakeInformer) SetTransform(cache.TransformFunc) error {
	return nil
}

func (f *FakeInformer) GetStore() cache.Store {
	return f.store
}

func (f *FakeInformer) GetIndexer() cache.Indexer {
	return f.store
}

func (f *FakeInformer) AddIndexers(indexers cache.Indexers) error {
	return f.store.AddIndexers(indexers)
}

func (f *FakeInformer) GetController() cache.Controller {
	return f.FakeController
}

func NewFakeNamespaceInformer(
	_ clientmeta.Interface,
) cache.SharedInformer {
	keyFunc := func(obj any) (string, error) {
		switch ns := obj.(type) {
		case *Namespace:
			return ns.Name, nil
		case Namespace:
			return ns.Name, nil
		case *api_v1.Namespace:
			return ns.Name, nil
		case api_v1.Namespace:
			return ns.Name, nil
		default:
			return "", fmt.Errorf("unknown type")
		}
	}
	return &FakeInformer{
		FakeController: &FakeController{},
		store:          cache.NewIndexer(keyFunc, cache.Indexers{}),
	}
}

func NewFakeReplicaSetInformer(
	_ clientmeta.Interface,
	_ string,
) cache.SharedInformer {
	keyFunc := func(obj any) (string, error) {
		switch rs := obj.(type) {
		case *ReplicaSet:
			return rs.Namespace + "/" + rs.Name, nil
		case ReplicaSet:
			return rs.Namespace + "/" + rs.Name, nil
		case *api_v1.Pod:
			return rs.Namespace + "/" + rs.Name, nil
		case *meta_v1.PartialObjectMetadata:
			return rs.Namespace + "/" + rs.Name, nil
		case meta_v1.PartialObjectMetadata:
			return rs.Namespace + "/" + rs.Name, nil
		default:
			return "", fmt.Errorf("unknown type")
		}
	}
	return &FakeInformer{
		FakeController: &FakeController{},
		store:          cache.NewIndexer(keyFunc, cache.Indexers{}),
	}
}

type FakeController struct {
	sync.Mutex
	stopped bool
}

func (*FakeController) HasSynced() bool {
	return true
}

func (c *FakeController) Run(stopCh <-chan struct{}) {
	<-stopCh
	c.Lock()
	c.stopped = true
	c.Unlock()
}

func (c *FakeController) RunWithContext(ctx context.Context) {
	c.Run(ctx.Done())
}

func (c *FakeController) HasStopped() bool {
	c.Lock()
	defer c.Unlock()
	return c.stopped
}

func (*FakeController) LastSyncResourceVersion() string {
	return ""
}

func (*FakeInformer) SetWatchErrorHandler(cache.WatchErrorHandler) error {
	return nil
}

func (*FakeInformer) SetWatchErrorHandlerWithContext(cache.WatchErrorHandlerWithContext) error {
	return nil
}

type NoOpInformer struct {
	*NoOpController
}

func NewNoOpInformer(
	_ clientmeta.Interface,
) cache.SharedInformer {
	return &NoOpInformer{
		NoOpController: &NoOpController{},
	}
}

func (f *NoOpInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return f.AddEventHandlerWithResyncPeriod(handler, time.Second)
}

func (f *NoOpInformer) AddEventHandlerWithResyncPeriod(cache.ResourceEventHandler, time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return f, nil
}

func (f *NoOpInformer) AddEventHandlerWithOptions(cache.ResourceEventHandler, cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return f, nil
}

func (*NoOpInformer) RemoveEventHandler(cache.ResourceEventHandlerRegistration) error {
	return nil
}

func (*NoOpInformer) SetTransform(cache.TransformFunc) error {
	return nil
}

func (*NoOpInformer) GetStore() cache.Store {
	return cache.NewStore(func(any) (string, error) { return "", nil })
}

func (f *NoOpInformer) GetController() cache.Controller {
	return f.NoOpController
}

type NoOpController struct {
	hasStopped bool
}

func (c *NoOpController) Run(stopCh <-chan struct{}) {
	go func() {
		<-stopCh
		c.hasStopped = true
	}()
}

func (c *NoOpController) RunWithContext(ctx context.Context) {
	c.Run(ctx.Done())
}

func (c *NoOpController) IsStopped() bool {
	return c.hasStopped
}

func (*NoOpController) HasSynced() bool {
	return true
}

func (*NoOpController) LastSyncResourceVersion() string {
	return ""
}

func (*NoOpController) SetWatchErrorHandler(cache.WatchErrorHandler) error {
	return nil
}

func (*NoOpController) SetWatchErrorHandlerWithContext(cache.WatchErrorHandlerWithContext) error {
	return nil
}
