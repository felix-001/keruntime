package watchhook

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage/etcd3"
	"k8s.io/klog/v2"

	"github.com/kubeedge/kubeedge/pkg/metaserver"
)

var (
	hooksLock sync.RWMutex
	// hooks is a map from hook.id to hook
	hooks = make(map[string]*WatchHook)
)

func AddHook(hook *WatchHook) error {
	hooksLock.Lock()
	defer hooksLock.Unlock()
	if _, exists := hooks[hook.id]; exists {
		return fmt.Errorf("unable to add hook %v because it was already registered", hook.id)
	}
	hooks[hook.id] = hook
	return nil
}

func DeleteHook(id string) error {
	hooksLock.Lock()
	defer hooksLock.Unlock()
	if _, exists := hooks[id]; exists {
		delete(hooks, id)
		return nil
	}
	return fmt.Errorf("unable to delete %q because it was not registered", id)
}

// Trigger triggers matching hooks to deliver a watch event, enforcing ResourceVersion ordering.
func Trigger(e watch.Event) {
	trigger(e, false)
}

// TriggerPodDeletion delivers a Pod Deleted event while guarding against stale UID deletions:
// if a hook already observed a different UID for the same key, the stale Deleted event is dropped;
// if the event has no ResourceVersion, delivery is restricted to hooks that observed the same UID.
func TriggerPodDeletion(e watch.Event) {
	trigger(e, true)
}

// trigger dispatches e to all matching hooks. When allowUIDFallback is true, Deleted events
// are additionally guarded by each hook's per-key observedUID (recorded only from non-Deleted
// events delivered through trigger).
func trigger(e watch.Event, allowUIDFallback bool) {
	key, err := metaserver.KeyFuncObj(e.Object)
	if err != nil {
		klog.Errorf("failed to get key, %v", err)
		return
	}
	gvr, ns, name := metaserver.ParseKey(key)
	accessor, accessorErr := meta.Accessor(e.Object)
	if accessorErr != nil {
		klog.Errorf("failed to get accessor, %v", accessorErr)
		return
	}
	hooksLock.RLock()
	hookList := make([]*WatchHook, 0, len(hooks))
	for _, hook := range hooks {
		hookList = append(hookList, hook)
	}
	hooksLock.RUnlock()
	for _, hook := range hookList {
		compGVR, compNS, compName, compRev := true, true, true, true
		if !hook.GetGVR().Empty() {
			compGVR = hook.GetGVR() == gvr
		}
		if hook.GetNamespace() != "" {
			compNS = hook.GetNamespace() == ns
		}
		if hook.GetName() != "" {
			compName = hook.GetName() == name
		}
		observedUID := hook.GetObservedUID(key)
		eventUID := string(accessor.GetUID())
		if e.Type == watch.Deleted && allowUIDFallback && observedUID != "" && eventUID != "" && observedUID != eventUID {
			compRev = false
		} else if e.Type == watch.Deleted && allowUIDFallback && accessor.GetResourceVersion() == "" {
			compRev = observedUID != "" && observedUID == eventUID
		} else if hook.GetResourceVersion() != 0 {
			rev, err := etcd3.Versioner.ParseResourceVersion(accessor.GetResourceVersion())
			if err != nil {
				klog.Errorf("failed to parse resource version, %v", err)
				return
			}
			compRev = hook.GetResourceVersion() < rev
		}
		if compGVR && compNS && compName && compRev {
			if e.Type != watch.Deleted {
				hook.SetObservedUID(key, eventUID)
			} else {
				hook.DeleteObservedUID(key)
			}
			err = hook.Do(e)
			if err != nil {
				klog.Errorf("failed to operate event, %v", err)
				continue
			}
		}
	}
}
