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

// Trigger trigger the corresponding hook to serve watch based on the event passed in
func Trigger(e watch.Event) {
	trigger(e, true, false)
}

func TriggerPodDeletion(e watch.Event) {
	trigger(e, true, true)
}

func trigger(e watch.Event, checkVersion bool, allowUIDFallback bool) {
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
		if e.Type == watch.Deleted && allowUIDFallback && accessor.GetResourceVersion() == "" {
			compRev = hook.GetObservedUID() != "" && hook.GetObservedUID() == string(accessor.GetUID())
		} else if checkVersion && hook.GetResourceVersion() != 0 {
			rev, err := etcd3.Versioner.ParseResourceVersion(accessor.GetResourceVersion())
			if err != nil {
				klog.Errorf("failed to parse resource version, %v", err)
				return
			}
			compRev = hook.GetResourceVersion() < rev
		}
		if compGVR && compNS && compName && compRev {
			if e.Type != watch.Deleted {
				hook.SetObservedUID(string(accessor.GetUID()))
			}
			err = hook.Do(e)
			if err != nil {
				klog.Errorf("failed to operate event, %v", err)
				return
			}
		}
	}
}
