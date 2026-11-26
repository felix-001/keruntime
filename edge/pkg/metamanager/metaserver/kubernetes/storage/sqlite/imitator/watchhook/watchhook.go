package watchhook

import (
	"sync"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"

	"github.com/kubeedge/kubeedge/pkg/metaserver"
)

type WatchHook struct {
	GVR             schema.GroupVersionResource
	Namespace       string
	Name            string
	ResourceVersion uint64
	id              string
	Receiver
	observedUIDs map[string]string
	stopped      bool
	lock         sync.Mutex
}

func NewWatchHook(key string, rev uint64, receiver Receiver) (*WatchHook, error) {
	id := uuid.New().String()
	gvr, ns, name := metaserver.ParseKey(key)
	wh := &WatchHook{
		id:              id,
		GVR:             gvr,
		Namespace:       ns,
		Name:            name,
		ResourceVersion: rev,
		Receiver:        receiver,
		observedUIDs:    make(map[string]string),
	}
	err := AddHook(wh)
	return wh, err
}

func (h *WatchHook) Do(event watch.Event) error {
	h.Lock()
	defer h.UnLock()
	if h.stopped {
		return nil
	}
	return h.Receive(event)
}

func (h *WatchHook) GetGVR() schema.GroupVersionResource {
	return h.GVR
}

func (h *WatchHook) GetNamespace() string {
	return h.Namespace
}

func (h *WatchHook) GetName() string {
	return h.Name
}
func (h *WatchHook) GetResourceVersion() uint64 {
	return h.ResourceVersion
}

// GetObservedUID returns the last non-Deleted object UID delivered to this hook
// for key via Trigger/TriggerPodDeletion. Objects seeded directly by initial List/sync
// are not tracked here.
func (h *WatchHook) GetObservedUID(key string) string {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.observedUIDs[key]
}

// SetObservedUID records the latest non-Deleted object UID delivered to this hook for key.
func (h *WatchHook) SetObservedUID(key, uid string) {
	h.lock.Lock()
	if h.observedUIDs == nil {
		h.observedUIDs = make(map[string]string)
	}
	h.observedUIDs[key] = uid
	h.lock.Unlock()
}

// DeleteObservedUID clears the tracked UID for key after a Deleted event is delivered.
func (h *WatchHook) DeleteObservedUID(key string) {
	h.lock.Lock()
	delete(h.observedUIDs, key)
	h.lock.Unlock()
}

func (h *WatchHook) Stop() {
	h.Lock()
	defer h.UnLock()
	if h.stopped {
		return
	}
	h.stopped = true
	if err := DeleteHook(h.id); err != nil {
		klog.Error(err)
	}
}

func (h *WatchHook) Lock() {
	h.lock.Lock()
}

func (h *WatchHook) UnLock() {
	h.lock.Unlock()
}
