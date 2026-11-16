package watchhook

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

func TestTriggerConcurrentWithHookLifecycle(t *testing.T) {
	const iterations = 200
	events := make(chan watch.Event, iterations)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < iterations; i++ {
			hook, err := NewWatchHook("/core/v1/pods/default/pod", 0, NewChanReceiver(events))
			if err != nil {
				t.Errorf("NewWatchHook: %v", err)
				return
			}
			hook.Stop()
		}
	}()

	object := podObject("uid", "300")
	for i := 0; i < iterations; i++ {
		TriggerPodDeletion(watch.Event{Type: watch.Deleted, Object: object})
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("hook lifecycle did not complete")
	}
}

func podObject(uid, resourceVersion string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":            "pod",
			"namespace":       "default",
			"uid":             uid,
			"resourceVersion": resourceVersion,
		},
	}}
}

func TestTriggerPodDeletionHonorsResourceVersion(t *testing.T) {
	events := make(chan watch.Event, 4)
	hook, err := NewWatchHook("/core/v1/pods/default/pod", 200, NewChanReceiver(events))
	if err != nil {
		t.Fatal(err)
	}
	defer hook.Stop()

	Trigger(watch.Event{Type: watch.Added, Object: podObject("new", "300")})
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("watcher did not observe the current Pod")
	}

	TriggerPodDeletion(watch.Event{Type: watch.Deleted, Object: podObject("old", "100")})
	select {
	case event := <-events:
		t.Fatalf("stale deletion was delivered: %#v", event)
	case <-time.After(20 * time.Millisecond):
	}

	TriggerPodDeletion(watch.Event{Type: watch.Deleted, Object: podObject("new", "300")})
	select {
	case event := <-events:
		if event.Type != watch.Deleted || event.Object.(*unstructured.Unstructured).GetUID() != "new" {
			t.Fatalf("unexpected deletion event: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("current deletion was not delivered")
	}
}

func TestTriggerPodDeletionWithoutResourceVersionRequiresObservedUID(t *testing.T) {
	events := make(chan watch.Event, 1)
	hook, err := NewWatchHook("/core/v1/pods/default/pod", 0, NewChanReceiver(events))
	if err != nil {
		t.Fatal(err)
	}
	defer hook.Stop()

	TriggerPodDeletion(watch.Event{Type: watch.Deleted, Object: podObject("unknown", "")})
	select {
	case event := <-events:
		t.Fatalf("unobserved deletion was delivered: %#v", event)
	case <-time.After(20 * time.Millisecond):
	}
}
