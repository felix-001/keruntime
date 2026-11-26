package edged

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	kubelettypes "k8s.io/kubernetes/pkg/kubelet/types"

	"github.com/kubeedge/beehive/pkg/common"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
)

func TestVerifyRestoredPod(t *testing.T) {
	resource := schema.GroupResource{Resource: "pods"}
	for _, test := range []struct {
		name     string
		err      error
		complete bool
	}{
		{"success", nil, true},
		{"missing", apierrors.NewNotFound(resource, "old"), true},
		{"wrong name not found", apierrors.NewNotFound(resource, "other"), false},
		{"wrong resource not found", apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, "old"), false},
		{"forbidden", apierrors.NewForbidden(resource, "old", fmt.Errorf("denied")), false},
		{"unauthorized", apierrors.NewUnauthorized("denied"), false},
		{"conflict", apierrors.NewConflict(resource, "old", fmt.Errorf("conflict")), false},
		{"bad request", apierrors.NewBadRequest("invalid"), false},
		{"invalid", apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "old", nil), false},
		{"uid mismatch", apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "old", field.ErrorList{field.Invalid(field.NewPath("metadata", "uid"), "old", "uid changed")}), true},
		{"uid mismatch wrong name", apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "other", field.ErrorList{field.Invalid(field.NewPath("metadata", "uid"), "old", "uid changed")}), false},
		{"unexpected 404", apierrors.NewGenericServerResponse(404, "PATCH", resource, "old", "proxy", 0, true), false},
		{"server error", apierrors.NewInternalError(fmt.Errorf("unavailable")), false},
		{"timeout", apierrors.NewTimeoutError("timeout", 1), false},
		{"connection error", fmt.Errorf("disconnected"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "default", UID: "old-uid"}}
			called := false
			complete := verifyRestoredPod(pod, func(name string, patch []byte) (*v1.Pod, error) {
				called = true
				if name != "old" || string(patch) != `{"metadata":{"uid":"old-uid"}}` {
					t.Fatalf("unexpected patch %s: %s", name, patch)
				}
				return nil, test.err
			})
			if !called || complete != test.complete {
				t.Fatalf("called=%v complete=%v", called, complete)
			}
		})
	}
}

func TestVerifyRestoredPodsLoopBackoffAndMaxAttempts(t *testing.T) {
	prevInitial := restoredPodVerifyInitialBackoff
	prevMax := restoredPodVerifyMaxBackoff
	prevMaxAttempts := restoredPodVerifyMaxAttempts
	defer func() {
		restoredPodVerifyInitialBackoff = prevInitial
		restoredPodVerifyMaxBackoff = prevMax
		restoredPodVerifyMaxAttempts = prevMaxAttempts
	}()
	restoredPodVerifyInitialBackoff = time.Millisecond
	restoredPodVerifyMaxBackoff = 4 * time.Millisecond
	restoredPodVerifyMaxAttempts = 3

	pod := v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "default", UID: "old-uid"}}
	stop := make(chan struct{})
	defer close(stop)
	attempts := 0
	verifyRestoredPodsLoop(
		[]v1.Pod{pod},
		stop,
		func() bool { return true },
		func(namespace, name string) (*v1.Pod, error) {
			return &pod, nil
		},
		func(namespace, name string, patch []byte) (*v1.Pod, error) {
			attempts++
			return nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, name, fmt.Errorf("denied"))
		},
	)
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestVerifyRestoredPodsLoopSkipsGoneOrReplacedLocalPod(t *testing.T) {
	prevInitial := restoredPodVerifyInitialBackoff
	defer func() { restoredPodVerifyInitialBackoff = prevInitial }()
	restoredPodVerifyInitialBackoff = time.Millisecond

	gonePod := v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "gone", Namespace: "default", UID: "old-1"}}
	replacedPod := v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "replaced", Namespace: "default", UID: "old-2"}}
	stop := make(chan struct{})
	defer close(stop)
	patchCalled := 0
	verifyRestoredPodsLoop(
		[]v1.Pod{gonePod, replacedPod},
		stop,
		func() bool { return true },
		func(namespace, name string) (*v1.Pod, error) {
			if name == "gone" {
				return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pod"}, name)
			}
			return &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: "new-2"}}, nil
		},
		func(namespace, name string, patch []byte) (*v1.Pod, error) {
			patchCalled++
			return nil, nil
		},
	)
	if patchCalled != 0 {
		t.Fatalf("patchCalled = %d, want 0 for gone/replaced local pods", patchCalled)
	}
}

func TestSendSyncDeleteErrorRespondsImmediately(t *testing.T) {
	beehiveContext.InitContext([]string{common.MsgCtxTypeChannel})
	beehiveContext.AddModule(&common.ModuleInfo{ModuleName: modules.EdgedModuleName, ModuleType: common.MsgCtxTypeChannel})
	t.Cleanup(func() { beehiveContext.Cleanup(modules.EdgedModuleName) })

	go func() {
		msg, err := beehiveContext.Receive(modules.EdgedModuleName)
		if err != nil {
			return
		}
		sendSyncDeleteError(msg, fmt.Errorf("decode failed"))
	}()

	req := model.NewMessage("").BuildRouter(modules.MetaManagerModuleName, modules.MetaGroup,
		"default/pod/old-pod", model.DeleteOperation).FillBody("{")
	resp, err := beehiveContext.SendSync(modules.EdgedModuleName, *req, time.Second)
	if err != nil {
		t.Fatalf("SendSync failed: %v", err)
	}
	if resp.GetParentID() != req.GetID() || resp.GetOperation() != model.ResponseErrorOperation || resp.GetContent() != "decode failed" {
		t.Fatalf("unexpected sync error response: %+v", resp)
	}
}

func TestHandlePodListFromMetaManager(t *testing.T) {
	deletionTime := metav1.Now()
	active := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "active"}, Spec: v1.PodSpec{NodeName: "edge-node"}}
	terminating := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "terminating", DeletionTimestamp: &deletionTime}, Spec: v1.PodSpec{NodeName: "edge-node"}}
	other := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other"}, Spec: v1.PodSpec{NodeName: "other-node"}}
	tests := []struct {
		name    string
		pods    []*v1.Pod
		want    []*v1.Pod
		content []byte
		wantErr bool
	}{
		{name: "active pod", pods: []*v1.Pod{active}, want: []*v1.Pod{active}},
		{name: "complete snapshot", pods: []*v1.Pod{active, terminating, other}, want: []*v1.Pod{active, terminating}},
		{name: "other node", pods: []*v1.Pod{other}},
		{name: "empty snapshot"},
		{name: "invalid list", content: []byte(`{`), wantErr: true},
		{name: "invalid pod", content: []byte(`["{"]`), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			content := test.content
			if content == nil {
				lists := make([]string, 0, len(test.pods))
				for _, pod := range test.pods {
					data, err := json.Marshal(pod)
					if err != nil {
						t.Fatal(err)
					}
					lists = append(lists, string(data))
				}
				var err error
				content, err = json.Marshal(lists)
				if err != nil {
					t.Fatal(err)
				}
			}
			updates := make(chan interface{}, 1)
			edge := &edged{nodeName: "edge-node"}
			err := edge.handlePodListFromMetaManager(content, updates)
			if (err != nil) != test.wantErr {
				t.Fatalf("unexpected error: %v, wantErr: %v", err, test.wantErr)
			}
			if test.wantErr {
				if len(updates) != 0 {
					t.Fatal("invalid input emitted a pod update")
				}
				return
			}
			select {
			case rawUpdate := <-updates:
				update, ok := rawUpdate.(kubelettypes.PodUpdate)
				if !ok {
					t.Fatalf("unexpected update type: %T", rawUpdate)
				}
				want := kubelettypes.PodUpdate{Op: kubelettypes.SET, Pods: test.want, Source: kubelettypes.ApiserverSource}
				wantJSON, _ := json.Marshal(want)
				gotJSON, _ := json.Marshal(update)
				if !reflect.DeepEqual(gotJSON, wantJSON) {
					t.Fatalf("unexpected update: %s, want: %s", gotJSON, wantJSON)
				}
			default:
				t.Fatal("missing pod update")
			}
		})
	}
}
