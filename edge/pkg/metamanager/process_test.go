/*
Copyright 2018 The KubeEdge Authors.

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

package metamanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	coordinationv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeedge/beehive/pkg/common"
	"github.com/kubeedge/beehive/pkg/core"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/common/constants"
	"github.com/kubeedge/kubeedge/edge/mocks/beego"
	connect "github.com/kubeedge/kubeedge/edge/pkg/common/cloudconnection"
	"github.com/kubeedge/kubeedge/edge/pkg/common/dbm"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	metaManagerConfig "github.com/kubeedge/kubeedge/edge/pkg/metamanager/config"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao"
	v2 "github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/v2"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/edgecore/v1alpha2"
)

const (
	MessageTest = "Test"
	// FailedDBOperation is common Database operation fail message
	FailedDBOperation = "Failed DB Operation"
	// ModuleNameEdged is name of edged module
	ModuleNameEdged = "edged"
	// ModuleNameEdgeHub is name of edgehub module
	ModuleNameEdgeHub = "websocket"
	// ModuleNameController is the name of the controller module
	ModuleNameController = "edgecontroller"
	marshalError         = "get message content data failed, error: marshal message content failed: json: unsupported type: chan int"
	testHolderIdentity   = "edgehub"
)

// errFailedDBOperation is common Database operation fail error
var errFailedDBOperation = errors.New(FailedDBOperation)

func init() {
	cfg := v1alpha2.NewDefaultEdgeCoreConfig()
	metaManagerConfig.InitConfigure(cfg.Modules.MetaManager)

	beehiveContext.InitContext([]string{common.MsgCtxTypeChannel})
	add := &common.ModuleInfo{
		ModuleName: modules.MetaManagerModuleName,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(add)
}

// TestProcessInsert is function to test processInsert
func TestProcessInsert(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	ormerMock := beego.NewMockOrmer(mockCtrl)
	rawSetterMock := beego.NewMockRawSeter(mockCtrl)
	dbm.DBAccess = ormerMock
	meta := newMetaManager(true)
	core.Register(meta)

	add := &common.ModuleInfo{
		ModuleName: meta.Name(),
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(add)
	beehiveContext.AddModuleGroup(meta.Name(), meta.Group())
	edgeHub := &common.ModuleInfo{
		ModuleName: ModuleNameEdgeHub,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(edgeHub)
	beehiveContext.AddModuleGroup(ModuleNameEdgeHub, modules.HubGroup)
	edged := &common.ModuleInfo{
		ModuleName: ModuleNameEdged,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(edged)
	connect.SetConnected(true)

	//SaveMeta Failed, feedbackError SendToCloud
	rawSetterMock.EXPECT().Exec().Return(nil, errFailedDBOperation).Times(1)
	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)

	msg := model.NewMessage("").BuildRouter(modules.MetaManagerModuleName, GroupResource, model.ResourceTypeConfigmap, model.InsertOperation)
	meta.processInsert(*msg)
	message, err := beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("insert error", func(t *testing.T) {
		if err != nil {
			t.Errorf("EdgeHub Channel not found: %v", err)
			return
		}
		want := "insert or update meta failed, " + FailedDBOperation
		if message.GetContent() != want {
			t.Errorf("Wrong Error message received : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	//SaveMeta Failed, feedbackError SendToEdged and 2 resources
	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)
	rawSetterMock.EXPECT().Exec().Return(nil, errFailedDBOperation).Times(1)

	resource := fmt.Sprintf("%s/%s/%s", "default", model.ResourceTypeLease, "leaseName")
	msg = model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, resource, model.InsertOperation)
	meta.processInsert(*msg)
	message, err = beehiveContext.Receive(ModuleNameEdgeHub)
	if err != nil {
		t.Errorf("EdgeHub Channel not found: %v", err)
		return
	}
	message.Header.ParentID = message.GetID()
	message.Content = &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "leaseName",
			Namespace: "default",
		},
	}
	beehiveContext.SendResp(message)
	message, err = beehiveContext.Receive(ModuleNameEdged)
	t.Run("Insert failed", func(t *testing.T) {
		if err != nil {
			t.Errorf("EdgeD Channel not found: %v", err)
			return
		}
		want := "insert or update meta failed, " + FailedDBOperation
		if message.GetContent() != want {
			t.Errorf("Wrong Error message received : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	//SaveMeta Failed, feedbackError SendToEdged and 2 resources
	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)
	rawSetterMock.EXPECT().Exec().Return(nil, nil).Times(1)

	resource = fmt.Sprintf("%s/%s/%s", "default", model.ResourceTypeLease, "leaseName")
	msg = model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, resource, model.InsertOperation)
	meta.processInsert(*msg)
	message, err = beehiveContext.Receive(ModuleNameEdgeHub)
	if err != nil {
		t.Errorf("EdgeHub Channel not found: %v", err)
		return
	}
	message.Header.ParentID = message.GetID()
	message.Content = &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "leaseName",
			Namespace: "default",
		},
	}
	beehiveContext.SendResp(message)
	message, err = beehiveContext.Receive(ModuleNameEdged)
	beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("Insert successfully", func(t *testing.T) {
		if err != nil {
			t.Errorf("EdgeD Channel not found: %v", err)
			return
		}
		leaseBytes, _ := message.GetContentData()
		lease := &coordinationv1.Lease{}
		err = json.Unmarshal(leaseBytes, lease)
		if err != nil {
			t.Errorf("Unmarshal lease failed: %v", err)
			return
		}
		if lease.Name != "leaseName" || lease.Namespace != "default" {
			t.Errorf("Wrong lease received : Wanted leaseName and Got %v", lease.Name)
		}
	})

	//jsonMarshall fail
	msg = model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, model.ResourceTypeLease, model.InsertOperation).FillBody(make(chan int))
	meta.processInsert(*msg)
	message, err = beehiveContext.Receive(ModuleNameEdgeHub)
	if err != nil {
		t.Errorf("EdgeHub Channel not found: %v", err)
		return
	}
	message.Header.ParentID = message.GetID()
	beehiveContext.SendResp(message)
	message, err = beehiveContext.Receive(ModuleNameEdged)
	t.Run("MarshallFail", func(t *testing.T) {
		if message.GetContent() != marshalError {
			t.Errorf("Wrong Error message received : Wanted %v and Got %v", marshalError, message.GetContent())
		}
	})

	//Successful Case and 3 resources
	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)
	rawSetterMock.EXPECT().Exec().Return(nil, nil).Times(1)

	msg = model.NewMessage("").BuildRouter(ModuleNameController, GroupResource, model.ResourceTypeLease+"/secondRes"+"/thirdRes", model.InsertOperation)
	meta.processInsert(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("InsertMessageToEdged", func(t *testing.T) {
		want := model.InsertOperation
		if message.GetOperation() != want {
			t.Errorf("Wrong message received : Wanted %v and Got %v", want, message.GetOperation())
		}
	})
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("ResponseMessageToEdgeHub", func(t *testing.T) {
		want := OK
		if message.GetContent() != want {
			t.Errorf("Wrong message received : Wanted %v and Got %v", want, message.GetContent())
		}
	})
}

// TestProcessUpdate is function to test processUpdate
func TestProcessUpdate(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	ormerMock := beego.NewMockOrmer(mockCtrl)
	rawSetterMock := beego.NewMockRawSeter(mockCtrl)
	dbm.DBAccess = ormerMock
	meta := newMetaManager(true)
	core.Register(meta)

	add := &common.ModuleInfo{
		ModuleName: meta.Name(),
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(add)
	beehiveContext.AddModuleGroup(meta.Name(), meta.Group())
	edgeHub := &common.ModuleInfo{
		ModuleName: ModuleNameEdgeHub,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(edgeHub)
	beehiveContext.AddModuleGroup(ModuleNameEdgeHub, modules.HubGroup)
	edged := &common.ModuleInfo{
		ModuleName: ModuleNameEdged,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(edged)
	connect.SetConnected(true)

	//jsonMarshall fail
	msg := model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, "namespace/"+model.ResourceTypeLease+"/id", model.UpdateOperation).FillBody(make(chan int))
	meta.processUpdate(*msg)
	message, _ := beehiveContext.Receive(ModuleNameEdgeHub)
	message.Header.ParentID = message.GetID()
	beehiveContext.SendResp(message)
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("MarshallFail", func(t *testing.T) {
		if message.GetContent() != marshalError {
			t.Errorf("Wrong Error message received : Wanted %v and Got %v", marshalError, message.GetContent())
		}
	})

	//Database save error
	rawSetterMock.EXPECT().Exec().Return(nil, errFailedDBOperation).Times(1)
	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)
	msg = model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, model.ResourceTypeNode, model.UpdateOperation)
	meta.processUpdate(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("DatabaseSaveError", func(t *testing.T) {
		want := "insert or update meta failed, " + FailedDBOperation
		if message.GetContent() != want {
			t.Errorf("Wrong Error message received : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	//Success Case Source Edged, sync = true
	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)
	rawSetterMock.EXPECT().Exec().Return(nil, nil).Times(1)
	msg = model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, model.ResourceTypeLease, model.UpdateOperation)
	meta.processUpdate(*msg)
	edgehubMsg, _ := beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("SuccessSourceEdgedReceiveEdgehub", func(t *testing.T) {
		want := model.UpdateOperation
		if edgehubMsg.GetOperation() != want {
			t.Errorf("Wrong message received : Wanted operation %v and Got operation %v", want, edgehubMsg.GetOperation())
		}
	})
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("SuccessSourceEdgedReceiveEdged", func(t *testing.T) {
		want := OK
		if message.GetContent() != want {
			t.Errorf("Wrong message received : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	//Success Case Source CloudControllerModel
	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)
	rawSetterMock.EXPECT().Exec().Return(nil, nil).Times(1)
	msg = model.NewMessage("").BuildRouter(CloudControllerModel, GroupResource, model.ResourceTypeLease, model.UpdateOperation)
	meta.processUpdate(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("SuccessSend[CloudController->Edged]", func(t *testing.T) {
		want := CloudControllerModel
		if message.GetSource() != want {
			t.Errorf("Wrong message received : Wanted from source %v and Got from source %v", want, message.GetSource())
		}
	})
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("SuccessSendCloud[CloudController->EdgeHub]", func(t *testing.T) {
		want := OK
		if message.GetContent() != want {
			t.Errorf("Wrong message received : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)
	rawSetterMock.EXPECT().Exec().Return(nil, errFailedDBOperation).Times(1)
	resource := fmt.Sprintf("%s/%s/%s", "default", model.ResourceTypeLease, "leaseName2")
	msg = model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, resource, model.UpdateOperation)
	meta.processUpdate(*msg)
	message, err := beehiveContext.Receive(ModuleNameEdgeHub)
	if err != nil {
		t.Errorf("EdgeHub Channel not found: %v", err)
		return
	}
	message.Header.ParentID = message.GetID()
	holderIdentity := testHolderIdentity
	message.Content = &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "leaseName2",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holderIdentity,
		},
	}
	beehiveContext.SendResp(message)
	message, err = beehiveContext.Receive(ModuleNameEdged)
	t.Run("Update failed", func(t *testing.T) {
		if err != nil {
			t.Errorf("EdgeD Channel not found: %v", err)
			return
		}
		want := "insert or update meta failed, " + FailedDBOperation
		if message.GetContent() != want {
			t.Errorf("Wrong Error message received : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)
	rawSetterMock.EXPECT().Exec().Return(nil, nil).Times(1)
	resource = fmt.Sprintf("%s/%s/%s", "default", model.ResourceTypeLease, "leaseName2")
	msg = model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, resource, model.UpdateOperation)
	meta.processUpdate(*msg)
	message, err = beehiveContext.Receive(ModuleNameEdgeHub)
	if err != nil {
		t.Errorf("EdgeHub Channel not found: %v", err)
		return
	}
	message.Header.ParentID = message.GetID()
	holderIdentity = testHolderIdentity
	message.Content = &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "leaseName2",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holderIdentity,
		},
	}
	beehiveContext.SendResp(message)
	message, err = beehiveContext.Receive(ModuleNameEdged)
	t.Run("Update successfully", func(t *testing.T) {
		if err != nil {
			t.Errorf("EdgeD Channel not found: %v", err)
			return
		}
		leBytes, err := message.GetContentData()
		if err != nil {
			t.Errorf("Error getting lease data: %v", err)
			return
		}
		var le = &coordinationv1.Lease{}
		err = json.Unmarshal(leBytes, le)
		if err != nil {
			t.Errorf("Error unmarshal lease: %v", err)
			return
		}

		if le.Spec.HolderIdentity == nil {
			t.Errorf("Error getting lease holderIdentity")
			return
		}
		if *le.Spec.HolderIdentity != testHolderIdentity {
			t.Errorf("Error getting lease holderIdentity, expected edgehub, got %s", *le.Spec.HolderIdentity)
			return
		}
	})
}

// TestProcessResponse is function to test processResponse
func TestProcessResponse(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	ormerMock := beego.NewMockOrmer(mockCtrl)
	rawSetterMock := beego.NewMockRawSeter(mockCtrl)
	dbm.DBAccess = ormerMock
	meta := newMetaManager(true)
	core.Register(meta)

	add := &common.ModuleInfo{
		ModuleName: meta.Name(),
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(add)
	beehiveContext.AddModuleGroup(meta.Name(), meta.Group())
	edgeHub := &common.ModuleInfo{
		ModuleName: ModuleNameEdgeHub,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(edgeHub)
	beehiveContext.AddModuleGroup(ModuleNameEdgeHub, modules.HubGroup)
	addEdged := &common.ModuleInfo{
		ModuleName: ModuleNameEdged,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(addEdged)

	//jsonMarshall fail
	msg := model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, model.ResourceTypeLease, model.ResponseOperation).FillBody(make(chan int))
	beehiveContext.Send(modules.MetaManagerModuleName, *msg)
	meta.processResponse(*msg)
	message, _ := beehiveContext.Receive(ModuleNameEdged)
	t.Run("MarshallFail", func(t *testing.T) {
		if message.GetContent() != marshalError {
			t.Errorf("Wrong Error message received : Wanted %v and Got %v", marshalError, message.GetContent())
		}
	})

	//Database save error
	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)
	rawSetterMock.EXPECT().Exec().Return(nil, errFailedDBOperation).Times(1)
	msg = model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, model.ResourceTypeLease, model.ResponseOperation)
	meta.processResponse(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("DatabaseSaveError", func(t *testing.T) {
		want := "insert or update meta failed, " + FailedDBOperation
		if message.GetContent() != want {
			t.Errorf("Wrong Error message received : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	//Success Case Source EdgeD
	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)
	rawSetterMock.EXPECT().Exec().Return(nil, nil).Times(1)
	msg = model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, model.ResourceTypeLease, model.ResponseOperation)
	meta.processResponse(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("SuccessSourceEdged", func(t *testing.T) {
		want := ModuleNameEdged
		if message.GetSource() != want {
			t.Errorf("Wrong message received : Wanted from source %v and Got from source %v", want, message.GetSource())
		}
	})

	//Success Case Source EdgeHub
	ormerMock.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(rawSetterMock).Times(1)
	rawSetterMock.EXPECT().Exec().Return(nil, nil).Times(1)
	msg = model.NewMessage("").BuildRouter(ModuleNameController, GroupResource, model.ResourceTypeLease, model.ResponseOperation)
	meta.processResponse(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("SuccessSourceEdgeHub", func(t *testing.T) {
		want := ModuleNameController
		if message.GetSource() != want {
			t.Errorf("Wrong message received : Wanted from source %v and Got from source %v", want, message.GetSource())
		}
	})
}

// TestProcessDelete is function to test processDelete
func TestProcessDelete(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	ormerMock := beego.NewMockOrmer(mockCtrl)
	querySetterMock := beego.NewMockQuerySeter(mockCtrl)
	dbm.DBAccess = ormerMock
	meta := newMetaManager(true)
	core.Register(meta)

	add := &common.ModuleInfo{
		ModuleName: meta.Name(),
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(add)
	beehiveContext.AddModuleGroup(meta.Name(), meta.Group())
	edgeHub := &common.ModuleInfo{
		ModuleName: ModuleNameEdgeHub,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(edgeHub)
	beehiveContext.AddModuleGroup(ModuleNameEdgeHub, modules.HubGroup)
	edged := &common.ModuleInfo{
		ModuleName: ModuleNameEdged,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(edged)

	//Database Save Error
	querySetterMock.EXPECT().Filter(gomock.Any(), gomock.Any()).Return(querySetterMock).Times(1)
	querySetterMock.EXPECT().Delete().Return(int64(1), errFailedDBOperation).Times(1)
	ormerMock.EXPECT().QueryTable(gomock.Any()).Return(querySetterMock).Times(1)
	msg := model.NewMessage("").BuildRouter(ModuleNameEdgeHub, GroupResource, model.ResourceTypeLease, model.DeleteOperation)
	meta.processDelete(*msg)
	message, _ := beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("DatabaseDeleteError", func(t *testing.T) {
		want := "delete meta failed, " + FailedDBOperation
		if message.GetContent() != want {
			t.Errorf("Wrong message received : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	//Success Case
	querySetterMock.EXPECT().Filter(gomock.Any(), gomock.Any()).Return(querySetterMock).Times(1)
	querySetterMock.EXPECT().Delete().Return(int64(1), nil).Times(1)
	ormerMock.EXPECT().QueryTable(gomock.Any()).Return(querySetterMock).Times(1)
	msg = model.NewMessage("").BuildRouter(ModuleNameEdgeHub, GroupResource, model.ResourceTypeLease, model.DeleteOperation)
	meta.processDelete(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("SuccessSourceEdgeHub", func(t *testing.T) {
		want := ModuleNameEdgeHub
		if message.GetSource() != want {
			t.Errorf("Wrong message received : Wanted from source %v and Got from source %v", want, message.GetSource())
		}
	})
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("SuccessResponseOK", func(t *testing.T) {
		want := OK
		if message.GetContent() != want {
			t.Errorf("Wrong message received : Wanted %v and Got %v", want, message.GetContent())
		}
	})
	// Success Case
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nginx",
			Namespace: "test",
			UID:       "1234567890",
		},
	}
	podBytes, _ := json.Marshal(pod)
	resource := fmt.Sprintf("test/%s/nginx", model.ResourceTypePod)
	msg = model.NewMessage("").BuildRouter(ModuleNameEdged, modules.MetaGroup, resource, model.DeleteOperation).FillBody(podBytes)
	meta.processDelete(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)

	t.Run("SuccessDeleteRemotePod", func(t *testing.T) {
		want := ModuleNameEdged
		if message.GetSource() != want {
			t.Errorf("Wrong message received : Wanted from source %v and Got from source %v", want, message.GetSource())
		}
		if message.GetOperation() != model.DeleteOperation {
			t.Errorf("Wrong message received : Wanted %v and Got %v", constants.MessageSuccessfulContent, message.GetContent())
		}
	})

	fakeDao := new([]dao.Meta)
	fakeDaoArray := make([]dao.Meta, 1)
	fakeDaoArray[0] = dao.Meta{Key: "Test", Value: string(podBytes)}
	fakeDao = &fakeDaoArray
	querySetterMock.EXPECT().Filter(gomock.Any(), gomock.Any()).Return(querySetterMock).Times(4)
	querySetterMock.EXPECT().All(gomock.Any()).SetArg(0, *fakeDao).Return(int64(1), nil).Times(1)
	querySetterMock.EXPECT().All(gomock.Any()).SetArg(0, []v2.MetaV2{}).Return(int64(0), nil).Times(1)
	querySetterMock.EXPECT().Delete().Return(int64(1), nil).Times(2)
	ormerMock.EXPECT().QueryTable(gomock.Any()).Return(querySetterMock).Times(5)
	snapshotQueryMock := beego.NewMockQuerySeter(mockCtrl)
	querySetterMock.EXPECT().Filter("type", model.ResourceTypePod).Return(snapshotQueryMock)
	snapshotQueryMock.EXPECT().All(gomock.Any()).SetArg(0, []dao.Meta{}).Return(int64(0), nil)

	resource = fmt.Sprintf("test/%s/nginx", model.ResourceTypePod)
	msg = model.NewMessage("").BuildRouter(ModuleNameController, modules.MetaGroup, resource, model.DeleteOperation).FillBody(podBytes)
	meta.processDelete(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("SuccessDeletePodToEdged", func(t *testing.T) {
		want := modules.MetaManagerModuleName
		if message.GetSource() != want {
			t.Errorf("Wrong message received : Wanted from source %v and Got from source %v", want, message.GetSource())
		}
		if message.GetOperation() != model.DeleteOperation {
			t.Errorf("Wrong message received : Wanted %v and Got %v", model.DeleteOperation, message.GetOperation())
		}
		if message.GetResource() != resource {
			t.Errorf("Wrong message received resource: Wanted %v and Got %v", resource, message.GetResource())
		}
		if snapshot, ok := message.GetContent().(PodDeleteSnapshot); !ok || len(snapshot) != 0 {
			t.Errorf("Expected an empty pod delete snapshot, got %#v", message.GetContent())
		}
	})
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("SuccessDeletePodResponse", func(t *testing.T) {
		want := ModuleNameController
		if message.GetSource() != want {
			t.Errorf("Wrong message received : Wanted from source %v and Got from source %v", want, message.GetSource())
		}
		if message.GetOperation() != model.ResponseOperation {
			t.Errorf("Wrong message received : Wanted %v and Got %v", model.ResponseOperation, message.GetOperation())
		}
		if message.GetContent() != OK {
			t.Errorf("Wrong message received : Wanted %v and Got %v", constants.MessageSuccessfulContent, message.GetContent())
		}
	})
}

func TestPodCleanupDB(t *testing.T) {
	for _, test := range []struct {
		name          string
		body          interface{}
		stored        string
		queryError    bool
		v2Stored      string
		v2QueryError  bool
		deleteFailure int
		wantError     bool
	}{
		{name: "missing UID", body: &v1.Pod{}, wantError: true},
		{name: "bad content", body: "{", wantError: true},
		{name: "marshal error", body: make(chan int), wantError: true},
		{name: "query failure", queryError: true, wantError: true},
		{name: "bad stored pod", stored: "{", wantError: true},
		{name: "different UID", stored: `{"metadata":{"uid":"new"}}`},
		{name: "duplicate delete"},
		{name: "delete matching", stored: `{"metadata":{"uid":"old"}}`},
		{name: "v2 failure", v2QueryError: true, wantError: true},
		{name: "invalid v2", v2Stored: "{", wantError: true},
		{name: "v2 different UID", v2Stored: `{"metadata":{"uid":"new"}}`},
		{name: "v2 matching UID", v2Stored: `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod","namespace":"default","uid":"old"}}`},
		{name: "v2 delete fails", v2Stored: `{"metadata":{"uid":"old"}}`, deleteFailure: 1, wantError: true},
		{name: "patch delete fails", deleteFailure: 2, wantError: true},
		{name: "meta delete fails", deleteFailure: 3, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			ormer := beego.NewMockOrmer(mockCtrl)
			query := beego.NewMockQuerySeter(mockCtrl)
			previous := dbm.DBAccess
			dbm.DBAccess = ormer
			defer func() { dbm.DBAccess = previous }()
			ormer.EXPECT().QueryTable(gomock.Any()).Return(query).AnyTimes()
			query.EXPECT().Filter(gomock.Any(), gomock.Any()).Return(query).AnyTimes()
			query.EXPECT().All(gomock.Any()).DoAndReturn(func(target interface{}, columns ...string) (int64, error) {
				switch result := target.(type) {
				case *[]dao.Meta:
					if test.queryError {
						return 0, errFailedDBOperation
					}
					if test.stored != "" {
						*result = []dao.Meta{{Value: test.stored}}
					}
				case *[]v2.MetaV2:
					if test.v2QueryError {
						return 0, errFailedDBOperation
					}
					if test.v2Stored != "" {
						*result = []v2.MetaV2{{Value: test.v2Stored}}
					}
				}
				return 1, nil
			}).AnyTimes()
			deleted := 0
			query.EXPECT().Delete().DoAndReturn(func() (int64, error) {
				deleted++
				if test.deleteFailure == 1 || test.deleteFailure == 2 && deleted == 1 || test.deleteFailure == 3 && deleted == 2 {
					return 0, errFailedDBOperation
				}
				return 1, nil
			}).AnyTimes()
			body := test.body
			if body == nil {
				body = &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "default", UID: "old"}}
			}
			message := model.NewMessage("").BuildRouter(ModuleNameController, modules.MetaGroup, "default/pod/pod", model.DeleteOperation).FillBody(body)
			err := processDeletePodDB(*message)
			if (err != nil) != test.wantError {
				t.Fatalf("err=%v", err)
			}
			if test.name == "different UID" && deleted != 0 {
				t.Fatal("new UID deleted")
			}
		})
	}
}

func TestProcessDeleteUIDMismatchIsIdempotent(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	previous := dbm.DBAccess
	defer func() { dbm.DBAccess = previous }()
	ormer := beego.NewMockOrmer(mockCtrl)
	query := beego.NewMockQuerySeter(mockCtrl)
	dbm.DBAccess = ormer
	ormer.EXPECT().QueryTable(gomock.Any()).Return(query).AnyTimes()
	query.EXPECT().Filter(gomock.Any(), gomock.Any()).Return(query).AnyTimes()
	query.EXPECT().All(gomock.Any()).DoAndReturn(func(target interface{}, columns ...string) (int64, error) {
		switch result := target.(type) {
		case *[]dao.Meta:
			*result = []dao.Meta{{Value: `{"metadata":{"uid":"new"}}`}}
		case *[]v2.MetaV2:
			*result = []v2.MetaV2{{Key: "/core/v1/pods/default/pod",
				Value: `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod","namespace":"default","uid":"old"}}`}}
		}
		return 1, nil
	}).AnyTimes()
	// Only the stale V2 record may be deleted; the V1 record of the new pod must be kept.
	// A second delete means the V1 record was removed as well, which must not happen.
	query.EXPECT().Delete().Return(int64(1), nil).Times(1)
	message := model.NewMessage("").BuildRouter(ModuleNameController, modules.MetaGroup, "default/pod/pod", model.DeleteOperation).
		FillBody(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pod", UID: "old"}})
	if err := processDeletePodDB(*message); err != nil {
		t.Fatalf("err=%v", err)
	}
}

func TestProcessPodPatchResponse(t *testing.T) {
	manager := newMetaManager(true)
	for _, name := range []string{ModuleNameEdgeHub, ModuleNameEdged, manager.Name()} {
		name := name
		beehiveContext.AddModule(&common.ModuleInfo{ModuleName: name, ModuleType: common.MsgCtxTypeChannel})
		t.Cleanup(func() { beehiveContext.Cleanup(name) })
	}
	beehiveContext.AddModuleGroup(ModuleNameEdgeHub, modules.HubGroup)
	for _, notFound := range []bool{false, true} {
		t.Run(fmt.Sprintf("notFound=%v", notFound), func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			previous := dbm.DBAccess
			defer func() { dbm.DBAccess = previous }()
			ormer := beego.NewMockOrmer(mockCtrl)
			raw := beego.NewMockRawSeter(mockCtrl)
			query := beego.NewMockQuerySeter(mockCtrl)
			dbm.DBAccess = ormer
			ormer.EXPECT().Raw(gomock.Any(), gomock.Any()).Return(raw).Times(1)
			raw.EXPECT().Exec().Return(nil, nil).Times(1)
			if notFound {
				ormer.EXPECT().QueryTable(gomock.Any()).Return(query).Times(5)
				query.EXPECT().Filter("key", "default/pod/old-pod").Return(query).Times(2)
				query.EXPECT().Filter("key", "/core/v1/pods/default/old-pod").Return(query)
				query.EXPECT().Filter("key", "default/podpatch/old-pod").Return(query)
				query.EXPECT().All(gomock.Any()).SetArg(0, []dao.Meta{{Value: `{"metadata":{"uid":"old-uid"}}`}}).Return(int64(1), nil)
				query.EXPECT().All(gomock.Any()).SetArg(0, []v2.MetaV2{}).Return(int64(0), nil)
				query.EXPECT().Delete().Return(int64(1), nil).Times(2)
				snapshot := beego.NewMockQuerySeter(mockCtrl)
				query.EXPECT().Filter("type", model.ResourceTypePod).Return(snapshot)
				snapshot.EXPECT().All(gomock.Any()).SetArg(0, []dao.Meta{{Value: `{"metadata":{"uid":"remaining-uid"}}`}}).Return(int64(1), nil)
			}
			request := model.NewMessage("").BuildRouter(modules.EdgedModuleName, modules.MetaGroup, "default/podpatch/old-pod", model.PatchOperation).
				FillBody(`{"metadata":{"uid":"old-uid"}}`)
			request.Header.Sync = false
			cloudResult := make(chan model.Message, 1)
			cleanupResponse := make(chan model.Message, 1)
			manager.syncTransport = func(module string, cloudRequest model.Message, _ time.Duration) (model.Message, error) {
				if module == manager.Name() {
					cloudRequest.Header.Sync = true
					beehiveContext.Send(module, cloudRequest)
					select {
					case response := <-cleanupResponse:
						if response.GetParentID() != cloudRequest.GetID() {
							return model.Message{}, fmt.Errorf("local cleanup response not correlated")
						}
						return response, nil
					case <-time.After(5 * time.Second):
						return model.Message{}, fmt.Errorf("no local cleanup response")
					}
				}
				if module != ModuleNameEdgeHub {
					return model.Message{}, fmt.Errorf("unexpected sync destination %q", module)
				}
				response := cloudRequest.NewRespByMessage(&cloudRequest, map[string]interface{}{
					"Object": map[string]interface{}{"metadata": map[string]interface{}{"uid": "old-uid"}}, "Err": nil,
				})
				if notFound {
					response.Content = map[string]interface{}{"Object": nil, "Err": map[string]interface{}{"ErrStatus": map[string]interface{}{"code": float64(404)}}}
				}
				cloudResult <- cloudRequest
				return *response, nil
			}
			done := manager.processPatch(*request)
			if done == nil {
				t.Fatal("PATCH bypassed processRemote")
			}
			defer func() { <-done }()
			select {
			case cloudRequest := <-cloudResult:
				if cloudRequest.GetID() == request.GetID() {
					t.Fatal("PATCH bypassed processRemote")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("PATCH did not reach cloud")
			}
			receive := func(module string) model.Message {
				result := make(chan model.Message, 1)
				go func() {
					message, _ := beehiveContext.Receive(module)
					result <- message
				}()
				select {
				case message := <-result:
					return message
				case <-time.After(5 * time.Second):
					t.Fatalf("no message received by %s", module)
					return model.Message{}
				}
			}
			if notFound {
				queued := receive(manager.Name())
				if queued.GetOperation() != model.DeleteOperation || queued.GetResource() != "default/pod/old-pod" {
					t.Fatalf("unexpected queued delete: %+v", queued)
				}
				manager.process(queued)
				deleted := receive(ModuleNameEdged)
				if deleted.GetOperation() != model.DeleteOperation || deleted.GetResource() != "default/pod/old-pod" {
					t.Fatalf("unexpected delete notification: %+v", deleted)
				}
				pods, ok := deleted.GetContent().(PodDeleteSnapshot)
				if deleted.GetType() != queued.GetType() || !deleted.IsSync() || deleted.GetID() != queued.GetID() ||
					!ok || len(pods) != 1 || pods[0] != `{"metadata":{"uid":"remaining-uid"}}` {
					t.Fatalf("unexpected delete snapshot: %+v", deleted)
				}
				cleanupResponse <- *deleted.NewRespByMessage(&deleted, OK)
			}
			response := receive(ModuleNameEdged)
			if response.GetParentID() != request.GetID() {
				t.Fatalf("response not correlated: %+v", response)
			}
			body, ok := response.Content.(map[string]interface{})
			if !ok || !isObjectResp(body) || (body["Err"] != nil) != notFound {
				t.Fatalf("ObjectResp contract lost: %+v", response)
			}
			receive(ModuleNameEdgeHub)
		})
	}
}

func TestIsPodPatchNotFound(t *testing.T) {
	status404 := map[string]interface{}{"ErrStatus": map[string]interface{}{"code": float64(404)}}
	for _, test := range []struct {
		name     string
		resource string
		err      interface{}
		want     bool
	}{
		{name: "pod patch 404", resource: "default/podpatch/pod", err: status404, want: true},
		{name: "pod 404", resource: "default/pod/pod", err: status404},
		{name: "forbidden", resource: "default/podpatch/pod", err: map[string]interface{}{"ErrStatus": map[string]interface{}{"code": float64(403)}}},
		{name: "conflict", resource: "default/podpatch/pod", err: map[string]interface{}{"ErrStatus": map[string]interface{}{"code": float64(409)}}},
		{name: "plain error", resource: "default/podpatch/pod", err: "not found"},
		{name: "missing status", resource: "default/podpatch/pod", err: map[string]interface{}{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			message := model.NewMessage("").BuildRouter(ModuleNameController, modules.MetaGroup, test.resource, model.PatchOperation)
			if got := isPodPatchNotFound(*message, test.err); got != test.want {
				t.Fatalf("isPodPatchNotFound() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestDeleteMissingPodBuildsNamedDelete(t *testing.T) {
	manager := newMetaManager(true)
	beehiveContext.AddModule(&common.ModuleInfo{ModuleName: manager.Name(), ModuleType: common.MsgCtxTypeChannel})
	t.Cleanup(func() { beehiveContext.Cleanup(manager.Name()) })
	deleted := make(chan model.Message, 1)
	go func() {
		request, err := beehiveContext.Receive(manager.Name())
		if err != nil {
			return
		}
		deleted <- request
		beehiveContext.SendResp(*request.NewRespByMessage(&request, OK))
	}()

	message := model.NewMessage("").BuildRouter(ModuleNameController, modules.MetaGroup,
		"default/podpatch/old-pod", model.PatchOperation).
		FillBody([]byte(`{"metadata":{"uid":"old-uid"}}`))
	if err := manager.deleteMissingPod(*message); err != nil {
		t.Fatalf("deleteMissingPod: %v", err)
	}
	var deleteMessage model.Message
	select {
	case deleteMessage = <-deleted:
	default:
		t.Fatal("deleteMissingPod did not send a delete request")
	}
	if deleteMessage.GetSource() != manager.Name() || deleteMessage.GetGroup() != modules.MetaGroup ||
		deleteMessage.GetResource() != "default/pod/old-pod" || deleteMessage.GetOperation() != model.DeleteOperation {
		t.Fatalf("unexpected delete route: %+v", deleteMessage.Router)
	}
	pod, ok := deleteMessage.GetContent().(*v1.Pod)
	if !ok || pod == nil || pod.Namespace != "default" || pod.Name != "old-pod" || pod.UID != "old-uid" {
		t.Fatalf("unexpected delete body: %T %+v", deleteMessage.GetContent(), deleteMessage.GetContent())
	}
}

// TestProcessQuery is function to test processQuery
func TestProcessQuery(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	ormerMock := beego.NewMockOrmer(mockCtrl)
	querySetterMock := beego.NewMockQuerySeter(mockCtrl)
	dbm.DBAccess = ormerMock
	meta := newMetaManager(true)
	core.Register(meta)

	add := &common.ModuleInfo{
		ModuleName: meta.Name(),
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(add)
	beehiveContext.AddModuleGroup(meta.Name(), meta.Group())
	edgeHub := &common.ModuleInfo{
		ModuleName: ModuleNameEdgeHub,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(edgeHub)
	beehiveContext.AddModuleGroup(ModuleNameEdgeHub, modules.HubGroup)
	edged := &common.ModuleInfo{
		ModuleName: ModuleNameEdged,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.AddModule(edged)

	connect.SetConnected(true)

	//process remote query jsonMarshall error
	msg := model.NewMessage("").BuildRouter(ModuleNameEdgeHub, GroupResource, "test/"+model.ResourceTypeConfigmap, model.QueryOperation).FillBody(make(chan int))
	meta.processQuery(*msg)
	message, _ := beehiveContext.Receive(ModuleNameEdgeHub)
	message.Header.ParentID = message.GetID()
	message.SetResourceOperation("test/"+model.ResourceTypeConfigmap, model.ResponseOperation)
	beehiveContext.SendResp(message)
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("ProcessRemoteQueryMarshallFail", func(t *testing.T) {
		if message.GetContent() != marshalError {
			t.Errorf("Wrong Error message received : Wanted %v and Got %v", marshalError, message.GetContent())
		}
	})

	//process remote query response error content
	msg = model.NewMessage("").BuildRouter(ModuleNameEdged, GroupResource, "test/"+model.ResourceTypeConfigmap, model.QueryOperation)
	meta.processQuery(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)
	msg = model.NewMessage(message.GetID()).BuildRouter(ModuleNameEdgeHub, GroupResource, "test/"+model.ResourceTypeConfigmap, model.QueryOperation).FillBody(fmt.Errorf("test"))
	beehiveContext.SendResp(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)
	msgEdged, _ := beehiveContext.Receive(ModuleNameEdged)
	t.Run("ProcessRemoteQueryResponseErrorContent", func(t *testing.T) {
		want := OK
		if message.GetContent().(string) != want {
			t.Errorf("Wrong Error message received : Wanted %v and Got %v", want, message.GetContent())
		}
		wantEdged := fmt.Errorf("test").Error()
		if msgEdged.GetContent().(error).Error() != wantEdged {
			t.Errorf("Wrong Error message received : Wanted %v and Got %v", wantEdged, msgEdged.GetContent().(error).Error())
		}
	})

	//process remote query db fail
	msg = model.NewMessage("").BuildRouter(ModuleNameEdgeHub, GroupResource, "test/"+model.ResourceTypeConfigmap, model.QueryOperation)
	meta.processQuery(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)
	msg = model.NewMessage(message.GetID()).BuildRouter(ModuleNameEdged, GroupResource, "test/"+model.ResourceTypeConfigmap, model.QueryOperation).FillBody("TestMessage")
	beehiveContext.SendResp(*msg)
	beehiveContext.Receive(ModuleNameEdgeHub)
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("ProcessRemoteQueryDbFail", func(t *testing.T) {
		want := "TestMessage"
		if message.GetContent() != want {
			t.Errorf("Wrong message received : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	// No error and connected true
	fakeDao := new([]dao.Meta)
	fakeDaoArray := make([]dao.Meta, 1)
	fakeDaoArray[0] = dao.Meta{Key: "Test", Value: MessageTest}
	fakeDao = &fakeDaoArray
	querySetterMock.EXPECT().All(gomock.Any()).SetArg(0, *fakeDao).Return(int64(1), nil).Times(1)
	querySetterMock.EXPECT().Filter(gomock.Any(), gomock.Any()).Return(querySetterMock).Times(1)
	ormerMock.EXPECT().QueryTable(gomock.Any()).Return(querySetterMock).Times(1)
	msg = model.NewMessage("").BuildRouter(ModuleNameEdgeHub, GroupResource, "test/"+model.ResourceTypeRule, model.QueryOperation)
	meta.processQuery(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("LocalDatabaseNoErrorAndMetaFound", func(t *testing.T) {
		want := make([]string, 1)
		want[0] = MessageTest
		bytesWant, _ := json.Marshal(want)
		bytesGot, _ := json.Marshal(message.GetContent())
		if string(bytesGot) != string(bytesWant) {
			t.Errorf("Wrong message receive : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	//ResId Nil database error
	querySetterMock.EXPECT().All(gomock.Any()).Return(int64(1), errFailedDBOperation).Times(1)
	querySetterMock.EXPECT().Filter(gomock.Any(), gomock.Any()).Return(querySetterMock).Times(1)
	ormerMock.EXPECT().QueryTable(gomock.Any()).Return(querySetterMock).Times(1)
	msg = model.NewMessage("").BuildRouter(ModuleNameEdgeHub, GroupResource, "test/"+model.ResourceTypeRule, model.QueryOperation)
	meta.processQuery(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("ResIDNilDatabaseError", func(t *testing.T) {
		want := "failed to query meta in DB: " + FailedDBOperation
		if message.GetContent() != want {
			t.Errorf("Wrong message receive : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	//ResID not nil database error
	querySetterMock.EXPECT().All(gomock.Any()).Return(int64(1), errFailedDBOperation).Times(1)
	querySetterMock.EXPECT().Filter(gomock.Any(), gomock.Any()).Return(querySetterMock).Times(1)
	ormerMock.EXPECT().QueryTable(gomock.Any()).Return(querySetterMock).Times(1)
	msg = model.NewMessage("").BuildRouter(ModuleNameEdgeHub, GroupResource, "test/"+model.ResourceTypeRule+"/test", model.QueryOperation)
	meta.processQuery(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdgeHub)
	t.Run("ResIDNotNilDatabaseError", func(t *testing.T) {
		want := "failed to query meta in DB: " + FailedDBOperation
		if message.GetContent() != want {
			t.Errorf("Wrong message receive : Wanted %v and Got %v", want, message.GetContent())
		}
	})

	//ResID not nil Success Case
	querySetterMock.EXPECT().All(gomock.Any()).SetArg(0, *fakeDao).Return(int64(1), nil).Times(1)
	querySetterMock.EXPECT().Filter(gomock.Any(), gomock.Any()).Return(querySetterMock).Times(1)
	ormerMock.EXPECT().QueryTable(gomock.Any()).Return(querySetterMock).Times(1)
	msg = model.NewMessage("").BuildRouter(ModuleNameEdgeHub, GroupResource, "test/"+model.ResourceTypeRule+"/test", model.QueryOperation)
	//beehiveContext.Send(MetaManagerModuleName, *msg)
	meta.processQuery(*msg)
	message, _ = beehiveContext.Receive(ModuleNameEdged)
	t.Run("DatabaseNoErrorAndMetaFound", func(t *testing.T) {
		want := make([]string, 1)
		want[0] = MessageTest
		bytesWant, _ := json.Marshal(want)
		bytesGot, _ := json.Marshal(message.GetContent())
		if string(bytesGot) != string(bytesWant) {
			t.Errorf("Wrong message receive : Wanted %v and Got %v", want, message.GetContent())
		}
	})
}
