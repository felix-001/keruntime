package metamanager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	cloudmodules "github.com/kubeedge/kubeedge/cloud/pkg/common/modules"
	edgecontrollerConstants "github.com/kubeedge/kubeedge/cloud/pkg/edgecontroller/constants"
	"github.com/kubeedge/kubeedge/common/constants"
	connect "github.com/kubeedge/kubeedge/edge/pkg/common/cloudconnection"
	edgeCommonMessage "github.com/kubeedge/kubeedge/edge/pkg/common/message"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/client"
	metaManagerConfig "github.com/kubeedge/kubeedge/edge/pkg/metamanager/config"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/kubernetes/storage/sqlite/imitator"
)

// Constants to check metamanager processes
const (
	OK                   = "OK"
	GroupResource        = "resource"
	CloudControllerModel = "edgecontroller"
	errNotConnected      = "not connected"
)

// PodDeleteSnapshot carries the full serialized Pod list queried from local MetaManager DB
// after a metadata-only Pod deletion so edged can reconcile kubelet via a SET update.
type PodDeleteSnapshot []string

const (
	podSnapshotRetryOperation = "pod-snapshot-retry"
)

var (
	podDeleteSnapshotRetryBackoff    = time.Second
	podDeleteSnapshotRetryMaxBackoff = 30 * time.Second
	podLocalCleanupTimeout           = 10 * time.Second
)

func feedbackError(err error, request model.Message) {
	errResponse := model.NewErrorMessage(&request, err.Error()).SetRoute(modules.MetaManagerModuleName, request.GetGroup())
	if request.GetSource() == modules.EdgedModuleName {
		sendToEdged(errResponse, request.IsSync())
	} else {
		sendToCloud(errResponse)
	}
}

func feedbackRemoteError(err error, request model.Message, parentID string) {
	if parentID == "" {
		parentID = request.GetID()
	}
	if request.GetSource() == modules.EdgedModuleName {
		_, resType, _, _, _ := parseResource(&request)
		if resType == model.ResourceTypePodPatch {
			statusErr := apierrors.NewInternalError(err)
			if strings.Contains(err.Error(), errNotConnected) {
				statusErr = apierrors.NewServiceUnavailable(err.Error())
			}
			resp := model.NewMessage(parentID).
				SetRoute(modules.MetaManagerModuleName, request.GetGroup()).
				SetResourceOperation(request.GetResource(), model.ResponseOperation).
				SetType(request.GetType()).
				FillBody(client.PodResp{Err: *statusErr})
			sendToEdged(resp, request.IsSync())
			return
		}
		errResponse := model.NewMessage(parentID).
			SetRoute(modules.MetaManagerModuleName, request.GetGroup()).
			SetResourceOperation(request.GetResource(), model.ResponseErrorOperation).
			SetType(request.GetType()).
			FillBody(err.Error())
		sendToEdged(errResponse, request.IsSync())
		return
	}
	feedbackError(err, request)
}

func feedbackResponse(message *model.Message, parentID string, resp *model.Message) {
	resp.BuildHeader(resp.GetID(), parentID, resp.GetTimestamp())
	sendToEdged(resp, message.IsSync())
	_, resType, _, _, _ := parseResource(message)
	if resType == model.ResourceTypePodPatch {
		return
	}
	respToCloud := message.NewRespByMessage(resp, OK)
	sendToCloud(respToCloud)
}

func sendToEdged(message *model.Message, sync bool) {
	if sync {
		beehiveContext.SendResp(*message)
	} else {
		beehiveContext.Send(modules.EdgedModuleName, *message)
	}
}

func sendToAppsd(message *model.Message, sync bool) {
	if sync {
		beehiveContext.SendResp(*message)
	} else {
		beehiveContext.Send(modules.AppsdModuleName, *message)
	}
}

func sendToCloud(message *model.Message) {
	beehiveContext.SendToGroup(string(metaManagerConfig.Config.ContextSendGroup), *message)
}

// Resource format: <namespace>/<restype>[/resid]
// return <reskey, restype, resid>
func parseResource(message *model.Message) (string, string, string, string, string) {
	resource := message.GetResource()
	tokens := strings.Split(resource, constants.ResourceSep)
	resType, resID, appName, domain := "", "", "", ""

	switch len(tokens) {
	case 2:
		resType = tokens[len(tokens)-1]
	case 3:
		resType = tokens[len(tokens)-2]
		resID = tokens[len(tokens)-1]
	case 4:
		resType = tokens[len(tokens)-3]
		resID = tokens[len(tokens)-2]
		appName = tokens[len(tokens)-1]
	case 5:
		resType = tokens[len(tokens)-4]
		resID = tokens[len(tokens)-3]
		appName = tokens[len(tokens)-2]
		domain = tokens[len(tokens)-1]
	default:
	}
	if resType != model.ResourceTypeServiceAccountToken {
		return resource, resType, resID, appName, domain
	}
	var tokenReq authenticationv1.TokenRequest
	content, err := message.GetContentData()
	if err != nil {
		klog.Errorf("failed to get token request from message %s, error %s", message.GetID(), err)
		return "", "", "", "", ""
	}
	if err = json.Unmarshal(content, &tokenReq); err != nil {
		klog.Errorf("failed to unmarshal token request from message %s, error %s", message.GetID(), err)
		return "", "", "", "", ""
	}

	trTokens := strings.Split(resource, constants.ResourceSep)
	if len(trTokens) != 3 {
		klog.Errorf("failed to get resource %s name and namespace", resource)
		return "", "", "", "", ""
	}
	return client.KeyFunc(trTokens[2], trTokens[0], &tokenReq), resType, "", "", ""
}

// Resource format: <namespace>/<restype>[/resid]
// return <reskey, namespace, restype, appname, domain>
func parseResourceFromAppsd(message *model.Message) (string, string, string, string, string) {
	resource := message.GetResource()
	tokens := strings.Split(resource, constants.ResourceSep)
	namespace, resType, appName, domain := "", "", "", ""

	switch len(tokens) {
	case 4:
		namespace = tokens[2]
		resType = tokens[3]
	case 5:
		namespace = tokens[2]
		resType = tokens[3]
		appName = tokens[4]
	case 6:
		namespace = tokens[2]
		resType = tokens[3]
		appName = tokens[4]
		domain = tokens[5]
	default:
	}
	return resource, namespace, resType, appName, domain
}

// is resource type require remote query
func requireRemoteQuery(resType string) bool {
	return resType == model.ResourceTypeConfigmap ||
		resType == model.ResourceTypeSecret ||
		resType == constants.ResourceTypePersistentVolume ||
		resType == constants.ResourceTypePersistentVolumeClaim ||
		resType == constants.ResourceTypeVolumeAttachment ||
		resType == model.ResourceTypeNode ||
		resType == model.ResourceTypeServiceAccountToken ||
		resType == model.ResourceTypeLease
}

func msgDebugInfo(message *model.Message) string {
	return fmt.Sprintf("msgID[%s] resource[%s]", message.GetID(), message.GetResource())
}

func parseLabels(labels map[string]string) (string, string) {
	if labels == nil || len(labels) == 0 {
		return "", ""
	}
	appName, domain := "", ""
	configType := labels[edgecontrollerConstants.ConfigType]
	if configType == constants.Native {
		if val, ok := labels[edgecontrollerConstants.AppName]; ok {
			appName = val
		}
		if val, ok := labels[edgecontrollerConstants.Domain]; ok {
			domain = val
		}
	}
	return appName, domain
}

func parseResourceFromObject(message *model.Message) (string, string, string) {
	data, _ := message.GetContentData()
	namespace, resourceType, resourceID, appName, domain := "", "", "", "", ""
	var secretObject corev1.Secret
	err := json.Unmarshal(data, &secretObject)
	if err == nil {
		labels := secretObject.Labels
		appName, domain = parseLabels(labels)
		namespace = secretObject.Namespace
		resourceType = model.ResourceTypeSecret
		resourceID = secretObject.Name
	} else {
		var configmapObject corev1.ConfigMap
		err = json.Unmarshal(data, &configmapObject)
		if err == nil {
			labels := configmapObject.Labels
			appName, domain = parseLabels(labels)
			namespace = configmapObject.Namespace
			resourceType = model.ResourceTypeConfigmap
			resourceID = configmapObject.Name
		}
	}
	resKey := ""
	if appName != "" || domain != "" {
		resKey, _ = edgeCommonMessage.BuildResource("", namespace, resourceType, resourceID, appName, domain)
	}
	return resKey, appName, domain
}

func (m *metaManager) handleMessage(message *model.Message) error {
	resKey, resType, _, appName, domain := parseResource(message)
	if appName == "" || domain == "" {
		objResKey, objAppName, objDomain := parseResourceFromObject(message)
		if objAppName != "" {
			appName = objAppName
			if objDomain != "" {
				domain = objDomain
			}
			if objResKey != "" {
				resKey = objResKey
			}
		}
	}
	switch message.GetOperation() {
	case model.InsertOperation, model.UpdateOperation, model.PatchOperation, model.ResponseOperation:
		content, err := message.GetContentData()
		if err != nil {
			klog.Errorf("get message content data failed, message: %s, error: %s", msgDebugInfo(message), err)
			return fmt.Errorf("get message content data failed, error: %s", err)
		}
		meta := &dao.Meta{
			Key:     resKey,
			Type:    resType,
			AppName: appName,
			Domain:  domain,
			Value:   string(content)}
		err = dao.InsertOrUpdate(meta)
		if err != nil {
			klog.Errorf("insert or update meta failed, message: %s, error: %v", msgDebugInfo(message), err)
			return fmt.Errorf("insert or update meta failed, %s", err)
		}
	case model.DeleteOperation:
		if resType == model.ResourceTypePod {
			err := processDeletePodDB(*message)
			if err != nil {
				klog.Errorf("delete pod meta failed, message %s, err: %v", msgDebugInfo(message), err)
				return fmt.Errorf("failed to delete pod meta to DB: %w", err)
			}
		} else {
			err := dao.DeleteMetaByKey(resKey)
			if err != nil {
				klog.Errorf("delete meta failed, %s", msgDebugInfo(message))
				return fmt.Errorf("delete meta failed, %s", err)
			}
		}
	}
	return nil
}

func (m *metaManager) processInsert(message model.Message) {
	imitator.DefaultV2Client.Inject(message)

	msgSource := message.GetSource()
	if msgSource == modules.EdgedModuleName {
		if !connect.IsConnected() {
			klog.Warningf("process remote failed, req[%s], err: %v", msgDebugInfo(&message), errNotConnected)
			feedbackError(fmt.Errorf("failed to process remote: %s", errNotConnected), message)
			return
		}
		m.processRemote(message)
		return
	}
	if err := m.handleMessage(&message); err != nil {
		feedbackError(err, message)
		return
	}
	if msgSource == cloudmodules.DeviceControllerModuleName {
		message.SetRoute(modules.MetaGroup, modules.DeviceTwinModuleName)
		beehiveContext.Send(modules.DeviceTwinModuleName, message)
	} else if msgSource != cloudmodules.PolicyControllerModuleName {
		// Notify edged
		sendToEdged(&message, false)
	}

	resp := message.NewRespByMessage(&message, OK)
	sendToCloud(resp)
}

func (m *metaManager) processUpdate(message model.Message) {
	imitator.DefaultV2Client.Inject(message)

	msgSource := message.GetSource()
	_, resType, _, _, _ := parseResource(&message)
	if msgSource == modules.EdgedModuleName && resType == model.ResourceTypeLease {
		if !connect.IsConnected() {
			klog.Warningf("process remote failed, req[%s], err: %v", msgDebugInfo(&message), errNotConnected)
			feedbackError(fmt.Errorf("failed to process remote: %s", errNotConnected), message)
			return
		}
		m.processRemote(message)
		return
	}
	if err := m.handleMessage(&message); err != nil {
		feedbackError(err, message)
		return
	}
	switch msgSource {
	case modules.EdgedModuleName:
		// For pod status update message, we need to wait for the response message
		// to ensure that the pod status is correctly reported to the kube-apiserver
		sendToCloud(&message)
		resp := message.NewRespByMessage(&message, OK)
		sendToEdged(resp, message.IsSync())
	case cloudmodules.EdgeControllerModuleName, cloudmodules.DynamicControllerModuleName:
		sendToEdged(&message, message.IsSync())
		resp := message.NewRespByMessage(&message, OK)
		sendToCloud(resp)
	case cloudmodules.DeviceControllerModuleName:
		resp := message.NewRespByMessage(&message, OK)
		sendToCloud(resp)
		message.SetRoute(modules.MetaGroup, modules.DeviceTwinModuleName)
		beehiveContext.Send(modules.DeviceTwinModuleName, message)
	case cloudmodules.PolicyControllerModuleName:
		resp := message.NewRespByMessage(&message, OK)
		sendToCloud(resp)
	default:
		klog.Errorf("unsupport message source, %s", msgSource)
	}
}

func (m *metaManager) processPatch(message model.Message) <-chan struct{} {
	_, resType, _, _, _ := parseResource(&message)
	if resType == model.ResourceTypePodPatch && message.GetSource() == modules.EdgedModuleName {
		if !connect.IsConnected() {
			klog.Warningf("process remote failed, req[%s], err: %v", msgDebugInfo(&message), errNotConnected)
			feedbackRemoteError(fmt.Errorf("failed to process remote: %s", errNotConnected), message, message.GetID())
			return nil
		}
		if err := m.handleMessage(&message); err != nil {
			feedbackRemoteError(err, message, message.GetID())
			return nil
		}
		klog.Infof("Processing pod status PATCH through processRemote, req[%s]", msgDebugInfo(&message))
		return m.processRemote(message)
	}
	if err := m.handleMessage(&message); err != nil {
		feedbackError(err, message)
		return nil
	}
	sendToCloud(&message)
	return nil
}

func (m *metaManager) processResponse(message model.Message) {
	if err := m.handleMessage(&message); err != nil {
		feedbackError(err, message)
		return
	}
	// Notify edged if the data is coming from cloud
	if message.GetSource() == CloudControllerModel {
		sendToEdged(&message, message.IsSync())
	} else {
		// Send to cloud if the update request is coming from edged
		sendToCloud(&message)
	}
}

func (m *metaManager) processDelete(message model.Message) {
	_, resType, _, _, _ := parseResource(&message)
	if resType == model.ResourceTypePod && message.GetSource() == modules.EdgedModuleName {
		// if pod is deleted in K8s, then a new delete message will be sent to edge
		sendToCloud(&message)
		return
	}
	// Pod deletions bypass Inject so meta_v2 removal is UID-guarded inside processDeletePodDB
	// via imitator.DefaultV2Client.DeletePodByUID.
	if resType != model.ResourceTypePod {
		imitator.DefaultV2Client.Inject(message)
	}

	localCleanup := resType == model.ResourceTypePod && message.GetSource() == m.Name() && message.IsSync()
	err := m.handleMessage(&message)
	edgedMessage := message
	snapshotPending := false
	if err == nil && resType == model.ResourceTypePod && message.GetSource() != cloudmodules.PolicyControllerModuleName {
		err = attachPodDeleteSnapshot(&edgedMessage)
		if err != nil {
			m.schedulePodDeleteSnapshotRetry(message)
			snapshotPending = true
			if localCleanup {
				beehiveContext.SendResp(*message.NewRespByMessage(&message, OK))
				return
			}
			err = nil
		}
	}
	if err != nil {
		if localCleanup {
			errResponse := model.NewMessage(message.GetID()).
				SetResourceOperation(message.GetResource(), model.ResponseErrorOperation).
				SetType(message.GetType()).
				FillBody(err.Error())
			beehiveContext.SendResp(*errResponse)
		} else {
			feedbackError(err, message)
		}
		return
	}
	msgSource := message.GetSource()
	if msgSource == cloudmodules.DeviceControllerModuleName {
		message.SetRoute(modules.MetaGroup, modules.DeviceTwinModuleName)
		beehiveContext.Send(modules.DeviceTwinModuleName, message)
	}

	if msgSource != cloudmodules.PolicyControllerModuleName && !snapshotPending {
		// Notify edged
		if localCleanup {
			beehiveContext.Send(modules.EdgedModuleName, edgedMessage)
			return
		}
		sendToEdged(&edgedMessage, false)
	}
	resp := message.NewRespByMessage(&message, OK)
	sendToCloud(resp)
}

func (m *metaManager) schedulePodDeleteSnapshotRetry(message model.Message) {
	m.podSnapshotRetryMu.Lock()
	if m.podSnapshotRetrySet {
		m.podSnapshotRetryMu.Unlock()
		return
	}
	m.podSnapshotRetrySet = true
	delay := m.podSnapshotRetryDelay
	if delay <= 0 {
		delay = podDeleteSnapshotRetryBackoff
	}
	nextDelay := delay * 2
	if podDeleteSnapshotRetryMaxBackoff > 0 && nextDelay > podDeleteSnapshotRetryMaxBackoff {
		nextDelay = podDeleteSnapshotRetryMaxBackoff
	}
	m.podSnapshotRetryDelay = nextDelay
	m.podSnapshotRetryMu.Unlock()

	retry := model.NewMessage("").BuildRouter(m.Name(), modules.MetaGroup,
		message.GetResource(), podSnapshotRetryOperation).FillBody(message.GetContent())
	go func() {
		if delay <= 0 {
			select {
			case <-beehiveContext.Done():
				return
			default:
				beehiveContext.Send(m.Name(), *retry)
				return
			}
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-beehiveContext.Done():
			return
		case <-timer.C:
			beehiveContext.Send(m.Name(), *retry)
		}
	}()
}

func (m *metaManager) processPodDeleteSnapshotRetry(message model.Message) {
	m.podSnapshotRetryMu.Lock()
	m.podSnapshotRetrySet = false
	m.podSnapshotRetryMu.Unlock()

	deleteMessage := message
	deleteMessage.SetResourceOperation(message.GetResource(), model.DeleteOperation)
	if err := attachPodDeleteSnapshot(&deleteMessage); err != nil {
		m.schedulePodDeleteSnapshotRetry(deleteMessage)
		return
	}
	m.podSnapshotRetryMu.Lock()
	m.podSnapshotRetryDelay = 0
	m.podSnapshotRetryMu.Unlock()
	sendToEdged(&deleteMessage, false)
}

// attachPodDeleteSnapshot replaces a metadata-only Pod delete payload (empty Spec)
// with the current local Pod list as PodDeleteSnapshot so edged can remove the Pod
// through kubelet SET without needing the deleted Pod's full Spec.
func attachPodDeleteSnapshot(message *model.Message) error {
	content, err := message.GetContentData()
	if err != nil {
		return err
	}
	var pod corev1.Pod
	if err := json.Unmarshal(content, &pod); err != nil {
		return err
	}
	if !reflect.DeepEqual(pod.Spec, corev1.PodSpec{}) {
		return nil
	}
	metas, err := queryMetasByType(model.ResourceTypePod)
	if err != nil {
		return fmt.Errorf("query pod snapshot after delete: %w", err)
	}
	message.SetRoute(modules.MetaManagerModuleName, message.GetGroup())
	message.FillBody(PodDeleteSnapshot(*metas))
	return nil
}

func processDeletePodDB(message model.Message) error {
	var msgPod corev1.Pod
	msgContent, err := message.GetContentData()
	if err != nil {
		return err
	}
	if err := json.Unmarshal(msgContent, &msgPod); err != nil {
		return err
	}
	if msgPod.Namespace == "" || msgPod.Name == "" {
		parts := strings.Split(message.GetResource(), constants.ResourceSep)
		if len(parts) == 3 && parts[1] == model.ResourceTypePod {
			if msgPod.Namespace == "" {
				msgPod.Namespace = parts[0]
			}
			if msgPod.Name == "" {
				msgPod.Name = parts[2]
			}
		}
	}
	if msgPod.UID == "" {
		if message.GetSource() == modules.MetaManagerModuleName {
			return fmt.Errorf("pod delete requires metadata.uid")
		}
		// Preserve base semantics for legacy cloud deletes without metadata.uid:
		// do not delete V1/meta_v2 records without UID confirmation so a recreated
		// same-name Pod cannot be removed, while still allowing processDelete to
		// refresh edged and ACK cloud.
		klog.Warningf("Skipping pod metadata deletion because message UID is empty, resource: %s, messageID: %s",
			message.GetResource(), message.GetID())
		return nil
	}
	podDBList, err := dao.QueryMeta("key", message.GetResource())
	if err != nil {
		return err
	}

	podList := *podDBList
	if len(podList) == 0 {
		klog.Infof("no pod with key %s key in DB", message.GetResource())
	} else {
		var podDB corev1.Pod
		if err := json.Unmarshal([]byte(podList[0]), &podDB); err != nil {
			return err
		}
		if podDB.UID != msgPod.UID {
			klog.Warningf("Skipping pod metadata deletion because UID does not match, resource: %s, storedPodUID: %s, messagePodUID: %s, messageID: %s",
				message.GetResource(), podDB.UID, msgPod.UID, message.GetID())
			// The V1 record belongs to a newer pod, so keep it, but meta_v2 may
			// still hold a record of the deleted pod from an earlier Inject that
			// failed to be replaced. Clean that up independently; deletePodV2
			// only removes records whose UID matches the deleted pod.
			return deletePodV2(msgPod)
		}
	}
	if err := deletePodV2(msgPod); err != nil {
		return err
	}

	podPatchKey := strings.Replace(message.GetResource(),
		constants.ResourceSep+model.ResourceTypePod+constants.ResourceSep,
		constants.ResourceSep+model.ResourceTypePodPatch+constants.ResourceSep, 1)
	err = dao.DeleteMetaByKey(podPatchKey)
	if err != nil {
		return err
	}
	if err := dao.DeleteMetaByKey(message.GetResource()); err != nil {
		return err
	}
	klog.InfoS("Deleted pod metadata before notifying edged",
		"resource", message.GetResource(),
		"pod", klog.KObj(&msgPod),
		"podUID", msgPod.UID,
		"messageID", message.GetID())

	return nil
}

func deletePodV2(pod corev1.Pod) error {
	return imitator.DefaultV2Client.DeletePodByUID(context.TODO(), pod)
}

func (m *metaManager) processQuery(message model.Message) {
	appName, domain := "", ""
	resKey, resType, resID, _, _ := parseResource(&message)
	if message.GetSource() == modules.AppsdModuleName {
		_, _, resType, appName, domain = parseResourceFromAppsd(&message)
	}

	var metas *[]string
	var err error
	if requireRemoteQuery(resType) && connect.IsConnected() {
		if message.GetSource() == modules.AppsdModuleName {
			conditions := map[string]string{"type": resType}
			if domain != "" {
				conditions["domain"] = domain
			}
			if appName != "" {
				conditions["appname"] = appName
			}
			metas, err = dao.QueryMetasByGroupCond(conditions)
			if err != nil {
				klog.Errorf("query meta failed, %s", msgDebugInfo(&message))
				feedbackError(fmt.Errorf("failed to query meta in DB: %s", err), message)
				return
			} else {
				resp := message.NewRespByMessage(&message, *metas)
				resp.SetRoute(modules.MetaManagerModuleName, resp.GetGroup())
				sendToAppsd(resp, message.IsSync())
			}
			return
		}
		m.processRemote(message)
		return
	}

	if resID == "" {
		// Get specific type resources
		metas, err = queryMetasByType(resType)
	} else {
		metas, err = dao.QueryMeta("key", resKey)
	}
	if err != nil {
		klog.Errorf("query meta failed, %s", msgDebugInfo(&message))
		feedbackError(fmt.Errorf("failed to query meta in DB: %s", err), message)
	} else {
		resp := message.NewRespByMessage(&message, *metas)
		resp.SetRoute(modules.MetaManagerModuleName, resp.GetGroup())
		sendToEdged(resp, message.IsSync())
	}
}

func queryMetasByType(resType string) (*[]string, error) {
	return dao.QueryMeta("type", resType)
}

func (m *metaManager) processRemote(message model.Message) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		// TODO: retry
		originalID := message.GetID()
		message.UpdateID()
		resp, err := m.sendSync(
			string(metaManagerConfig.Config.ContextSendModule),
			message,
			time.Duration(metaManagerConfig.Config.RemoteQueryTimeout)*time.Second)
		if err != nil {
			klog.Errorf("process remote failed, req[%s], err: %v", msgDebugInfo(&message), err)
			feedbackRemoteError(fmt.Errorf("failed to process remote: %s", err), message, originalID)
			return
		}
		klog.V(4).Infof("process remote: req[%s], resp[%s]", msgDebugInfo(&message), msgDebugInfo(&resp))
		if raw, ok := resp.GetContent().([]byte); ok {
			if string(raw) == constants.MessageSuccessfulContent {
				resp.Content = constants.MessageSuccessfulContent
			} else {
				var decoded interface{}
				if err := json.Unmarshal(raw, &decoded); err != nil {
					klog.Errorf("unmarshal remote response failed, req[%s], err: %v", msgDebugInfo(&message), err)
					feedbackRemoteError(err, message, originalID)
					return
				}
				resp.Content = decoded
			}
		}
		content, ok := resp.GetContent().(string)
		if ok && content == constants.MessageSuccessfulContent {
			klog.V(4).Infof("process remote successfully")
			feedbackResponse(&message, originalID, &resp)
			return
		}
		errContent, ok := resp.GetContent().(error)
		if ok {
			klog.V(4).Infof("process remote err: %v", errContent)
			feedbackResponse(&message, originalID, &resp)
			return
		}
		mapContent, ok := resp.GetContent().(map[string]interface{})
		if ok && isObjectResp(mapContent) {
			if mapContent["Err"] != nil {
				if isPodPatchNotFound(message, mapContent["Err"]) || isPodPatchUIDMismatch(message, mapContent["Err"]) {
					klog.Infof("Pod status PATCH proved the cached UID is gone, starting local cleanup, req[%s]", msgDebugInfo(&message))
					if err := m.deleteMissingPod(message); err != nil {
						klog.Errorf("delete missing pod failed, req[%s], err: %v", msgDebugInfo(&message), err)
						resp.FillBody(client.PodResp{Err: *apierrors.NewInternalError(err)})
						feedbackResponse(&message, originalID, &resp)
						return
					}
				}
				klog.V(4).Infof("process remote objResp err: %v", mapContent["Err"])
				feedbackResponse(&message, originalID, &resp)
				return
			}
			klog.V(4).Infof("process remote objResp: %+v", mapContent["Object"])
			_, resType, _, _, _ := parseResource(&message)
			if resType == model.ResourceTypePodPatch || resType == model.ResourceTypeLease {
				feedbackResponse(&message, originalID, &resp)
				return
			}
			if resType == model.ResourceTypeNode && message.GetOperation() == model.InsertOperation {
				cacheResp := resp
				cacheResp.Content = mapContent["Object"]
				if err := m.handleMessage(&cacheResp); err != nil {
					feedbackRemoteError(err, message, originalID)
					return
				}
				feedbackResponse(&message, originalID, &resp)
				return
			}
			resp.Content = mapContent["Object"]
		}
		if err := m.handleMessage(&resp); err != nil {
			feedbackRemoteError(err, message, originalID)
			return
		}
		feedbackResponse(&message, originalID, &resp)
	}()
	return done
}

// IsPodPatchNotFoundStatus reports whether status proves the named Pod does not exist.
func IsPodPatchNotFoundStatus(status *metav1.Status, podName string) bool {
	if status == nil || podName == "" {
		return false
	}
	if status.Code != http.StatusNotFound || status.Reason != metav1.StatusReasonNotFound {
		return false
	}
	if status.Details == nil || status.Details.Group != "" || status.Details.Kind != "pods" || status.Details.Name != podName {
		return false
	}
	for _, cause := range status.Details.Causes {
		if cause.Type == metav1.CauseTypeUnexpectedServerResponse {
			return false
		}
	}
	return true
}

// IsPodPatchUIDMismatchStatus reports whether status proves the named Pod's metadata.uid differs on the server.
func IsPodPatchUIDMismatchStatus(status *metav1.Status, podName string) bool {
	if status == nil || podName == "" {
		return false
	}
	if status.Code != http.StatusUnprocessableEntity || status.Reason != metav1.StatusReasonInvalid {
		return false
	}
	if status.Details == nil || status.Details.Group != "" || status.Details.Kind != "Pod" || status.Details.Name != podName {
		return false
	}
	for _, cause := range status.Details.Causes {
		if cause.Field == "metadata.uid" && cause.Type == metav1.CauseTypeFieldValueInvalid {
			return true
		}
	}
	return false
}

func decodeStatusError(errContent interface{}) (*metav1.Status, bool) {
	errMap, ok := errContent.(map[string]interface{})
	if !ok {
		return nil, false
	}
	rawStatus, ok := errMap["ErrStatus"]
	if !ok || rawStatus == nil {
		return nil, false
	}
	data, err := json.Marshal(rawStatus)
	if err != nil {
		return nil, false
	}
	var status metav1.Status
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, false
	}
	return &status, true
}

func isPodPatchNotFound(message model.Message, errContent interface{}) bool {
	_, resourceType, podName, _, _ := parseResource(&message)
	if resourceType != model.ResourceTypePodPatch || podName == "" {
		return false
	}
	status, ok := decodeStatusError(errContent)
	if !ok {
		return false
	}
	return IsPodPatchNotFoundStatus(status, podName)
}

func isPodPatchUIDMismatch(message model.Message, errContent interface{}) bool {
	_, resourceType, podName, _, _ := parseResource(&message)
	if resourceType != model.ResourceTypePodPatch || podName == "" {
		return false
	}
	status, ok := decodeStatusError(errContent)
	if !ok {
		return false
	}
	return IsPodPatchUIDMismatchStatus(status, podName)
}

func (m *metaManager) deleteMissingPod(patchMessage model.Message) error {
	patch, err := patchMessage.GetContentData()
	if err != nil {
		return err
	}
	var pod corev1.Pod
	if err := json.Unmarshal(patch, &pod); err != nil {
		return err
	}
	if pod.UID == "" {
		return fmt.Errorf("pod status patch requires metadata.uid")
	}
	parts := strings.Split(patchMessage.GetResource(), constants.ResourceSep)
	if len(parts) != 3 || parts[0] == "" || parts[1] != model.ResourceTypePodPatch || parts[2] == "" {
		return fmt.Errorf("invalid pod patch resource: %s", patchMessage.GetResource())
	}
	pod.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
	pod.Namespace, pod.Name = parts[0], parts[2]
	resource := strings.Replace(patchMessage.GetResource(),
		constants.ResourceSep+model.ResourceTypePodPatch+constants.ResourceSep,
		constants.ResourceSep+model.ResourceTypePod+constants.ResourceSep, 1)
	deleteMessage := model.NewMessage("").
		BuildRouter(m.Name(), modules.MetaGroup, resource, model.DeleteOperation).
		FillBody(&pod)
	resp, err := m.sendSync(m.Name(), *deleteMessage, podLocalCleanupTimeout)
	if err != nil {
		return fmt.Errorf("wait for local pod cleanup: %w", err)
	}
	if resp.GetContent() != OK {
		return fmt.Errorf("local pod cleanup failed: %v", resp.GetContent())
	}
	return nil
}

func isObjectResp(data map[string]interface{}) bool {
	_, ok := data["Object"]
	if !ok {
		return false
	}
	_, ok = data["Err"]
	return ok
}

func (m *metaManager) processVolume(message model.Message) {
	klog.Info("process volume started")
	back, err := beehiveContext.SendSync(modules.EdgedModuleName, message, constants.CSISyncMsgRespTimeout)
	klog.Infof("process volume get: req[%+v], back[%+v], err[%+v]", message, back, err)
	if err != nil {
		klog.Errorf("process volume send to edged failed: %v", err)
	}

	resp := message.NewRespByMessage(&message, back.GetContent())
	sendToCloud(resp)
	klog.Infof("process volume send to cloud resp[%+v]", resp)
}

func (m *metaManager) process(message model.Message) {
	operation := message.GetOperation()

	switch operation {
	case podSnapshotRetryOperation:
		m.processPodDeleteSnapshotRetry(message)
	case model.InsertOperation:
		m.processInsert(message)
	case model.UpdateOperation:
		m.processUpdate(message)
	case model.PatchOperation:
		m.processPatch(message)
	case model.DeleteOperation:
		m.processDelete(message)
	case model.QueryOperation:
		m.processQuery(message)
	case model.ResponseOperation:
		m.processResponse(message)
	case constants.CSIOperationTypeCreateVolume,
		constants.CSIOperationTypeDeleteVolume,
		constants.CSIOperationTypeControllerPublishVolume,
		constants.CSIOperationTypeControllerUnpublishVolume:
		m.processVolume(message)
	default:
		klog.Errorf("metamanager not supported operation: %v", operation)
	}
}

func (m *metaManager) runMetaManager() {
	go func() {
		for {
			select {
			case <-beehiveContext.Done():
				klog.Warning("MetaManager main loop stop")
				return
			default:
			}
			msg, err := beehiveContext.Receive(m.Name())
			if err != nil {
				klog.Errorf("get a message %+v: %v", msg, err)
				continue
			}
			klog.V(2).Infof("get a message %+v", msg)
			m.process(msg)
		}
	}()
}
