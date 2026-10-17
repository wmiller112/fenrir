package controllers

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"encoding/json"
	"hash/fnv"
	"maps"
	"net"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"games-on-whales.github.io/direwolf/pkg/api/v1alpha1"
	v1alpha1types "games-on-whales.github.io/direwolf/pkg/api/v1alpha1"
	v1alpha1client "games-on-whales.github.io/direwolf/pkg/generated/clientset/versioned/typed/api/v1alpha1"
	"games-on-whales.github.io/direwolf/pkg/generic"
	"games-on-whales.github.io/direwolf/pkg/util"
	"games-on-whales.github.io/direwolf/pkg/wolfapi"
	// "github.com/pelletier/go-toml/v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	v1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	gatewayv1alpha2 "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned/typed/apis/v1alpha2"
)

var (
	WOLF_IMAGE = func() string {
		if im := os.Getenv("WOLF_IMAGE"); im != "" {
			return im
		}

		return "ghcr.io/games-on-whales/wolf:stable"
	}()
)

type userGame struct {
	User string
	Game string
}

type SessionControllerOptions struct {
	WolfAgentImage string
	LBSharingKey   string
}

// Session Controller manages the lifecycle of a streaming session for
// a given game, of a given user.
// If is responsible for:
//   - 1. Setting up port forwards via Gateway API
//   - 2. Setting up service, pods, etc. for session
//   - 3. Polling the pods wolf-agent to find when session is complete, cleaning up
//   - 4. Calling fake-udev to set up the controllers for the game (wolf-agent instead, probably)
//   - 5. Cleaning up all resources when session is complete
//
// Watchers lists of users and games to:
//   - 1. Delete sessions for games that were deleted
type SessionController struct {
	SessionClient   v1alpha1client.SessionInterface
	SessionInformer generic.Informer[*v1alpha1types.Session]

	AppInformer  generic.Informer[*v1alpha1types.App]
	UserInformer generic.Informer[*v1alpha1types.User]

	TCPRouteClient gatewayv1alpha2.TCPRouteInterface
	UDPRouteClient gatewayv1alpha2.UDPRouteInterface

	K8sClient kubernetes.Interface

	trackedSessions map[userGame]sets.Set[string]
	trackedGames    map[string]userGame

	// Port slot reserved by each user's game, so concurrent streams on the
	// shared LoadBalancer IP don't collide. Guarded by portsMu since the
	// session controller runs several workers.
	portsMu   sync.Mutex
	portSlots map[userGame]int32

	// Serializes creating and joining Wolf lobbies for co-op apps
	lobbyMu sync.Mutex
	// Serializes picking a Wolf client and adding a session with it
	addSessionMu sync.Mutex

	controller           generic.Controller[*v1alpha1types.Session]
	deploymentController generic.Controller[*appsv1.Deployment]
	SessionControllerOptions
}

// NewSessionController creates a new session controller.
func NewSessionController(
	k8sClient kubernetes.Interface,
	tcpRouteClient gatewayv1alpha2.TCPRouteInterface,
	udpRouteClient gatewayv1alpha2.UDPRouteInterface,
	sessionClient v1alpha1client.SessionInterface,
	sessionInformer generic.Informer[*v1alpha1types.Session],
	appInformer generic.Informer[*v1alpha1types.App],
	userInformer generic.Informer[*v1alpha1types.User],
	deploymentInformer generic.Informer[*appsv1.Deployment],
	options SessionControllerOptions,
) *SessionController {
	res := &SessionController{
		K8sClient:                k8sClient,
		TCPRouteClient:           tcpRouteClient,
		UDPRouteClient:           udpRouteClient,
		SessionClient:            sessionClient,
		SessionInformer:          sessionInformer,
		AppInformer:              appInformer,
		UserInformer:             userInformer,
		trackedSessions:          make(map[userGame]sets.Set[string]),
		trackedGames:             make(map[string]userGame),
		portSlots:                make(map[userGame]int32),
		SessionControllerOptions: options,
	}

	res.controller = generic.NewController(
		sessionInformer,
		res.Reconcile,
		generic.ControllerOptions{
			Name:    "session-controller",
			Workers: 2,
		},
	)

	//!TODO: Also watch any udproutes, services, deployments, etc. that we create
	// and re-reconcile their sessions when they change.
	res.deploymentController = generic.NewController(
		deploymentInformer,
		func(namepace, name string, newObj *appsv1.Deployment) error {
			// Load bearing. If we pass nil it will be casted to interface and
			// not be comparable with nil :)
			if newObj == nil {
				return nil
			}
			return res.reconcileDependant(newObj)
		},
		generic.ControllerOptions{
			Name:    "session-controller-deployment",
			Workers: 1,
		},
	)

	return res
}

func (c *SessionController) Run(ctx context.Context) error {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Apps are needed too, to know which sessions share a co-op pod
	if !cache.WaitForCacheSync(sessionCtx.Done(), c.SessionInformer.HasSynced, c.AppInformer.HasSynced) {
		return fmt.Errorf("failed to sync session and app informers")
	}

	// Build initial listing of sessions
	sessions, err := c.SessionInformer.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("failed to list sessions: %v", err)
	}

	for _, session := range sessions {
		ug := c.podKey(session)
		if existing, ok := c.trackedSessions[ug]; ok {
			existing.Insert(session.Name)
		} else {
			c.trackedSessions[ug] = sets.New(session.Name)
		}

		c.trackedGames[session.Name] = ug
	}

	go func() {
		defer cancel()
		err := c.deploymentController.Run(sessionCtx)
		if err != nil {
			klog.Errorf("Failed to run deployment controller: %v", err)
		}
	}()

	return c.controller.Run(sessionCtx)
}

func (c *SessionController) HasSynced() bool {
	return c.SessionInformer.HasSynced()
}

type K8sObject interface {
	metav1.Object
	runtime.Object
}

func (c *SessionController) reconcileDependant(obj K8sObject) error {
	// If object doesnt have direwolf/user and direwolf/app labels, skip
	if obj.GetLabels() == nil {
		return nil
	}

	if _, ok := obj.GetLabels()["direwolf/user"]; !ok {
		return nil
	}

	if _, ok := obj.GetLabels()["direwolf/app"]; !ok {
		return nil
	}

	klog.Infof("Reconciling dependant %s %s/%s", obj.GetObjectKind().GroupVersionKind().String(), obj.GetNamespace(), obj.GetName())

	// Lookup sessions associated with his object
	for _, owner := range obj.GetOwnerReferences() {
		if owner.Kind == "Session" {
			klog.Infof("Found owner %s/%s", owner.Name, owner.UID)
			c.controller.Enqueue(obj.GetNamespace(), owner.Name)
		}
	}

	return nil
}

func (c *SessionController) Reconcile(namespace, name string, newObj *v1alpha1types.Session) error {
	klog.Infof("Reconciling session %s/%s", namespace, name)
	defer klog.Infof("Finished Reconciling session %s/%s", namespace, name)

	if newObj == nil {
		// Session was deleted. Stuff will be garbage collected by Kubernetes
		// due to owner references. Nothing to do.
		if gam, ok := c.trackedGames[name]; ok {
			if existing, ok := c.trackedSessions[gam]; ok {
				existing.Delete(name)
				if existing.Len() == 0 {
					delete(c.trackedSessions, gam)
					c.releasePorts(gam)
				}
			}

			delete(c.trackedGames, name)
		}
		return nil
	} else if newObj.Status.WolfSessionID == "" && newObj.CreationTimestamp.Add(1*time.Minute).Before(time.Now()) {
		klog.Infof("Session %s/%s is older than 1 minute and has no wolf session ID, deleting", newObj.Namespace, newObj.Name)
		err := c.SessionClient.Delete(context.TODO(), newObj.Name, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			klog.Errorf("Failed to delete session %s/%s: %v", newObj.Namespace, newObj.Name, err)
			return err
		}
		return nil
	}
	ug := c.podKey(newObj)

	if existing, ok := c.trackedSessions[ug]; ok {
		existing.Insert(newObj.Name)
	} else {
		c.trackedSessions[ug] = sets.New(newObj.Name)
	}
	c.trackedGames[newObj.Name] = ug

	oldStatus := newObj.Status.DeepCopy()
	portsError := c.allocatePorts(context.TODO(), newObj)

	if portsError != nil {
		klog.Errorf("Failed to allocate ports: %s", portsError)
		meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
			Type:    "PortsAllocated",
			Status:  metav1.ConditionFalse,
			Reason:  "PortsAllocationFailed",
			Message: portsError.Error(),
		})
	} else {
		meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
			Type:   "PortsAllocated",
			Status: metav1.ConditionTrue,
			Reason: "Success",
		})
	}

	// configError := c.reconcileConfigMap(context.TODO(), newObj)
	// if configError != nil {
	// 	klog.Errorf("Failed to reconcile configmap: %s", configError)
	// 	meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
	// 		Type:    "ConfigMapCreated",
	// 		Status:  metav1.ConditionFalse,
	// 		Reason:  "ConfigMapCreationFailed",
	// 		Message: configError.Error(),
	// 	})
	// } else {
	// 	meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
	// 		Type:   "ConfigMapCreated",
	// 		Status: metav1.ConditionTrue,
	// 		Reason: "Success",
	// 	})
	// }

	if pvcError := c.reconcilePVC(context.TODO(), newObj); pvcError != nil {
		klog.Errorf("Failed to reconcile pvc: %s", pvcError)
		meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
			Type:    "VolumeCreated",
			Status:  metav1.ConditionFalse,
			Reason:  "PVCAllocationFailed",
			Message: pvcError.Error(),
		})
	} else {
		meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
			Type:   "VolumeCreated",
			Status: metav1.ConditionTrue,
			Reason: "Success",
		})
	}

	if podError := c.reconcilePod(context.TODO(), newObj); podError != nil {
		klog.Errorf("Failed to reconcile pod: %s", podError)
		meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
			Type:    "DeploymentCreated",
			Status:  metav1.ConditionFalse,
			Reason:  "PodCreationFailed",
			Message: podError.Error(),
		})
	} else {
		meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
			Type:   "DeploymentCreated",
			Status: metav1.ConditionTrue,
			Reason: "Success",
		})
	}

	if serviceError := c.reconcileService(context.TODO(), newObj); serviceError != nil {
		klog.Errorf("Failed to reconcile service: %s", serviceError)
		meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
			Type:    "ServiceCreated",
			Status:  metav1.ConditionFalse,
			Reason:  "ServiceCreationFailed",
			Message: serviceError.Error(),
		})
	} else {
		meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
			Type:   "ServiceCreated",
			Status: metav1.ConditionTrue,
			Reason: "ServiceCreated",
		})
	}

	// Gateway not yet supported
	// if gatewayError := c.reconcileGateway(context.TODO(), newObj); gatewayError != nil {
	// 	klog.Errorf("Failed to reconcile gateway: %s", gatewayError)
	// 	meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
	// 		Type:    "RoutesCreated",
	// 		Status:  metav1.ConditionFalse,
	// 		Reason:  "GatewayConfigurationFailed",
	// 		Message: gatewayError.Error(),
	// 	})
	// } else {
	// 	meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
	// 		Type:   "RoutesCreated",
	// 		Status: metav1.ConditionTrue,
	// 		Reason: "Success",
	// 	})
	// }

	if streamError := c.reconcileActiveStreams(context.TODO(), newObj); streamError != nil {
		klog.Errorf("Failed to reconcile active streams: %s", streamError)
		meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
			Type:    "StreamStarted",
			Status:  metav1.ConditionFalse,
			Reason:  "StreamStartFailed",
			Message: streamError.Error(),
		})
	} else {
		meta.SetStatusCondition(&newObj.Status.Conditions, metav1.Condition{
			Type:   "StreamStarted",
			Status: metav1.ConditionTrue,
			Reason: "WaitForPing", //!TOOD: use actual current stream status?
		})
	}

	// Set the new status, if it is changed
	if !reflect.DeepEqual(newObj.Status, oldStatus) {
		_, err := c.SessionClient.UpdateStatus(
			context.TODO(),
			newObj,
			metav1.UpdateOptions{
				FieldManager: "session-controller-status",
			},
		)

		// Failed to update status....nothing to do but try again with
		// exponential backoff. Could be API server issue. Depends on response
		// code?
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	//!TODO: figure our retry logic. Some of these errors surely are retriable
	return nil
}

// // !TODO: Unused. Part of testing gateway implementation. The final idea is for
// // Direwolf to dynamically set up port forwards / UDPRoutes via Kubernetes
// // Gateway API for RTSP, ENet, Video RTP, Audio RTP.
// func (c *SessionController) reconcileGateway(ctx context.Context, session *v1alpha1types.Session) error {
// 	// 1. Decide the ports this session will use for RTSP, Enet, Video RTP, Audio RTP
// 	// 2. Create TCPRoute for RTSP, UDP routes for Enet, RTP via Gateway API
// 	if !meta.IsStatusConditionPresentAndEqual(session.Status.Conditions, "PortsAllocated", metav1.ConditionTrue) {
// 		return fmt.Errorf("waiting for PortsAllocated")
// 	}

// 	_, err := c.UDPRouteClient.Apply(
// 		ctx,
// 		gatewayv1alpha2ac.UDPRoute(session.Name, session.Namespace).
// 			WithOwnerReferences(metav1ac.OwnerReference().
// 				WithName(session.Name).
// 				WithAPIVersion(v1alpha1.GroupVersion.String()).
// 				WithKind("Session").
// 				WithUID(session.UID).
// 				WithController(true)).
// 			WithLabels(
// 				map[string]string{
// 					"app":           "direwolf-worker",
// 					"direwolf/app":  session.Spec.GameReference.Name,
// 					"direwolf/user": session.Spec.UserReference.Name,
// 				}).
// 			WithSpec(
// 				gatewayv1alpha2ac.UDPRouteSpec().
// 					WithParentRefs(gatewayv1ac.ParentReference().
// 						WithKind("Gateway").
// 						WithGroup("gateway.networking.k8s.io").
// 						WithName(gatewayv1.ObjectName(session.Spec.GatewayReference.Name)).
// 						WithNamespace(gatewayv1.Namespace(session.Spec.GatewayReference.Namespace))).
// 					WithRules(
// 						gatewayv1alpha2ac.UDPRouteRule().
// 							WithName(gatewayv1.SectionName(session.Name)).
// 							WithBackendRefs(
// 								gatewayv1ac.BackendRef().
// 									WithPort(gatewayv1.PortNumber(session.Status.Ports.Control)).
// 									WithKind(gatewayv1.Kind("Service")).
// 									WithName(gatewayv1.ObjectName(session.Namespace)).
// 									WithNamespace(gatewayv1.Namespace(session.Namespace)),
// 								gatewayv1ac.BackendRef().
// 									WithPort(gatewayv1.PortNumber(session.Status.Ports.VideoRTP)).
// 									WithKind(gatewayv1.Kind("Service")).
// 									WithName(gatewayv1.ObjectName(session.Namespace)).
// 									WithNamespace(gatewayv1.Namespace(session.Namespace)),
// 								gatewayv1ac.BackendRef().
// 									WithPort(gatewayv1.PortNumber(session.Status.Ports.AudioRTP)).
// 									WithKind(gatewayv1.Kind("Service")).
// 									WithName(gatewayv1.ObjectName(session.Namespace)).
// 									WithNamespace(gatewayv1.Namespace(session.Namespace)),
// 							),
// 					),
// 			),
// 		metav1.ApplyOptions{
// 			FieldManager: "direwolf-session-controller-udp-route",
// 			Force:        true,
// 		},
// 	)
// 	if err != nil {
// 		return fmt.Errorf("failed to apply udp route: %s", err)
// 	}

// 	_, err = c.TCPRouteClient.Apply(
// 		ctx,
// 		gatewayv1alpha2ac.TCPRoute(session.Name, session.Namespace).
// 			WithOwnerReferences(metav1ac.OwnerReference().
// 				WithName(session.Name).
// 				WithAPIVersion(v1alpha1.GroupVersion.String()).
// 				WithKind("Session").
// 				WithUID(session.UID).
// 				WithController(true)).
// 			WithLabels(
// 				map[string]string{
// 					"app":           "direwolf-worker",
// 					"direwolf/app":  session.Spec.GameReference.Name,
// 					"direwolf/user": session.Spec.UserReference.Name,
// 				}).
// 			WithSpec(
// 				gatewayv1alpha2ac.TCPRouteSpec().
// 					WithParentRefs(gatewayv1ac.ParentReference().
// 						WithKind("Gateway").
// 						WithGroup("gateway.networking.k8s.io").
// 						WithName(gatewayv1.ObjectName(session.Spec.GatewayReference.Name)).
// 						WithNamespace(gatewayv1.Namespace(session.Spec.GatewayReference.Namespace))).
// 					WithRules(
// 						gatewayv1alpha2ac.TCPRouteRule().
// 							WithName(gatewayv1.SectionName(session.Name)).
// 							WithBackendRefs(
// 								gatewayv1ac.BackendRef().
// 									WithPort(gatewayv1.PortNumber(session.Status.Ports.RTSP)).
// 									WithKind(gatewayv1.Kind("Service")).
// 									WithName(gatewayv1.ObjectName(session.Namespace)).
// 									WithNamespace(gatewayv1.Namespace(session.Namespace)),
// 							),
// 					),
// 			),
// 		metav1.ApplyOptions{
// 			FieldManager: "direwolf-session-controller-TCP-route",
// 			Force:        true,
// 		},
// 	)
// 	if err != nil {
// 		return fmt.Errorf("failed to apply TCP route: %s", err)
// 	}

// 	return nil
// }

func (c *SessionController) reconcileService(ctx context.Context, session *v1alpha1types.Session) error {
	if !meta.IsStatusConditionPresentAndEqual(session.Status.Conditions, "PortsAllocated", metav1.ConditionTrue) {
		return fmt.Errorf("waiting for PortsAllocated")
	}

	clampString := func(s string, max int) string {
		if len(s) > max {
			return s[:max]
		}
		return s
	}

	// Each session gets its own service, replacing older ones of its pod. Co-op
	// sessions stream at the same time, so they share one service instead,
	// owned by all of them.
	serviceName := session.Name
	owners := []*metav1ac.OwnerReferenceApplyConfiguration{
		metav1ac.OwnerReference().
			WithName(session.Name).
			WithAPIVersion(v1alpha1.GroupVersion.String()).
			WithKind("Session").
			WithUID(session.UID).
			WithController(true),
	}
	if c.coopConfig(session) != nil {
		serviceName = c.deploymentName(session)
		owners = ownerApplyConfigs(c.sessionOwners(session))
	}
	session.Status.ServiceName = fmt.Sprintf("%s-rtp", clampString(serviceName, 56))

	podLabels := c.podLabels(session)
	serviceLabels := map[string]string{"app": "direwolf-worker"}
	maps.Copy(serviceLabels, podLabels)

	// Delete older services for this pod: they point at the same pod with the
	// same ports. Other pods' streams have their own ports and are left alone.
	allServices, err := c.K8sClient.CoreV1().Services(session.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(serviceLabels).String(),
	})

	if err != nil {
		return fmt.Errorf("failed to list services: %s", err)
	}

	for _, svc := range allServices.Items {
		if svc.Name != session.Status.ServiceName {
			klog.Infof("Deleting service %s/%s", svc.Namespace, svc.Name)
			err := c.K8sClient.CoreV1().Services(svc.Namespace).Delete(ctx, svc.Name, metav1.DeleteOptions{})
			if err != nil {
				klog.Errorf("Failed to delete service %s/%s: %s", svc.Namespace, svc.Name, err)
				return fmt.Errorf("failed to delete service %s/%s: %s", svc.Namespace, svc.Name, err)
			}
		}
	}

	// 1. Use the set up a service with correct ports pointing to the pods
	_, err = c.K8sClient.CoreV1().
		Services(session.Namespace).
		Apply(
			context.Background(),
			v1ac.Service(session.Status.ServiceName, session.Namespace).
				WithAnnotations(map[string]string{
					// Try to support popular service LoadBalancer implementation
					// sharing key annotations.
					"lbipam.cilium.io/sharing-key":        c.LBSharingKey,
					"metallb.universe.tf/allow-shared-ip": c.LBSharingKey,
				}).
				WithLabels(serviceLabels).
				WithOwnerReferences(owners...).
				WithSpec(
					v1ac.ServiceSpec().
						WithType(corev1.ServiceTypeLoadBalancer).
						WithSelector(podLabels).
						WithPorts(
							v1ac.ServicePort().
								WithName("wa"). // wolf-agent
								WithPort(wolfAgentServicePort(session.Status.Ports)).
								WithTargetPort(intstr.FromInt32(wolfAgentPort)),
							v1ac.ServicePort().
								WithName("rtsp"). // moonlight-rtsp
								WithPort(session.Status.Ports.RTSP),
							v1ac.ServicePort().
								WithName("enet"). // moonlight-enet
								WithProtocol(corev1.ProtocolUDP).
								WithPort(session.Status.Ports.Control),
							v1ac.ServicePort().
								WithName("video"). // moonlight-video
								WithProtocol(corev1.ProtocolUDP).
								WithPort(session.Status.Ports.VideoRTP),
							v1ac.ServicePort().
								WithName("audio"). // moonlight-audio
								WithProtocol(corev1.ProtocolUDP).
								WithPort(session.Status.Ports.AudioRTP),
						),
				),
			metav1.ApplyOptions{
				FieldManager: "direwolf-session-controller-svc",
			})
	if err != nil {
		return fmt.Errorf("failed to apply service: %s", err)
	}
	return nil
}

// mergeResourceRequirements merges a default and an override ResourceRequirements object for sidecars.
// It gives precedence to the values specified in the overrides.
func mergeResourceRequirements(defaults corev1.ResourceRequirements, overrides *corev1.ResourceRequirements) corev1.ResourceRequirements {
	if overrides == nil {
		return defaults
	}

	// Start with a copy of the defaults
	merged := defaults.DeepCopy()

	// Ensure maps are initialized
	if merged.Limits == nil {
		merged.Limits = make(corev1.ResourceList)
	}
	if merged.Requests == nil {
		merged.Requests = make(corev1.ResourceList)
	}

	// Override limits
	for resourceName, quantity := range overrides.Limits {
		merged.Limits[resourceName] = quantity
	}

	// Override requests
	for resourceName, quantity := range overrides.Requests {
		merged.Requests[resourceName] = quantity
	}

	return *merged
}

// validateAppResources checks if the app's resource requirements are within the user's policy.
// It returns an error if any app request/limit exceeds the user policy.
// If the policy is nil, it allows any resources.
func validateAppResources(appResources corev1.ResourceRequirements, userPolicy *corev1.ResourceRequirements) (corev1.ResourceRequirements, error) {
	// If there's no policy, the app's resources are inherently valid.
	if userPolicy == nil {
		return appResources, nil
	}

	// Validate Limits
	for resourceName, appLimit := range appResources.Limits {
		if userLimit, ok := userPolicy.Limits[resourceName]; ok {
			// Cmp returns 1 if appLimit > userLimit
			if appLimit.Cmp(userLimit) > 0 {
				return corev1.ResourceRequirements{}, fmt.Errorf(
					"app limit for resource %q (%s) exceeds user policy limit (%s)",
					resourceName, appLimit.String(), userLimit.String(),
				)
			}
		}
	}

	// Validate Requests, I'm not sure if this is needed because we could just limit using... limits.
	for resourceName, appRequest := range appResources.Requests {
		if userRequest, ok := userPolicy.Requests[resourceName]; ok {
			// Cmp returns 1 if appRequest > userRequest
			if appRequest.Cmp(userRequest) > 0 {
				return corev1.ResourceRequirements{}, fmt.Errorf(
					"app request for resource %q (%s) exceeds user policy request (%s)",
					resourceName, appRequest.String(), userRequest.String(),
				)
			}
		}
	}

	// All checks passed. The app's requested resources are valid.
	return appResources, nil
}

// validateVolumeMounts checks if all volume mounts in the provided slice
// correspond to a volume defined in the validVolumes map.
func validateVolumeMounts(mounts []corev1.VolumeMount, validVolumes map[string]struct{}, sidecarName string) error {
	for _, vm := range mounts {
		if _, ok := validVolumes[vm.Name]; !ok {
			return fmt.Errorf("validation failed: volumeMount %q in %s sidecar policy refers to a volume that is not defined in the UserSpec.volumes", vm.Name, sidecarName)
		}
	}
	return nil
}

func (c *SessionController) reconcilePod(ctx context.Context, session *v1alpha1types.Session) error {
	//!TODO: Just allocate a ton of ports on the container, we wont be able to
	// change them while its running if another user connects
	if !meta.IsStatusConditionPresentAndEqual(session.Status.Conditions, "PortsAllocated", metav1.ConditionTrue) {
		return fmt.Errorf("waiting for PortsAllocated")
	}

	// Get the user object to access resource policies
	user, err := c.UserInformer.Namespaced(session.Namespace).Get(session.Spec.UserReference.Name)
	if err != nil {
		return fmt.Errorf("failed to get user %s: %w", session.Spec.UserReference.Name, err)
	}

	// Every session streaming from the pod owns it, so it outlives any one
	// of them, like the first player leaving a co-op game
	owners := c.sessionOwners(session)

	// If deployment already exists, just update its owners. Patch rather
	// than apply: the cache may still hold a deployment that was just
	// garbage collected with its last session, and apply would try to
	// recreate it from the owners alone.
	deploymentName := c.deploymentName(session)
	if _, err := c.deploymentController.Informer().Namespaced(session.Namespace).Get(deploymentName); err == nil {
		klog.Infof("Deployment %s/%s already exists, just updating metadata", session.Namespace, deploymentName)
		patch, err := json.Marshal(map[string]any{
			"metadata": map[string]any{"ownerReferences": owners},
		})
		if err != nil {
			return fmt.Errorf("failed to encode owners patch: %w", err)
		}
		_, err = c.K8sClient.AppsV1().Deployments(session.Namespace).Patch(
			ctx, deploymentName, types.MergePatchType, patch, metav1.PatchOptions{})
		if err == nil {
			return nil
		}
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to update owners of deployment %s: %w", deploymentName, err)
		}
		klog.Infof("Deployment %s/%s is gone, creating it again", session.Namespace, deploymentName)
	}

	coop := c.coopConfig(session)

	// Create pod from pod template
	app, err := c.AppInformer.Namespaced(session.Namespace).Get(session.Spec.GameReference.Name)
	if err != nil {
		return fmt.Errorf("failed to get app: %s", err)
	}
	// Prepare environment variables for the wolf container
	// commenting these out was the main reason nvidia stuff wasn't working.
	// specifically the NVIDIA_VISIBLE_DEVICES
	// I need a better method of injecting env vars / configs to the pod
	// TODO
	wolfEnvVars := map[string]string{
		"PUID":                   "1000",
		"PGID":                   "1000",
		"UNAME":                  "ubuntu",
		"XDG_RUNTIME_DIR":        "/tmp/.X11-unix",
		"PULSE_SERVER":           "unix:/tmp/.X11-unix/pulse-socket",
		"HOST_APPS_STATE_FOLDER": "/mnt/data/wolf",
		// "WOLF_STREAM_CLIENT_IP":  "10.128.1.0", //Need to find the correct streaming id / ingress, later.
		"WOLF_SOCKET_PATH":       "/etc/wolf/wolf.sock", 
		// "WOLF_CFG_FILE":          "/etc/wolf/cfg/config.toml", // no longer needed
		// "WOLF_PRIVATE_CERT_FILE": "/mnt/data/wolf/cfg/cert.pem",
		// "WOLF_PRIVATE_KEY_FILE": "/mnt/data/wolf/cfg/key.pem",
		// "WOLF_PULSE_IMAGE":       "ghcr.io/games-on-whales/pulseaudio:master",
		// "WOLF_CFG_FOLDER":        "/etc/wolf/cfg",
		// Keeping those for later
		// "GST_VAAPI_ALL_DRIVERS":      "1",
		// "GST_DEBUG":                  "2",
		// "__GL_SYNC_TO_VBLANK":        "0",
		// "NVIDIA_VISIBLE_DEVICES":     "all",
		// "NVIDIA_DRIVER_CAPABILITIES": "all",
		// "LIBVA_DRIVER_NAME":          "nvidia",
		// "LD_LIBRARY_PATH":            "/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/lib",
	}

	// Check if runtime variables are defined in the App spec and override defaults
	if app.Spec.WolfConfig.RuntimeVariables != nil {
		runtimeVars := app.Spec.WolfConfig.RuntimeVariables
		if runtimeVars.LogLevel != "" {
			wolfEnvVars["WOLF_LOG_LEVEL"] = runtimeVars.LogLevel
		}
		if runtimeVars.TimeZone != "" {
			wolfEnvVars["TZ"] = runtimeVars.TimeZone
		}
		if runtimeVars.RenderNode != "" {
			wolfEnvVars["WOLF_RENDER_NODE"] = runtimeVars.RenderNode
		}
	}

	// A co-op pod streams to several clients, which each pass their IP when
	// their Wolf session is added
	if session.Spec.Config.ClientIP != "" && coop == nil {
		wolfEnvVars["WOLF_STREAM_CLIENT_IP"] = session.Spec.Config.ClientIP
	}

	// Listen on this session's ports so concurrent streams can share the
	// LoadBalancer IP. Wolf sends the stream ports to Moonlight in its RTSP
	// replies, and Moonlight takes the RTSP port from the launch response.
	wolfEnvVars["WOLF_RTSP_SETUP_PORT"] = fmt.Sprint(session.Status.Ports.RTSP)
	wolfEnvVars["WOLF_CONTROL_PORT"] = fmt.Sprint(session.Status.Ports.Control)
	wolfEnvVars["WOLF_VIDEO_PING_PORT"] = fmt.Sprint(session.Status.Ports.VideoRTP)
	wolfEnvVars["WOLF_AUDIO_PING_PORT"] = fmt.Sprint(session.Status.Ports.AudioRTP)
	var podToCreate corev1.PodTemplateSpec
	if app.Spec.Template != nil {
		podToCreate.ObjectMeta = app.Spec.Template.ObjectMeta
		podToCreate.Spec = *app.Spec.Template.Spec.DeepCopy()
	}

	if podToCreate.Labels == nil {
		podToCreate.Labels = map[string]string{}
	}

	podToCreate.Labels["app"] = "direwolf-worker"
	maps.Copy(podToCreate.Labels, c.podLabels(session))

	// The app draws into the session's own display, or for co-op the
	// lobby's, which the lobby runner links to a fixed name
	waylandDisplay := "wayland-1"
	if coop != nil {
		waylandDisplay = coopWaylandDisplay
	}

	// if podToCreate.Spec.SecurityContext == nil {
	// 	podToCreate.Spec.SecurityContext = &corev1.PodSecurityContext{}
	// }

	// if podToCreate.Spec.SecurityContext.SeccompProfile == nil {
	// 	podToCreate.Spec.SecurityContext.SeccompProfile = &corev1.SeccompProfile{
	// 		Type: corev1.SeccompProfileTypeUnconfined,
	// 	}
	// }

	// if podToCreate.Spec.SecurityContext.AppArmorProfile == nil {
	// 	podToCreate.Spec.SecurityContext.AppArmorProfile = &corev1.AppArmorProfile{
	// 		Type: corev1.AppArmorProfileTypeUnconfined,
	// 	}
	// }

	wolfEnvVarsSlice := make([]corev1.EnvVar, 0, len(wolfEnvVars))
	for k, v := range wolfEnvVars {
		wolfEnvVarsSlice = append(wolfEnvVarsSlice, corev1.EnvVar{Name: k, Value: v})
 	}


	// Inject volume mounts into existing containers
	for i := range podToCreate.Spec.Containers {
		podToCreate.Spec.Containers[i].VolumeMounts = append(podToCreate.Spec.Containers[i].VolumeMounts,
			corev1.VolumeMount{
				Name:      "wolf-runtime",
				MountPath: "/tmp/.X11-unix",
			},
			corev1.VolumeMount{
				Name:      "wolf-data",
				MountPath: "/home/retro",
				SubPath:   fmt.Sprintf("state/%s", app.Name),
			},
		)

		podToCreate.Spec.Containers[i].Env = append(podToCreate.Spec.Containers[i].Env, []corev1.EnvVar{
			// Standard GOW envars
			{Name: "DISPLAY", Value: ":0"},
			// Container must have extra logic to wait for this to be set up
			// unfortunately.
			{Name: "WAYLAND_DISPLAY", Value: waylandDisplay},
			{Name: "TZ", Value: wolfEnvVars["TZ"]},
			{Name: "UNAME", Value: "retro"},
			{Name: "XDG_RUNTIME_DIR", Value: "/tmp/.X11-unix"},			// "UID":             "1000",
			// "GID":             "1000",
			{Name: "PULSE_SERVER", Value: "unix:/tmp/.X11-unix/pulse-socket"},
			// PULSE_SINK & PULSE_SOURCE set at runtime calculated based off session ID.
			// But would be nice if unnecessary

			// Assorted NVIDIA. Unsure if required. Probabky not.
			// just gonna uncomment to make sure that this is not the reason firefox keeps crashing and failing to play videos.
			// yeah now no audio, probably because i'm developing on an integrated amd gpu.
			{Name: "LIBVA_DRIVER_NAME", Value: "nvidia"},
			{Name: "LD_LIBRARY_PATH", Value: "/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/lib"},
			{Name: "NVIDIA_DRIVER_CAPABILITIES", Value: "all"},
			{Name: "NVIDIA_VISIBLE_DEVICES", Value: "all"},
			{Name: "GST_VAAPI_ALL_DRIVERS", Value: "1"},
			{Name: "GST_DEBUG", Value: "2"},

			// Gamescape envar injection. Ham-handed. Why not.
			{Name: "GAMESCOPE_WIDTH", Value: fmt.Sprint(session.Spec.Config.VideoWidth)},
			{Name: "GAMESCOPE_HEIGHT", Value: fmt.Sprint(session.Spec.Config.VideoHeight)},
			{Name: "GAMESCOPE_REFRESH", Value: fmt.Sprint(session.Spec.Config.VideoRefreshRate)},

			// Who launched the app. Co-op pods are shared, so their user
			// label is empty, and this is the user who started the pod.
			// Comes after the app's own env so it wins over an older
			// fieldRef to that label.
			{Name: "DIREWOLF_USER", Value: session.Spec.UserReference.Name},
			{Name: "DIREWOLF_APP", Value: session.Spec.GameReference.Name},
		}...)

		// Validate the main app container's resources against the user's policy.
		validatedResources, err := validateAppResources(podToCreate.Spec.Containers[i].Resources, user.Spec.Resources)
		if err != nil {
			// The error will be handled by the main Reconcile loop to update the session status.
			return fmt.Errorf("resource validation for main app container failed: %w", err)
		}
		podToCreate.Spec.Containers[i].Resources = validatedResources
	}

	podToCreate.Spec.InitContainers = append(podToCreate.Spec.InitContainers,
		corev1.Container{
			Name:  "init",
			Image: "ghcr.io/games-on-whales/base:edge",
			// This will need to be updated / removed since /etc/wolf/cfg is no longer used by wolf
			// Also, we're no longer injecting the app info into the config.toml
			Command: []string{
				"sh", "-c", `
				mkdir -p /mnt/data/wolf/cfg
				cp /certs/* /mnt/data/wolf/cfg/
				# Wolf's entrypoint puts its config under HOST_APPS_STATE_FOLDER.
				# On a persistent volume, a config from an earlier pod is kept.
				if [ ! -f /mnt/data/wolf/cfg/config.toml ]; then
					cat > /mnt/data/wolf/cfg/config.toml <<'DIREWOLF_EOF'
`+wolfSeedConfig(string(uuid.NewUUID()))+`DIREWOLF_EOF
				fi
				chown 1000:1000 /mnt/data/wolf
				chmod 777 /mnt/data/wolf
				chown -R 1000:1000 /mnt/data/wolf/cfg
				chmod 777 /mnt/data/wolf/cfg
				chown -R ubuntu:ubuntu /tmp/.X11-unix
				chmod 1777 -R /tmp/.X11-unix
				mkdir -p /etc/wolf/cfg
				# cp -LR /cfg/* /etc/wolf/cfg
				chown -R ubuntu:ubuntu /etc/wolf
				chmod 777 -R /etc/wolf
			`,
			},
			VolumeMounts: []corev1.VolumeMount{
				// {
				// 	Name:      "wolf-tls-secret",
				// 	MountPath: "/certs",
				// 	ReadOnly:  true,
				// },
				{
					Name:      "wolf-cfg",
					MountPath: "/etc/wolf",
				},
				{
					Name:      "wolf-data",
					MountPath: "/mnt/data/wolf",
				},
				{
					Name:      "wolf-runtime",
					MountPath: "/tmp/.X11-unix",
				},
				// {
				// 	Name:      "config",
				// 	MountPath: "/cfg",
				// },
			},
		},
	)

	// Define default resources for sidecars
	wolfAgentDefaultResources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("100Mi"),
		},
	}
	pulseAudioDefaultResources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("100Mi"),
		},
	}
	wolfDefaultResources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("100Mi"),
		},
	}

	// Prepare sidecar policies
	var wolfAgentEnv, pulseAudioEnv, wolfEnv []corev1.EnvVar
	var wolfAgentResources, pulseAudioResources, wolfResources corev1.ResourceRequirements
	var wolfAgentVolumeMounts, pulseAudioVolumeMounts, wolfVolumeMounts []corev1.VolumeMount
	var wolfAgentSecurityContext, pulseAudioSecurityContext, wolfSecurityContext *corev1.SecurityContext
	var podHostIPC bool // Variable to track if HostIPC should be enabled for the pod

	// Set defaults first
	wolfAgentResources = wolfAgentDefaultResources
	pulseAudioResources = pulseAudioDefaultResources
	wolfResources = wolfDefaultResources

	// Create a set of valid volume names for quick lookup
	validVolumes := make(map[string]struct{})
	for _, volume := range user.Spec.Volumes {
		validVolumes[volume.Name] = struct{}{}
	}

	if user.Spec.SidecarPolicies != nil {
		policies := user.Spec.SidecarPolicies
		if policies.WolfAgent != nil {
			if err := validateVolumeMounts(policies.WolfAgent.VolumeMounts, validVolumes, "wolfAgent"); err != nil {
				return err
			}
			wolfAgentEnv = policies.WolfAgent.Env
			// klog.debug("User defined env vars: %+v", wolfAgentEnv)
			wolfAgentResources = mergeResourceRequirements(wolfAgentDefaultResources, policies.WolfAgent.Resources)
			wolfAgentVolumeMounts = policies.WolfAgent.VolumeMounts
			wolfAgentSecurityContext = policies.WolfAgent.SecurityContext
			if policies.WolfAgent.HostIPC != nil && *policies.WolfAgent.HostIPC {
				podHostIPC = true
			}
		}
		if policies.PulseAudio != nil {
			if err := validateVolumeMounts(policies.PulseAudio.VolumeMounts, validVolumes, "pulseAudio"); err != nil {
				return err
			}
			pulseAudioEnv = policies.PulseAudio.Env
			// klog.Infof("User defined env vars: %+v", pulseAudioEnv)
			pulseAudioResources = mergeResourceRequirements(pulseAudioDefaultResources, policies.PulseAudio.Resources)
			pulseAudioVolumeMounts = policies.PulseAudio.VolumeMounts
			pulseAudioSecurityContext = policies.PulseAudio.SecurityContext
			if policies.PulseAudio.HostIPC != nil && *policies.PulseAudio.HostIPC {
				podHostIPC = true
			}
		}
		if policies.Wolf != nil {
			if err := validateVolumeMounts(policies.Wolf.VolumeMounts, validVolumes, "wolf"); err != nil {
				return err
			}
			wolfEnv = policies.Wolf.Env
			// klog.Infof("User defined env vars: %+v", wolfEnv)
			wolfResources = mergeResourceRequirements(wolfDefaultResources, policies.Wolf.Resources)
			wolfVolumeMounts = policies.Wolf.VolumeMounts
			wolfSecurityContext = policies.Wolf.SecurityContext
			if policies.Wolf.HostIPC != nil && *policies.Wolf.HostIPC {
				podHostIPC = true
			}
		}
	}

	// Apply HostIPC setting to the pod spec if requested by any sidecar policy
	podToCreate.Spec.HostIPC = podHostIPC

	podToCreate.Spec.Containers = append(podToCreate.Spec.Containers,
		corev1.Container{
			Name:  "wolf-agent",
			Image: c.WolfAgentImage,
			ImagePullPolicy: corev1.PullAlways,
			// ImagePullPolicy: corev1.PullIfNotPresent,
			Args: []string{
				"--socket=/etc/wolf/wolf.sock",
				"--port=8443",
			},
			Ports: []corev1.ContainerPort{
				{
					Name:          "wa",
					ContainerPort: 8443,
				},
			},
			Env: append([]corev1.EnvVar{
					{
						Name:  "XDG_RUNTIME_DIR",
						Value: "/tmp/.X11-unix",
					},
					// {
					// 	Name:  "PUID",
					// 	Value: "1000",
					// },
					// {
					// 	Name:  "PGID",
					// 	Value: "1000",
					// },
					{
						Name:  "WOLF_SOCKET_PATH",
						Value: "/etc/wolf/wolf.sock",
					},
					{
						Name:  "DIREWOLF_USER",
						Value: session.Spec.UserReference.Name,
					},
					{
						Name:  "DIREWOLF_APP",
						Value: session.Spec.GameReference.Name,
					},
					{
						Name: "POD_NAME",
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.name",
							},
						},
					},
					{
						Name: "POD_NAMESPACE",
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.namespace",
							},
						},
					},
				},wolfAgentEnv...
			),
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path:   "/readyz",
						Port:   intstr.FromInt(8443),
						Scheme: corev1.URISchemeHTTPS,
					},
				},
			},
			LivenessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path:   "/livez",
						Port:   intstr.FromInt(8443),
						Scheme: corev1.URISchemeHTTPS,
					},
				},
			},
			Resources:       wolfAgentResources,
			SecurityContext: wolfAgentSecurityContext,
			VolumeMounts: append([]corev1.VolumeMount{
				{
					Name:      "wolf-cfg",
					MountPath: "/etc/wolf",
				},
				{
					Name:      "wolf-runtime",
					MountPath: "/tmp/.X11-unix",
				},
			}, wolfAgentVolumeMounts...),
		},
		corev1.Container{
			Name:  "pulseaudio",
			Image: "ghcr.io/games-on-whales/pulseaudio:edge",
			Env: append([]corev1.EnvVar{
				{Name: "TZ", Value: wolfEnvVars["TZ"]},
				{Name: "UNAME", Value: "retro"},
				{Name: "XDG_RUNTIME_DIR", Value: "/tmp/pulse"},
				// "UID":             "1000",
				// "GID":             "1000",
			}, pulseAudioEnv...),

			Resources:       pulseAudioResources,
			SecurityContext: pulseAudioSecurityContext,
			Lifecycle:       outliveAppOnStop,
			VolumeMounts: append([]corev1.VolumeMount{
				{
					Name:      "wolf-runtime",
					MountPath: "/tmp/pulse",
				},
			}, pulseAudioVolumeMounts...),
		},
		corev1.Container{
			Name:      "wolf",
			Image:     WOLF_IMAGE,
			Env:       append(wolfEnvVarsSlice, wolfEnv...),
			Lifecycle: outliveAppOnStop,
			// Note: Container Ports list is strictly informational. As long
			// as process is listening on 0.0.0.0 it can be bound by a service.
			Ports: []corev1.ContainerPort{
				{
					Name:          "http",
					ContainerPort: 48989,
				},
				{
					Name:          "https",
					ContainerPort: 48984,
				},
				{
					Name:          "rtsp",
					ContainerPort: session.Status.Ports.RTSP,
				},
				{
					Name:          "enet",
					ContainerPort: session.Status.Ports.Control,
				},
				{
					Name:          "video",
					ContainerPort: session.Status.Ports.VideoRTP,
				},
				{
					Name:          "audio",
					ContainerPort: session.Status.Ports.AudioRTP,
				},
			},
			Resources:       wolfResources,
			SecurityContext: wolfSecurityContext,
			VolumeMounts: append([]corev1.VolumeMount{
				{
					Name:      "wolf-cfg",
					MountPath: "/etc/wolf",
				},
				{
					Name:      "wolf-runtime",
					MountPath: "/tmp/.X11-unix",
				},
				{
					Name:      "wolf-data",
					MountPath: "/mnt/data/wolf",
				},
				// {
				// 	Name:      "dev-input",
				// 	MountPath: "/dev/input",
				// },
				// {
				// 	Name:      "dev-uinput",
				// 	MountPath: "/dev/uinput",
				// },
				// {
				// 	Name:      "host-udev",
				// 	MountPath: "/run/udev", //Need to find a more secure way to mount this
				// },
			}, wolfVolumeMounts...),
		},
	)

	var wolfDataVolumeSource corev1.VolumeSource
	if app.Spec.VolumeClaimTemplate != nil {
		wolfDataVolumeSource = corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: c.deploymentName(session),
			},
		}
	} else {
		wolfDataVolumeSource = corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		}
	}

	podToCreate.Spec.Volumes = append(podToCreate.Spec.Volumes,
		// corev1.Volume{
		// 	Name: "config",
		// 	VolumeSource: corev1.VolumeSource{
		// 		ConfigMap: &corev1.ConfigMapVolumeSource{
		// 			LocalObjectReference: corev1.LocalObjectReference{
		// 				Name: c.deploymentName(session),
		// 			},
		// 		},
		// 	},
		// },
		// corev1.Volume{
		// 	Name: "wolf-tls-secret",
		// 	VolumeSource: corev1.VolumeSource{
		// 		Secret: &corev1.SecretVolumeSource{
		// 			SecretName: "wolf-tls-secret",
		// 			Optional:   ptr.To(true), // Optional so it doesn't crash if you forgot to create it
		// 		},
		// 	},
		// },
		corev1.Volume{
			Name: "wolf-cfg",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		corev1.Volume{
			Name: "wolf-runtime",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		corev1.Volume{
			Name:         "wolf-data",
			VolumeSource: wolfDataVolumeSource,
		},
		// corev1.Volume{ //Needs to be changed into something more secure, without host path
		// 	Name: "dev-input",
		// 	VolumeSource: corev1.VolumeSource{
		// 		HostPath: &corev1.HostPathVolumeSource{
		// 			Path: "/dev/input",
		// 			Type: ptr.To(corev1.HostPathDirectory),
		// 		},
		// 	},
		// },
		// I'm moving this to volumeConfig
		// corev1.Volume{
		// 	Name: "dev-uinput",
		// 	VolumeSource: corev1.VolumeSource{
		// 		HostPath: &corev1.HostPathVolumeSource{
		// 			Path: "/dev/uinput",
		// 			Type: ptr.To(corev1.HostPathFile),
		// 		},
		// 	},
		// },
		// corev1.Volume{ //Needs to be changed into something more secure, without host path
		// 	Name: "host-udev",
		// 	VolumeSource: corev1.VolumeSource{
		// 		HostPath: &corev1.HostPathVolumeSource{
		// 			Path: "/run/udev",
		// 			Type: ptr.To(corev1.HostPathDirectory),
		// 		},
		// 	},
		// },
	)

	// Add volumes from the user spec
	if len(user.Spec.Volumes) > 0 {
		podToCreate.Spec.Volumes = append(podToCreate.Spec.Volumes, user.Spec.Volumes...)
	}

	// Create deployment scaled to 1 for this pod
	// Should use deployment so that changes in spec aren't rejected.
	deployment := appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.deploymentName(session),
			Namespace: session.Namespace,
			Labels: map[string]string{
				"app":           "direwolf-worker",
				"direwolf/app":  c.podKey(session).Game,
				"direwolf/user": c.podKey(session).User,
			},
			OwnerReferences: owners,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](1),
			Selector: &metav1.LabelSelector{
				MatchLabels: c.podLabels(session),
			},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			RevisionHistoryLimit:    ptr.To[int32](1),
			ProgressDeadlineSeconds: ptr.To[int32](10),
			Template:                podToCreate,
		},
	}

	unstructuredDeployment, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&deployment)
	if err != nil {
		return fmt.Errorf("failed to convert deployment to unstructured: %s", err)
	}

	// NOTE: Kinda dumb cuz its just gona get serialized again....
	// could just use dynamic client
	var deploymentApplyConfig appsv1ac.DeploymentApplyConfiguration
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredDeployment, &deploymentApplyConfig)
	if err != nil {
		return fmt.Errorf("failed to convert unstructured to deployment: %s", err)
	}

	_, err = c.K8sClient.AppsV1().Deployments(session.Namespace).Apply(
		ctx,
		&deploymentApplyConfig,
		metav1.ApplyOptions{
			FieldManager: "direwolf-session-controller-deployment",
		})

	if err != nil {
		return fmt.Errorf("failed to apply deployment: %s", err)
	}

	return nil
}

// func (c *SessionController) reconcileConfigMap(
// 	ctx context.Context,
// 	session *v1alpha1types.Session,
// ) error {
// 	app, err := c.AppInformer.Namespaced(session.Namespace).Get(session.Spec.GameReference.Name)
// 	if err != nil {
// 		return fmt.Errorf("failed to get app: %s", err)
// 	}

// 	user, err := c.UserInformer.Namespaced(session.Namespace).Get(session.Spec.UserReference.Name)
// 	if err != nil {
// 		return fmt.Errorf("failed to get user: %s", err)
// 	}

// 	wolfConfig, err := GenerateWolfConfig(app)
// 	if err != nil {
// 		return fmt.Errorf("failed to generate wolf config: %s", err)
// 	}
// 	deploymentName := c.deploymentName(session)

// 	_, err = c.K8sClient.CoreV1().
// 		ConfigMaps(session.Namespace).
// 		Apply(
// 			context.Background(),
// 			v1ac.ConfigMap(deploymentName, session.Namespace).
// 				WithLabels(
// 					map[string]string{
// 						"app":           "direwolf-worker",
// 						"direwolf/app":  session.Spec.GameReference.Name,
// 						"direwolf/user": session.Spec.UserReference.Name,
// 					}).
// 				WithOwnerReferences(
// 					metav1ac.OwnerReference().
// 						WithName(app.Name).
// 						WithAPIVersion(v1alpha1.GroupVersion.String()).
// 						WithKind("App").
// 						WithUID(app.UID).
// 						WithController(true),
// 					metav1ac.OwnerReference().
// 						WithName(user.Name).
// 						WithAPIVersion(v1alpha1.GroupVersion.String()).
// 						WithKind("User").
// 						WithUID(user.UID),
// 				).
// 				WithData(map[string]string{
// 					"config.toml": wolfConfig,
// 				}),
// 			metav1.ApplyOptions{
// 				FieldManager: "direwolf-session-controller",
// 			})
// 	if err != nil {
// 		return fmt.Errorf("failed to apply configmap: %s", err)
// 	}
// 	return nil
// }

func (c *SessionController) reconcilePVC(ctx context.Context, session *v1alpha1types.Session) error {
	user, err := c.UserInformer.Namespaced(session.Namespace).Get(session.Spec.UserReference.Name)
	if err != nil {
		return fmt.Errorf("failed to get user: %s", err)
	}
	app, err := c.AppInformer.Namespaced(session.Namespace).Get(session.Spec.GameReference.Name)
	if err != nil {
		return fmt.Errorf("failed to get user: %s", err)
	}

	// Check if the user defined a volume claim template. If not, return nil.
	if app.Spec.VolumeClaimTemplate == nil {
		klog.Infof("App %s does not define a VolumeClaimTemplate, skipping PVC creation.", app.Name)
		return nil
	}

	pvcName := c.deploymentName(session)
	templateSpec := app.Spec.VolumeClaimTemplate.Spec.DeepCopy()

	// Default Access Mode: RWO
	if len(templateSpec.AccessModes) == 0 {
		templateSpec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}

	// Default Storage: 5Gi
	if templateSpec.Resources.Requests == nil {
		templateSpec.Resources.Requests = make(corev1.ResourceList)
	}
	if _, ok := templateSpec.Resources.Requests[corev1.ResourceStorage]; !ok {
		templateSpec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("5Gi")
	}
	// Note: Default storage class is handled by Kubernetes if StorageClassName is nil.

	// Build the PVC spec apply configuration from the defaulted spec
	pvcSpec := v1ac.PersistentVolumeClaimSpec().
		WithAccessModes(templateSpec.AccessModes...).
		WithResources(v1ac.VolumeResourceRequirements().
			WithLimits(templateSpec.Resources.Limits).
			WithRequests(templateSpec.Resources.Requests))

	if templateSpec.Selector != nil {
		selectorConfig := metav1ac.LabelSelector()
		if len(templateSpec.Selector.MatchLabels) > 0 {
			selectorConfig.WithMatchLabels(templateSpec.Selector.MatchLabels)
		}
		if len(templateSpec.Selector.MatchExpressions) > 0 {
			var expressions []*metav1ac.LabelSelectorRequirementApplyConfiguration
			for _, req := range templateSpec.Selector.MatchExpressions {
				expressions = append(expressions, metav1ac.LabelSelectorRequirement().
					WithKey(req.Key).
					WithOperator(req.Operator).
					WithValues(req.Values...))
			}
			selectorConfig.WithMatchExpressions(expressions...)
		}
		pvcSpec.WithSelector(selectorConfig)
	}
	if templateSpec.StorageClassName != nil {
		pvcSpec.WithStorageClassName(*templateSpec.StorageClassName)
	}
	if templateSpec.VolumeMode != nil {
		pvcSpec.WithVolumeMode(*templateSpec.VolumeMode)
	}
	if templateSpec.DataSource != nil {
		dsConfig := v1ac.TypedLocalObjectReference().
			WithKind(templateSpec.DataSource.Kind).
			WithName(templateSpec.DataSource.Name)
		if templateSpec.DataSource.APIGroup != nil {
			dsConfig.WithAPIGroup(*templateSpec.DataSource.APIGroup)
		}
		pvcSpec.WithDataSource(dsConfig)
	}
	if templateSpec.DataSourceRef != nil {
		dsrConfig := v1ac.TypedObjectReference().
			WithKind(templateSpec.DataSourceRef.Kind).
			WithName(templateSpec.DataSourceRef.Name)
		if templateSpec.DataSourceRef.APIGroup != nil {
			dsrConfig.WithAPIGroup(*templateSpec.DataSourceRef.APIGroup)
		}
		if templateSpec.DataSourceRef.Namespace != nil {
			dsrConfig.WithNamespace(*templateSpec.DataSourceRef.Namespace)
		}
		pvcSpec.WithDataSourceRef(dsrConfig)
	}

	_, err = c.K8sClient.CoreV1().PersistentVolumeClaims(session.Namespace).Apply(
		ctx,
		v1ac.PersistentVolumeClaim(pvcName, session.Namespace).
			WithLabels(map[string]string{
				"app":           "direwolf-worker",
				"direwolf/app":  session.Spec.GameReference.Name,
				"direwolf/user": session.Spec.UserReference.Name,
			}).
			WithOwnerReferences(metav1ac.OwnerReference().
				WithName(user.Name).
				WithAPIVersion(v1alpha1.GroupVersion.String()).
				WithKind("User").
				WithUID(user.UID).
				WithController(true)).
			WithSpec(pvcSpec),
		metav1.ApplyOptions{
			FieldManager: "direwolf-session-controller-pvc",
		},
	)

	if err != nil {
		return fmt.Errorf("failed to apply PVC %s: %w", pvcName, err)
	}

	return nil
}
func (c *SessionController) deploymentName(session *v1alpha1types.Session) string {
	key := c.podKey(session)
	if key.User == "" {
		return "coop-" + key.Game
	}
	return fmt.Sprintf("%s-%s", key.User, key.Game)
}

// coopConfig returns the co-op settings of the session's app, or nil if the
// app isn't co-op.
func (c *SessionController) coopConfig(session *v1alpha1types.Session) *v1alpha1types.CoopConfig {
	if c.AppInformer == nil {
		return nil
	}
	app, err := c.AppInformer.Namespaced(session.Namespace).Get(session.Spec.GameReference.Name)
	if err != nil {
		return nil
	}
	return app.Spec.Coop
}

// podKey identifies the pod a session streams from. Each user gets their own
// pod for an app, except for co-op apps where everyone shares one, keyed with
// an empty user.
func (c *SessionController) podKey(session *v1alpha1types.Session) userGame {
	key := userGame{
		Game: session.Spec.GameReference.Name,
		User: session.Spec.UserReference.Name,
	}
	if c.coopConfig(session) != nil {
		key.User = ""
	}
	return key
}

// podLabels selects the pod a session streams from, and labels the objects
// made for it. Co-op pods have an empty user label.
func (c *SessionController) podLabels(session *v1alpha1types.Session) map[string]string {
	key := c.podKey(session)
	return map[string]string{
		"direwolf/app":  key.Game,
		"direwolf/user": key.User,
	}
}

// sessionOwners lists the sessions streaming from the session's pod, as owner
// references for the objects they share. None of them is the controller,
// since an object may only have one.
func (c *SessionController) sessionOwners(session *v1alpha1types.Session) []metav1.OwnerReference {
	var owners []metav1.OwnerReference
	for name := range c.trackedSessions[c.podKey(session)] {
		sess, err := c.SessionInformer.Namespaced(session.Namespace).Get(name)
		if err != nil {
			klog.Errorf("Failed to get session %s/%s: %s", session.Namespace, name, err)
			continue
		}
		owners = append(owners, metav1.OwnerReference{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "Session",
			Name:       name,
			UID:        sess.UID,
		})
	}
	slices.SortFunc(owners, func(a, b metav1.OwnerReference) int {
		return strings.Compare(a.Name, b.Name)
	})
	return owners
}

func ownerApplyConfigs(owners []metav1.OwnerReference) []*metav1ac.OwnerReferenceApplyConfiguration {
	res := make([]*metav1ac.OwnerReferenceApplyConfiguration, 0, len(owners))
	for _, owner := range owners {
		res = append(res, metav1ac.OwnerReference().
			WithName(owner.Name).
			WithAPIVersion(owner.APIVersion).
			WithKind(owner.Kind).
			WithUID(owner.UID))
	}
	return res
}

func (c *SessionController) allocatePorts(
	ctx context.Context,
	session *v1alpha1types.Session,
) error {
	c.portsMu.Lock()
	defer c.portsMu.Unlock()

	ug := c.podKey(session)

	// Every session of a pod shares its ports: those of a user's game, or of
	// a co-op game. Keep ports already assigned, then a slot already reserved
	// for this pod, then one held by another of its sessions (e.g. after an
	// operator restart).
	if session.Status.Ports.RTSP != 0 {
		if slot, ok := portSlotOf(session.Status.Ports); ok {
			c.portSlots[ug] = slot
		}
		return nil
	}

	slot, ok := c.portSlots[ug]
	if !ok {
		sessions, err := c.SessionInformer.Namespaced(session.Namespace).List(labels.Everything())
		if err != nil {
			return fmt.Errorf("failed to list sessions: %w", err)
		}

		used := sets.New[int32]()
		for other, s := range c.portSlots {
			if other != ug {
				used.Insert(s)
			}
		}
		for _, other := range sessions {
			otherSlot, hasSlot := portSlotOf(other.Status.Ports)
			if !hasSlot {
				continue
			}
			if c.podKey(other) == ug {
				slot, ok = otherSlot, true
				break
			}
			used.Insert(otherSlot)
		}

		for candidate := int32(0); !ok && candidate < maxPortSlots; candidate++ {
			if !used.Has(candidate) {
				slot, ok = candidate, true
			}
		}
		if !ok {
			return fmt.Errorf("all %d stream port slots are in use", maxPortSlots)
		}
		c.portSlots[ug] = slot
	}

	session.Status.Ports = portsForSlot(slot)
	return nil
}

// releasePorts frees the port slot of a pod once it has no sessions
func (c *SessionController) releasePorts(ug userGame) {
	c.portsMu.Lock()
	defer c.portsMu.Unlock()
	delete(c.portSlots, ug)
}

const (
	// Concurrent streams each get their own block of ports, this far apart,
	// on the LoadBalancer IP the sessions share
	portSlotStride = 1000
	maxPortSlots   = 16
)

// Slot 0 is Wolf's default ports
var basePorts = v1alpha1types.SessionPorts{
	RTSP:     48010,
	Control:  47999,
	VideoRTP: 48100,
	AudioRTP: 48200,
}

func portsForSlot(slot int32) v1alpha1types.SessionPorts {
	offset := slot * portSlotStride
	return v1alpha1types.SessionPorts{
		RTSP:     basePorts.RTSP + offset,
		Control:  basePorts.Control + offset,
		VideoRTP: basePorts.VideoRTP + offset,
		AudioRTP: basePorts.AudioRTP + offset,
	}
}

// When a session's pod stops, Kubernetes signals all its containers at once.
// Apps like RetroArch save their settings as they quit, which fails if Wolf's
// display and the audio server are already gone, so those wait this long
// before being signalled. Well within the default 30s grace period.
const sidecarStopDelaySeconds = 10

var outliveAppOnStop = &corev1.Lifecycle{
	PreStop: &corev1.LifecycleHandler{
		Sleep: &corev1.SleepAction{Seconds: sidecarStopDelaySeconds},
	},
}

// Port wolf-agent listens on inside the session pod
const wolfAgentPort = 8443

// wolfAgentServicePort is wolf-agent's port on the session's Service. It
// differs per slot like the stream ports: MetalLB and Cilium only share the
// LoadBalancer IP between Services whose ports don't overlap.
func wolfAgentServicePort(ports v1alpha1types.SessionPorts) int32 {
	slot, ok := portSlotOf(ports)
	if !ok {
		return wolfAgentPort
	}
	return wolfAgentPort + slot
}

// portSlotOf returns the slot ports were allocated from, if they were
func portSlotOf(ports v1alpha1types.SessionPorts) (int32, bool) {
	if ports.RTSP < basePorts.RTSP || (ports.RTSP-basePorts.RTSP)%portSlotStride != 0 {
		return 0, false
	}
	slot := (ports.RTSP - basePorts.RTSP) / portSlotStride
	return slot, ports == portsForSlot(slot)
}

// reconcileActiveStreams calls out to wolf-agent on the running pod to ensure
// that wolf is configured in the correct state and listening for streams on the
// correct ports for each session trying to connect to the Pod.
func (c *SessionController) reconcileActiveStreams(
	ctx context.Context,
	session *v1alpha1types.Session,
) error {
	deploymentName := c.deploymentName(session)

	// !TODO: Use informer for cache reads instead?
	deployment, err := c.K8sClient.AppsV1().Deployments(session.Namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get deployment: %s", err)
	}

	// A deployment that was just created reports 0/0 replicas, which isn't
	// ready: wolf-agent's service has no endpoints yet
	if deployment.Status.ObservedGeneration != deployment.Generation ||
		deployment.Status.ReadyReplicas < ptr.Deref(deployment.Spec.Replicas, 1) ||
		deployment.Status.ReadyReplicas != deployment.Status.Replicas {
		return fmt.Errorf("deployment %s/%s not ready (Observed %d, Latest %d) (%d/%d)", session.Namespace, deploymentName, deployment.Status.ObservedGeneration, deployment.Generation, deployment.Status.ReadyReplicas, deployment.Status.Replicas)
	}

	// Get service for the deployment
	service, err := c.K8sClient.CoreV1().Services(session.Namespace).Get(ctx, session.Status.ServiceName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get service: %s", err)
	}

	// List all the "sessions".
	// Ensure they match each of our k8s sessions. Hash on AESKey/IV
	// In the future it might make sense to just match on ClientID/ClientCertFingerprint
	// but that is hardcoded for now :)
	//
	// Calls time out rather than holding up the session's reconcile: the
	// workqueue doesn't rerun it in the meantime, and Moonlight gives up on
	// the launch after 25s. Creating a lobby takes Wolf up to 20s.
	wolfclient := wolfapi.NewClient(fmt.Sprintf("https://%s:%d", service.Spec.ClusterIP, wolfAgentServicePort(session.Status.Ports)), &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext:     (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	})
	sessions, err := wolfclient.ListSessions(ctx)
	if err != nil {
		return fmt.Errorf("failed to list sessions: %s", err)
	}

	keyIVHash := util.Hash([]byte(session.Spec.Config.AESKey), []byte(session.Spec.Config.AESIV))
	var found bool
	for _, s := range sessions {
		sHash := util.Hash([]byte(s.AESKey), []byte(s.AESIV))
		if bytes.Equal(sHash, keyIVHash) {
			found = true
			break
		}
	}

	if found != (session.Status.WolfSessionID != "") {
		klog.Infof("Session %s/%s found: %v, status: %v", session.Namespace, session.Name, found, session.Status.WolfSessionID)
		// Either the session was already added but not in the list, or
		// the session was already in the list without being added.
		//
		// Either scenario is invalid. Delete the session
		return c.SessionClient.Delete(ctx, session.Name, metav1.DeleteOptions{})
	}
	// Will need this for later
	app, err := c.AppInformer.Namespaced(session.Namespace).Get(session.Spec.GameReference.Name)
	if err != nil {
		return fmt.Errorf("failed to get app: %s", err)
	}

	if !found && app != nil {
		// apps, err := wolfclient.ListApps(ctx)
		// if err != nil {
		// 	return fmt.Errorf("failed to list apps from wolf: %w", err)
		// }

		// Determine the title we expect the app to have
		// expectedTitle := app.Spec.Title
		// if app.Spec.WolfConfig.Title != "" {
		// 	expectedTitle = app.Spec.WolfConfig.Title
		// }

		// var appID string

		// 1. Check if the app already exists
		// for _, loadedApp := range apps {
		// 	if loadedApp.Title == expectedTitle {
		// 		appID = loadedApp.ID
		// 		klog.Infof("App '%s' already exists in Wolf with ID: %s. Skipping creation.", expectedTitle, appID)
		// 		break
		// 	}
		// }

		// 2. If app doesn't exist, create it
		// if appID == "" {
		// 	klog.Infof("App '%s' not found in Wolf. Creating it...", expectedTitle)

		// 	if len(apps) == 0 {
		// 		return fmt.Errorf("no apps found in wolf container to use as template")
		// 	}

		// 	// get the GST pipelines from the first app (supposedly wolf-ui)
		// 	templateApp := apps[0]
		// 	klog.Infof("Using app[0] (%s) as template for pipelines", templateApp.Title)

		// 	// Defaults for booleans if nil
		// 	startAudio := true
		// 	if app.Spec.WolfConfig.StartAudioServer != nil {
		// 		startAudio = *app.Spec.WolfConfig.StartAudioServer
		// 	}
		// 	startCompositor := true
		// 	if app.Spec.WolfConfig.StartVirtualCompositor != nil {
		// 		startCompositor = *app.Spec.WolfConfig.StartVirtualCompositor
		// 	}

		// 	runner := wolfapi.Runner{
		// 		Type:   "process",
		// 		RunCmd: "sh -c \"while :; do echo 'running...'; sleep 10; done\"",
		// 	}
		// 	if app.Spec.WolfConfig.Runner != nil {
		// 		runner.Type = app.Spec.WolfConfig.Runner.Type
		// 		runner.RunCmd = app.Spec.WolfConfig.Runner.RunCommand
		// 	}
		// 	var renderNode string
		// 	if app.Spec.WolfConfig.RuntimeVariables != nil {
		// 		renderNode = app.Spec.WolfConfig.RuntimeVariables.RenderNode
		// 	}

		// 	// Create the App in Wolf
		// 	newApp := wolfapi.App{
		// 		ID:                     app.Name, // Use K8s App Name as ID
		// 		Title:                  expectedTitle,
		// 		SupportHDR:             false, // Default
		// 		StartVirtualCompositor: startCompositor,
		// 		StartAudioServer:       startAudio,
		// 		RenderNode:             renderNode,
		// 		Runner:                 runner,

		// 		// Injected Pipelines
		// 		H264GSTPipeline: templateApp.H264GSTPipeline,
		// 		HEVCGSTPipeline: templateApp.HEVCGSTPipeline,
		// 		AV1GSTPipeline:  templateApp.AV1GSTPipeline,
		// 		OpusGSTPipeline: templateApp.OpusGSTPipeline,
		// 	}
		// 	// no need to create an app
		// 	// klog.Infof("Creating App in Wolf: %+v", newApp)
		// 	// if err := wolfclient.AddApp(ctx, newApp); err != nil {
		// 	// 	return fmt.Errorf("failed to add app to wolf: %w", err)
		// 	// }
		// 	appID = newApp.ID
		// }

		//!TODO: Add the ports into the request for this to support multiple
		// sessions per Gateway.
		//
		// Create the session
		clientIP := "10.128.1.0" // I'm  keeping this, for now...
		if session.Spec.Config.ClientIP != "" {
			clientIP = session.Spec.Config.ClientIP
		}

		// Sessions reconcile on several workers: hold the lock so two
		// players starting at once don't take the same client
		c.addSessionMu.Lock()
		defer c.addSessionMu.Unlock()

		pairedClients, err := wolfclient.ListPairedClients(ctx)
		if err != nil {
			return fmt.Errorf("failed to list paired clients: %w", err)
		}
		// Sessions may have been added since the list above
		sessions, err := wolfclient.ListSessions(ctx)
		if err != nil {
			return fmt.Errorf("failed to list sessions: %w", err)
		}
		clientID, err := freePlayerClient(pairedClients, sessions)
		if err != nil {
			return err
		}
		if clientID == "" {
			klog.Warningf("Wolf of %s has no player slots, session %s/%s gets the shared default ID", deploymentName, session.Namespace, session.Name)
		}

		takenIPs := sets.New[string]()
		for _, s := range sessions {
			takenIPs.Insert(s.RTSPFakeIP)
		}
		fakeIP := rtspFakeIP(session, takenIPs)

		sessionID, err := wolfclient.AddSession(ctx, wolfapi.Session{
			ClientID:          clientID,
			VideoWidth:        session.Spec.Config.VideoWidth,
			VideoHeight:       session.Spec.Config.VideoHeight,
			VideoRefreshRate:  session.Spec.Config.VideoRefreshRate,
			// AppID:             appID,
			AudioChannelCount: 2, // !TODO: parse from audio info

			ClientIP: clientIP, // In the future, this will be acquired dynamically
			//If this isn't present it crashes
			//so, I'll keep it here until I figure out a way to pass off from moonlight client
			ClientSettings: wolfapi.ClientSettings{
				RunGID:              1000,
				RunUID:              1000,
				ControllersOverride: []string{"XBOX"},
				MouseAcceleration:   1.0,
				VScrollAcceleration: 1.0,
				HScrollAcceleration: 1.0,
			},
			AESKey: session.Spec.Config.AESKey,
			AESIV:  session.Spec.Config.AESIV,
			RTSPFakeIP: fakeIP,
		})

		if err != nil {
			return fmt.Errorf("failed to create session: %s", err)
		}
		session.Status.WolfSessionID = sessionID
		session.Status.StreamURL = fmt.Sprintf("rtsp://%s:%d", fakeIP, session.Status.Ports.RTSP)
	} else {
		//!TODO: Update wolf API to include session ID in list so we can update
		// these details/validate discrepencies
		// assert wolf session ID non-empty and matches what we expect
	}

	if session.Status.StreamURL == "" {
		// Added by an older operator, which used the service's address
		session.Status.StreamURL = fmt.Sprintf("rtsp://%s:%d", service.Spec.ClusterIP, session.Status.Ports.RTSP)
	}

	if coop := app.Spec.Coop; coop != nil {
		if err := c.ensureLobby(ctx, wolfclient, session, app, coop, wolfRenderNode(deployment)); err != nil {
			return fmt.Errorf("failed to start co-op lobby: %w", err)
		}
	}
	return nil
}

const (
	// Each pod's Wolf starts with this many placeholder paired clients, one
	// per concurrent session. Wolf names a session after its client, so
	// sessions added without one all get the same ID and break each other
	// once a co-op pod streams to several players.
	playerClientSlots = 8
	// app_state_folder, and certificate, of the placeholder clients. Wolf
	// only hashes the certificate, so it needn't be a real one.
	playerClientPrefix = "direwolf-player-"
)

// wolfSeedConfig is the config.toml a pod's Wolf starts from. Wolf rebuilds
// configs older than v7 from its defaults, keeping only the hostname, uuid
// and paired clients, which is all this sets.
func wolfSeedConfig(serverUUID string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "config_version = 6\nhostname = \"Direwolf\"\nuuid = %q\n", serverUUID)
	for i := 1; i <= playerClientSlots; i++ {
		name := fmt.Sprintf("%s%d", playerClientPrefix, i)
		fmt.Fprintf(&b, "\n[[paired_clients]]\nclient_cert = %q\napp_state_folder = %q\n", name, name)
	}
	return b.String()
}

// rtspFakeIP is the address in a session's RTSP URL. Wolf tells sessions apart
// by the host in the URL their client sends, while Moonlight only takes the
// port from it and connects to the server it paired with. Sessions sharing a
// pod need distinct ones, which taken holds.
func rtspFakeIP(session *v1alpha1types.Session, taken sets.Set[string]) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(session.UID))
	for {
		v := h.Sum32()
		ip := fmt.Sprintf("10.%d.%d.%d", byte(v>>16), byte(v>>8), byte(v))
		if !taken.Has(ip) {
			return ip
		}
		_, _ = h.Write([]byte{0})
	}
}

// freePlayerClient returns the ID of a placeholder paired client no running
// session uses, or "" if the pod's Wolf has none (it predates them).
func freePlayerClient(clients []wolfapi.PairedClient, sessions []wolfapi.Session) (string, error) {
	used := sets.New[string]()
	for _, s := range sessions {
		used.Insert(s.ClientID)
	}

	var slots int
	for _, client := range clients {
		if !strings.HasPrefix(client.AppStateFolder, playerClientPrefix) {
			continue
		}
		slots++
		if !used.Has(client.ClientID) {
			return client.ClientID, nil
		}
	}
	if slots == 0 {
		return "", nil
	}
	return "", fmt.Errorf("all %d player slots of the pod are in use", slots)
}

const (
	// Name the co-op lobby runner links the lobby's Wayland socket to, in
	// the XDG_RUNTIME_DIR shared by the pod's containers
	coopWaylandDisplay = "direwolf-lobby"
	// File in XDG_RUNTIME_DIR the lobby runner writes the lobby's audio sink
	// name to, for the app to use as PULSE_SINK
	coopSinkFile = "direwolf-lobby-sink"
)

// Wolf runs this inside its container for as long as the lobby lives. The app
// is the pod's own container, so this only points it at the lobby's display
// and audio sink. Wolf splits the command line itself, without a shell.
var coopLobbyRunCmd = fmt.Sprintf(
	`sh -c "ln -sfn $WAYLAND_DISPLAY $XDG_RUNTIME_DIR/%s && echo $PULSE_SINK > $XDG_RUNTIME_DIR/%s && exec sleep infinity"`,
	coopWaylandDisplay, coopSinkFile,
)

// wolfRenderNode is the render node the pod's Wolf uses for its sessions, which
// the lobby's compositor must use too: another GPU may not even support GBM,
// like NVIDIA cards next to an iGPU.
func wolfRenderNode(deployment *appsv1.Deployment) string {
	renderNode := "/dev/dri/renderD128" // Wolf's default
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name != "wolf" {
			continue
		}
		// Later entries win, like user sidecar policies over app settings
		for _, env := range container.Env {
			if env.Name == "WOLF_RENDER_NODE" && env.Value != "" {
				renderNode = env.Value
			}
		}
	}
	return renderNode
}

// Prefix of the co-op lobbies the operator creates, which wolf-agent joins
// streams to
const coopLobbyPrefix = "direwolf-"

func coopLobbyName(app *v1alpha1types.App) string {
	return coopLobbyPrefix + app.Name
}

// ensureLobby starts the co-op lobby of the session's pod if it isn't running
// yet, before Moonlight gets the stream URL. wolf-agent joins each session to
// it once its stream is up: Wolf only switches streams that already run.
func (c *SessionController) ensureLobby(
	ctx context.Context,
	wolfclient wolfapi.Client,
	session *v1alpha1types.Session,
	app *v1alpha1types.App,
	coop *v1alpha1types.CoopConfig,
	renderNode string,
) error {
	// Sessions reconcile on several workers, two players starting at once
	// must not each create a lobby
	c.lobbyMu.Lock()
	defer c.lobbyMu.Unlock()

	lobbies, err := wolfclient.ListLobbies(ctx)
	if err != nil {
		return fmt.Errorf("failed to list lobbies: %w", err)
	}

	name := coopLobbyName(app)
	for _, lobby := range lobbies {
		if lobby.Name == name {
			return nil
		}
	}

	caps := coop.VideoProducerBufferCaps
	if caps == "" {
		caps = "video/x-raw"
	}

	klog.Infof("Creating co-op lobby %s for session %s/%s", name, session.Namespace, session.Name)
	_, err = wolfclient.CreateLobby(ctx, wolfapi.CreateLobbyRequest{
		ProfileID: "direwolf",
		Name:      name,
		MultiUser: true,
		// The lobby lives as long as the pod, which goes away once its
		// last session does. Restarting it would give the app a new
		// display, losing the game in progress.
		StopWhenEveryoneLeaves: false,
		// Wolf scales the lobby's frames to each client's resolution
		VideoSettings: wolfapi.LobbyVideoSettings{
			Width:                   session.Spec.Config.VideoWidth,
			Height:                  session.Spec.Config.VideoHeight,
			RefreshRate:             session.Spec.Config.VideoRefreshRate,
			WaylandRenderNode:       renderNode,
			RunnerRenderNode:        renderNode,
			VideoProducerBufferCaps: caps,
		},
		AudioSettings:     wolfapi.LobbyAudioSettings{ChannelCount: 2},
		RunnerStateFolder: app.Name,
		Runner: wolfapi.Runner{
			Type:   "process",
			RunCmd: coopLobbyRunCmd,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create lobby %s: %w", name, err)
	}
	return nil
}
