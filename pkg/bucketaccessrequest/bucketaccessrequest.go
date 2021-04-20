package bucketaccessrequest

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"github.com/kubernetes-sigs/container-object-storage-interface-controller/pkg/util"
	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	bucketclientset "sigs.k8s.io/container-object-storage-interface-api/clientset"
	bucketcontroller "sigs.k8s.io/container-object-storage-interface-api/controller"

	"k8s.io/klog/v2"
)

type bucketAccessRequestListener struct {
	kubeClient   kubeclientset.Interface
	bucketClient bucketclientset.Interface
}

func NewListener() bucketcontroller.BucketAccessRequestListener {
	return &bucketAccessRequestListener{}
}

func (b *bucketAccessRequestListener) InitializeKubeClient(k kubeclientset.Interface) {
	b.kubeClient = k
}

func (b *bucketAccessRequestListener) InitializeBucketClient(bc bucketclientset.Interface) {
	b.bucketClient = bc
}

// Add is in response to user adding a BucketAccessRequest. The call here will respond by creating a BucketAccess Object.
func (b *bucketAccessRequestListener) Add(ctx context.Context, obj *v1alpha1.BucketAccessRequest) error {
	klog.V(1).Infof("Add called for BucketAccessRequest %s", obj.Name)
	bucketAccessRequest := obj

	err := b.provisionBucketAccess(ctx, bucketAccessRequest)
	if err != nil {
		// Provisioning is 100% finished / not in progress.
		switch err {
		case util.ErrBucketAccessAlreadyExists:
			klog.V(1).Infof("BucketAccess already exist for this BucketAccessRequest %v.", bucketAccessRequest.Name)
			err = nil
		default:
			klog.V(1).Infof("Error occurred processing BucketAccessRequest %v: %v", bucketAccessRequest.Name, err)
		}
		return err
	}

	klog.V(1).Infof("BucketAccessRequest %v is successfully processed.", bucketAccessRequest.Name)
	return nil
}

// Update is called in response to a change to BucketAccessRequest. At this point
// BucketAccess cannot be changed once created as the Provisioner might have already acted upon the create BucketAccess and created the backend Bucket Credentials
// Changes to Protocol, Provisioner, BucketInstanceName, BucketRequest cannot be allowed. Best is to delete and recreate a new BucketAccessRequest
// Changes to ServiceAccount, PolicyActionsConfigMapData and Parameters should be considered in lieu with sidecar implementation
func (b *bucketAccessRequestListener) Update(ctx context.Context, old, new *v1alpha1.BucketAccessRequest) error {
	klog.V(1).Infof("Update called for BucketAccessRequest %v", old.Name)
	if new.ObjectMeta.DeletionTimestamp != nil {
		// BucketAccessRequest is being deleted, check and remove finalizer once BA is deleted
		return b.removeBucketAccess(ctx, new)
	}
	return nil
}

// Delete is in response to user deleting a BucketAccessRequest. The call here will respond by deleting a BucketAccess Object.
func (b *bucketAccessRequestListener) Delete(ctx context.Context, bucketAccessRequest *v1alpha1.BucketAccessRequest) error {
	klog.V(1).Infof("Delete called for BucketAccessRequest %v/%v", bucketAccessRequest.Namespace, bucketAccessRequest.Name)
	return nil
}

// provisionBucketAccess  attempts to provision a BucketAccess for the given bucketAccessRequest.
// Returns nil error only when the bucketaccess was provisioned. An error is return if we cannot create bucket access.
// A normal error is returned when  bucket acess  was not provisioned and provisioning should be retried (requeue the bucketAccessRequest),
// or a special error  errBucketAccessAlreadyExists, errInvalidBucketAccessClass is returned when provisioning was impossible and
// no further attempts to provision should be tried.
func (b *bucketAccessRequestListener) provisionBucketAccess(ctx context.Context, bucketAccessRequest *v1alpha1.BucketAccessRequest) error {
	baClient := b.bucketClient.ObjectstorageV1alpha1().BucketAccesses()
	bacClient := b.bucketClient.ObjectstorageV1alpha1().BucketAccessClasses()
	brClient := b.bucketClient.ObjectstorageV1alpha1().BucketRequests
	barClient := b.bucketClient.ObjectstorageV1alpha1().BucketAccessRequests
	coreClient := b.kubeClient.CoreV1()

	name := string(bucketAccessRequest.GetUID())

	if bucketAccessRequest.Status.BucketAccessName != "" {
		return util.ErrBucketAccessAlreadyExists
	}

	bucketAccessClassName := bucketAccessRequest.Spec.BucketAccessClassName
	bucketAccessClass, err := bacClient.Get(ctx, bucketAccessClassName, metav1.GetOptions{})
	if err != nil {
		// bucket access class is invalid or not specified, cannot continue with provisioning.
		klog.Errorf("error fetching bucketaccessclass [%v]: %v", bucketAccessClassName, err)
		return util.ErrInvalidBucketAccessClass
	}

	brName := bucketAccessRequest.Spec.BucketRequestName
	// TODO: catch this in a admission controller
	if brName == "" {
		return util.ErrInvalidBucketAccessRequest
	}
	bucketRequest, err := brClient(bucketAccessRequest.Namespace).Get(ctx, brName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("error fetching bucket request [%v]: %v", brName, err)
		return err
	}

	if bucketRequest.Status.BucketName == "" || !bucketRequest.Status.BucketAvailable {
		return util.ErrWaitForBucketProvisioning
	}

	saName := bucketAccessRequest.Spec.ServiceAccountName
	sa := &v1.ServiceAccount{}
	if saName != "" {
		sa, err = coreClient.ServiceAccounts(bucketAccessRequest.Namespace).Get(ctx, saName, metav1.GetOptions{})
		if err != nil {
			return err
		}
	}

	bucketaccess := &v1alpha1.BucketAccess{}
	bucketaccess.Name = name

	bucketaccess.Spec.BucketName = bucketRequest.Status.BucketName

	bucketaccess.Spec.BucketAccessRequest = &v1.ObjectReference{
		Name:      bucketAccessRequest.Name,
		Namespace: bucketAccessRequest.Namespace,
		UID:       bucketAccessRequest.ObjectMeta.UID,
	}
	bucketaccess.Spec.ServiceAccount = &v1.ObjectReference{
		Name:      sa.Name,
		Namespace: sa.Namespace,
		UID:       sa.ObjectMeta.UID,
	}
	// bucketaccess.Spec.MintedSecretName - set by the driver
	bucketaccess.Spec.PolicyActionsConfigMapData, err = util.ReadConfigData(b.kubeClient, bucketAccessClass.PolicyActionsConfigMap)
	if err != nil && err != util.ErrNilConfigMap {
		return err
	}
	//  bucketaccess.Spec.Principal - set by the driver

	bucketaccess.Spec.Parameters = util.CopySS(bucketAccessClass.Parameters)

	bucketaccess, err = baClient.Create(context.Background(), bucketaccess, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	if !util.CheckFinalizer(bucketAccessRequest, util.BARDeleteFinalizer) {
		bucketAccessRequest.ObjectMeta.Finalizers = append(bucketAccessRequest.ObjectMeta.Finalizers, util.BARDeleteFinalizer)
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		bucketAccessRequest.Status.BucketAccessName = bucketaccess.Name
		bucketAccessRequest.Status.AccessGranted = true
		_, err := barClient(bucketAccessRequest.Namespace).UpdateStatus(ctx, bucketAccessRequest, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	klog.Infof("Finished creating BucketAccess %v", bucketaccess.Name)
	return nil
}

func (b *bucketAccessRequestListener) removeBucketAccess(ctx context.Context, bucketAccessRequest *v1alpha1.BucketAccessRequest) error {
	bucketaccess := b.FindBucketAccess(ctx, bucketAccessRequest)
	if bucketaccess == nil {
		// bucketaccess for this BucketAccessRequest is not found
		return util.ErrBucketAccessDoesNotExist
	}

	// time to delete the BucketAccess Object
	err := b.bucketClient.ObjectstorageV1alpha1().BucketAccesses().Delete(context.Background(), bucketaccess.Name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	// we can safely remove the finalizer
	return b.removeBARDeleteFinalizer(ctx, bucketAccessRequest)
}

func (b *bucketAccessRequestListener) FindBucketAccess(ctx context.Context, bucketAccessRequest *v1alpha1.BucketAccessRequest) *v1alpha1.BucketAccess {
	bucketAccessList, err := b.bucketClient.ObjectstorageV1alpha1().BucketAccesses().List(ctx, metav1.ListOptions{})
	if err != nil || len(bucketAccessList.Items) <= 0 {
		return nil
	}
	for _, bucketaccess := range bucketAccessList.Items {
		if bucketaccess.Spec.BucketAccessRequest.Name == bucketAccessRequest.Name &&
			bucketaccess.Spec.BucketAccessRequest.Namespace == bucketAccessRequest.Namespace &&
			bucketaccess.Spec.BucketAccessRequest.UID == bucketAccessRequest.UID {
			return &bucketaccess
		}
	}
	return nil
}

func (b *bucketAccessRequestListener) removeBARDeleteFinalizer(ctx context.Context, bucketAccessRequest *v1alpha1.BucketAccessRequest) error {
	newFinalizers := []string{}
	for _, finalizer := range bucketAccessRequest.ObjectMeta.Finalizers {
		if finalizer != util.BARDeleteFinalizer {
			newFinalizers = append(newFinalizers, finalizer)
		}
	}
	bucketAccessRequest.ObjectMeta.Finalizers = newFinalizers

	_, err := b.bucketClient.ObjectstorageV1alpha1().BucketAccessRequests(bucketAccessRequest.Namespace).Update(ctx, bucketAccessRequest, metav1.UpdateOptions{})
	return err
}
