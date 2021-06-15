package bucketaccessrequest

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	bucketclientset "sigs.k8s.io/container-object-storage-interface-api/clientset"
	bucketcontroller "sigs.k8s.io/container-object-storage-interface-api/controller"

	"sigs.k8s.io/container-object-storage-interface-controller/pkg/util"
)

const (
	finalizer = "cosi.objectstorage.k8s.io/bucketaccessrequest-protection"
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

func (b *bucketAccessRequestListener) Update(ctx context.Context, old, new *v1alpha1.BucketAccessRequest) error {
	klog.V(1).Infof("Update called for BucketAccessRequest %v", old.Name)
	return nil
}

func (b *bucketAccessRequestListener) Delete(ctx context.Context, bucketAccessRequest *v1alpha1.BucketAccessRequest) error {
	klog.V(1).Infof("Delete called for BucketAccessRequest %v", bucketAccessRequest.Name)

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
	if err != nil {
		return err
	}
	//  bucketaccess.Spec.Principal - set by the driver

	bucketaccess.Spec.Parameters = util.CopySS(bucketAccessClass.Parameters)

	bucketaccess, err = baClient.Create(context.Background(), bucketaccess, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	controllerutil.AddFinalizer(bucketAccessRequest, finalizer)

	bucketAccessRequest.Status.BucketAccessName = bucketaccess.Name
	bucketAccessRequest.Status.AccessGranted = true
	_, err = barClient(bucketAccessRequest.Namespace).UpdateStatus(ctx, bucketAccessRequest, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	klog.Infof("Finished creating BucketAccess %v", bucketaccess.Name)
	return nil
}
