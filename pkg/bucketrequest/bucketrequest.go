package bucketrequest

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	"github.com/kubernetes-sigs/container-object-storage-interface-controller/pkg/util"
	kubeclientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	bucketclientset "sigs.k8s.io/container-object-storage-interface-api/clientset"
	objectstoragev1alpha1 "sigs.k8s.io/container-object-storage-interface-api/clientset/typed/objectstorage.k8s.io/v1alpha1"

	"k8s.io/klog/v2"
)

// bucketRequestListener is a resource handler for bucket requests objects
type bucketRequestListener struct {
	kubeClient   kubeclientset.Interface
	bucketClient bucketclientset.Interface
}

func NewBucketRequestListener() *bucketRequestListener {
	return &bucketRequestListener{}
}

// Add creates a bucket in response to a bucketrequest
func (b *bucketRequestListener) Add(ctx context.Context, bucketRequest *v1alpha1.BucketRequest) error {
	klog.V(3).InfoS("Add BucketRequest",
		"name", bucketRequest.Name,
		"ns", bucketRequest.Namespace,
		"bucketClass", bucketRequest.Spec.BucketClassName,
		"bucketPrefix", bucketRequest.Spec.BucketPrefix,
	)

	err := b.provisionBucketRequestOperation(ctx, bucketRequest)
	if err != nil {
		switch err {
		case util.ErrInvalidBucketClass:
			klog.ErrorS(util.ErrInvalidBucketClass,
				"bucketRequest", bucketRequest.Name,
				"ns", bucketRequest.Namespace,
				"bucketClassName", bucketRequest.Spec.BucketClassName)
		case util.ErrBucketAlreadyExists:
			klog.V(3).InfoS("Bucket already exists",
				"bucketRequest", bucketRequest.Name,
				"ns", bucketRequest.Namespace,
			)
			return nil
		default:
			klog.ErrorS(err,
				"name", bucketRequest.Name,
				"ns", bucketRequest.Namespace,
				"err", err)
		}
		return err
	}

	klog.V(3).InfoS("Add BucketRequest success",
		"name", bucketRequest.Name,
		"ns", bucketRequest.Namespace)
	return nil
}

// update processes any updates  made to the bucket request
func (b *bucketRequestListener) Update(ctx context.Context, old, new *v1alpha1.BucketRequest) error {
	klog.V(3).InfoS("Update BucketRequest",
		"name", old.Name,
		"ns", old.Namespace)
	if (old.ObjectMeta.DeletionTimestamp == nil) &&
		(new.ObjectMeta.DeletionTimestamp != nil) {
		// BucketRequest is being deleted, check and remove finalizer once BA is deleted
		return b.removeBucket(ctx, new)
	}
	return nil
}

// Delete processes a bucket for which bucket request is deleted
func (b *bucketRequestListener) Delete(ctx context.Context, bucketRequest *v1alpha1.BucketRequest) error {
	klog.V(3).Infof("Delete BucketRequest  %v",
		"name", bucketRequest.Name,
		"ns", bucketRequest.Namespace)
	return nil
}

// provisionBucketRequestOperation attempts to provision a bucket for a given bucketRequest.
// Return values
//    nil - BucketRequest successfully processed
//    ErrInvalidBucketClass - BucketClass does not exist          [requeue'd with exponential backoff]
//    ErrBucketAlreadyExists - BucketRequest already processed
//    non-nil err - Internal error                                [requeue'd with exponential backoff]
func (b *bucketRequestListener) provisionBucketRequestOperation(ctx context.Context, bucketRequest *v1alpha1.BucketRequest) error {
	bucketClassName := b.getBucketClass(bucketRequest)
	bucketClass, err := b.BucketClasses().Get(ctx, bucketClassName, metav1.GetOptions{})
	if err != nil {
		klog.ErrorS(err, "Get Bucketclass Error", "name", bucketClassName)
		return util.ErrInvalidBucketClass
	}

	name := bucketRequest.Spec.BucketPrefix
	if name != "" {
		name = name + "-"
	}
	name = name + string(bucketRequest.GetUID())

	if bucketRequest.Status.BucketName != "" {
		return util.ErrBucketAlreadyExists
	}

	// create bucket
	bucket := &v1alpha1.Bucket{}

	bucket.Name = name
	bucket.Spec.BucketID = name
	bucket.Spec.Provisioner = bucketClass.Provisioner
	bucket.Spec.BucketClassName = bucketClass.Name
	bucket.Spec.DeletionPolicy = bucketClass.DeletionPolicy
	bucket.Spec.BucketRequest = &v1.ObjectReference{
		Name:      bucketRequest.Name,
		Namespace: bucketRequest.Namespace,
		UID:       bucketRequest.ObjectMeta.UID,
	}
	bucket.Spec.AllowedNamespaces = util.CopyStrings(bucketClass.AllowedNamespaces)
	bucket.Spec.Protocol = *bucketClass.Protocol.DeepCopy()
	bucket.Spec.Parameters = util.CopySS(bucketClass.Parameters)

	bucket, err = b.Buckets().Create(ctx, bucket, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		klog.ErrorS(err, "name", bucket.Name)
		return err
	}

	if !util.CheckFinalizer(bucketRequest, util.BRDeleteFinalizer) {
		bucketRequest.ObjectMeta.Finalizers = append(bucketRequest.ObjectMeta.Finalizers, util.BRDeleteFinalizer)
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		bucketRequest.Status.BucketName = bucket.Name
		bucketRequest.Status.BucketAvailable = true
		_, err := b.BucketRequests(bucketRequest.Namespace).UpdateStatus(ctx, bucketRequest, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	klog.Infof("Finished creating Bucket %v", bucket.Name)
	return nil
}

// When a BR is deleted before the finalizer is removed then the bucket corresponding to the BR should be deleted.
func (b *bucketRequestListener) removeBucket(ctx context.Context, bucketRequest *v1alpha1.BucketRequest) error {
	if bucketRequest.Status.BucketName == "" {
		// bucket for this BucketRequest is not found
		return util.ErrBucketDoesNotExist
	}

	// time to delete the Bucket Object
	err := b.bucketClient.ObjectstorageV1alpha1().Buckets().Delete(context.Background(), bucketRequest.Status.BucketName, metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	// we can safely remove the finalizer
	return b.removeBRDeleteFinalizer(ctx, bucketRequest)
}

// getBucketClass returns BucketClassName. If no bucket class was in the request it returns empty
// TODO this methods can be more sophisticate to address bucketClass overrides using annotations just like SC.
func (b *bucketRequestListener) getBucketClass(bucketRequest *v1alpha1.BucketRequest) string {
	if bucketRequest.Spec.BucketClassName != "" {
		return bucketRequest.Spec.BucketClassName
	}

	return ""
}

// cloneTheBucket clones a bucket to a different namespace when a BR is for brownfield.
func (b *bucketRequestListener) cloneTheBucket(bucketRequest *v1alpha1.BucketRequest) error {
	klog.InfoS("Cloning Bucket", "name", bucketRequest.Status.BucketName)
	return util.ErrNotImplemented
}

func (b *bucketRequestListener) InitializeKubeClient(k kubeclientset.Interface) {
	b.kubeClient = k
}

func (b *bucketRequestListener) InitializeBucketClient(bc bucketclientset.Interface) {
	b.bucketClient = bc
}

func (b *bucketRequestListener) Buckets() objectstoragev1alpha1.BucketInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().Buckets()
	}
	panic("uninitialized listener")
}

func (b *bucketRequestListener) BucketClasses() objectstoragev1alpha1.BucketClassInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketClasses()
	}
	panic("uninitialized listener")
}

func (b *bucketRequestListener) removeBRDeleteFinalizer(ctx context.Context, bucketRequest *v1alpha1.BucketRequest) error {
	newFinalizers := []string{}
	for _, finalizer := range bucketRequest.ObjectMeta.Finalizers {
		if finalizer != util.BRDeleteFinalizer {
			newFinalizers = append(newFinalizers, finalizer)
		}
	}
	bucketRequest.ObjectMeta.Finalizers = newFinalizers

	_, err := b.bucketClient.ObjectstorageV1alpha1().BucketRequests(bucketRequest.Namespace).Update(ctx, bucketRequest, metav1.UpdateOptions{})
	return err
}

func (b *bucketRequestListener) BucketRequests(namespace string) objectstoragev1alpha1.BucketRequestInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketRequests(namespace)
	}
	panic("uninitialized listener")
}
