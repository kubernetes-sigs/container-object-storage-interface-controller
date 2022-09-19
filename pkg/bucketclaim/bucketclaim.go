package bucketclaim

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage/v1alpha1"
	bucketclientset "sigs.k8s.io/container-object-storage-interface-api/client/clientset/versioned"
	objectstoragev1alpha1 "sigs.k8s.io/container-object-storage-interface-api/client/clientset/versioned/typed/objectstorage/v1alpha1"

	"sigs.k8s.io/container-object-storage-interface-controller/pkg/util"
)

// bucketClaimListener is a resource handler for bucket requests objects
type bucketClaimListener struct {
	kubeClient   kubeclientset.Interface
	bucketClient bucketclientset.Interface
}

func NewBucketClaimListener() *bucketClaimListener {
	return &bucketClaimListener{}
}

// Add creates a bucket in response to a bucketClaim
func (b *bucketClaimListener) Add(ctx context.Context, bucketClaim *v1alpha1.BucketClaim) error {
	klog.V(3).InfoS("Add BucketClaim",
		"name", bucketClaim.ObjectMeta.Name,
		"ns", bucketClaim.ObjectMeta.Namespace,
		"bucketClass", bucketClaim.Spec.BucketClassName,
	)

	err := b.provisionBucketClaimOperation(ctx, bucketClaim)
	if err != nil {
		switch err {
		case util.ErrInvalidBucketClass:
			klog.ErrorS(util.ErrInvalidBucketClass,
				"bucketClaim", bucketClaim.ObjectMeta.Name,
				"ns", bucketClaim.ObjectMeta.Namespace,
				"bucketClassName", bucketClaim.Spec.BucketClassName)
		case util.ErrBucketAlreadyExists:
			klog.V(3).InfoS("Bucket already exists",
				"bucketClaim", bucketClaim.ObjectMeta.Name,
				"ns", bucketClaim.ObjectMeta.Namespace,
			)
			return nil
		default:
			klog.ErrorS(err,
				"name", bucketClaim.ObjectMeta.Name,
				"ns", bucketClaim.ObjectMeta.Namespace,
				"err", err)
		}
		return err
	}

	klog.V(3).InfoS("Add BucketClaim success",
		"name", bucketClaim.ObjectMeta.Name,
		"ns", bucketClaim.ObjectMeta.Namespace)
	return nil
}

// update processes any updates  made to the bucket request
func (b *bucketClaimListener) Update(ctx context.Context, old, new *v1alpha1.BucketClaim) error {
	klog.V(3).InfoS("Update BucketClaim",
		"name", old.Name,
		"ns", old.Namespace)

	bucketClaim := new.DeepCopy()

	if !new.GetDeletionTimestamp().IsZero() {
		if controllerutil.ContainsFinalizer(bucketClaim, util.BucketClaimFinalizer) {
			bucketName := bucketClaim.Status.BucketName
			err := b.buckets().Delete(ctx, bucketName, metav1.DeleteOptions{})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Delete processes a bucket for which bucket request is deleted
func (b *bucketClaimListener) Delete(ctx context.Context, bucketClaim *v1alpha1.BucketClaim) error {
	klog.V(3).Infof("Delete BucketClaim  %v",
		"name", bucketClaim.ObjectMeta.Name,
		"ns", bucketClaim.ObjectMeta.Namespace)

	return nil
}

// provisionBucketClaimOperation attempts to provision a bucket for a given bucketClaim.
// Return values
//    nil - BucketClaim successfully processed
//    ErrInvalidBucketClass - BucketClass does not exist          [requeue'd with exponential backoff]
//    ErrBucketAlreadyExists - BucketClaim already processed
//    non-nil err - Internal error                                [requeue'd with exponential backoff]
func (b *bucketClaimListener) provisionBucketClaimOperation(ctx context.Context, inputBucketClaim *v1alpha1.BucketClaim) error {
	bucketClaim := inputBucketClaim.DeepCopy()
	if bucketClaim.Status.BucketReady {
		return util.ErrBucketAlreadyExists
	}

	var bucketName string
	var err error

	if bucketClaim.Spec.ExistingBucketName != "" {
		bucketName = bucketClaim.Spec.ExistingBucketName
		bucket, err := b.buckets().Get(ctx, bucketName, metav1.GetOptions{})
		if err != nil {
			klog.ErrorS(err, "Get Bucket with ExistingBucketName error", "name", bucketClaim.Spec.ExistingBucketName)
			return err
		}

		bucket.Spec.BucketClaim = &v1.ObjectReference{
			Name:      bucketClaim.ObjectMeta.Name,
			Namespace: bucketClaim.ObjectMeta.Namespace,
			UID:       bucketClaim.ObjectMeta.UID,
		}

		_, err = b.buckets().Update(ctx, bucket, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		bucketClaim.Status.BucketName = bucketName
		bucketClaim.Status.BucketReady = true
	} else {
		bucketClassName := bucketClaim.Spec.BucketClassName
		if bucketClassName == "" {
			return util.ErrInvalidBucketClass
		}

		bucketClass, err := b.bucketClasses().Get(ctx, bucketClassName, metav1.GetOptions{})
		if err != nil {
			klog.ErrorS(err, "Get Bucketclass Error", "name", bucketClassName)
			return util.ErrInvalidBucketClass
		}

		bucketName = bucketClassName + string(bucketClaim.ObjectMeta.UID)

		// create bucket
		bucket := &v1alpha1.Bucket{}
		bucket.Name = bucketName
		bucket.Spec.DriverName = bucketClass.DriverName
		bucket.Status.BucketReady = false
		bucket.Spec.BucketClassName = bucketClassName
		bucket.Spec.DeletionPolicy = bucketClass.DeletionPolicy
		bucket.Spec.Parameters = util.CopySS(bucketClass.Parameters)

		bucket.Spec.BucketClaim = &v1.ObjectReference{
			Name:      bucketClaim.ObjectMeta.Name,
			Namespace: bucketClaim.ObjectMeta.Namespace,
			UID:       bucketClaim.ObjectMeta.UID,
		}

		protocolCopy := make([]v1alpha1.Protocol, len(bucketClaim.Spec.Protocols))
		copy(protocolCopy, bucketClaim.Spec.Protocols)

		bucket.Spec.Protocols = protocolCopy
		bucket, err = b.buckets().Create(ctx, bucket, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			klog.ErrorS(err, "name", bucketName)
			return err
		}

		bucketClaim.Status.BucketName = bucketName
		bucketClaim.Status.BucketReady = false
	}

	_, err = b.bucketClaims(bucketClaim.ObjectMeta.Namespace).UpdateStatus(ctx, bucketClaim, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	// Add the finalizers so that bucketClaim is deleted
	// only after the associated bucket is deleted.
	controllerutil.AddFinalizer(bucketClaim, util.BucketClaimFinalizer)
	_, err = b.bucketClaims(bucketClaim.ObjectMeta.Namespace).Update(ctx, bucketClaim, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	klog.Infof("Finished creating Bucket %v", bucketName)
	return nil
}

func (b *bucketClaimListener) InitializeKubeClient(k kubeclientset.Interface) {
	b.kubeClient = k
}

func (b *bucketClaimListener) InitializeBucketClient(bc bucketclientset.Interface) {
	b.bucketClient = bc
}

func (b *bucketClaimListener) buckets() objectstoragev1alpha1.BucketInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().Buckets()
	}
	panic("uninitialized listener")
}

func (b *bucketClaimListener) bucketClasses() objectstoragev1alpha1.BucketClassInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketClasses()
	}
	panic("uninitialized listener")
}

func (b *bucketClaimListener) bucketClaims(namespace string) objectstoragev1alpha1.BucketClaimInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketClaims(namespace)
	}
	panic("uninitialized listener")
}
