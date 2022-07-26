package bucketclaim

import (
	"context"

	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	bucketclientset "sigs.k8s.io/container-object-storage-interface-api/clientset"
	objectstoragev1alpha1 "sigs.k8s.io/container-object-storage-interface-api/clientset/typed/objectstorage.k8s.io/v1alpha1"

	"sigs.k8s.io/container-object-storage-interface-controller/pkg/util"
)

const (
	finalizer = "cosi.objectstorage.k8s.io/bucketclaim-protection"
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
		"name", bucketClaim.Name,
		"ns", bucketClaim.Namespace,
		"bucketClass", bucketClaim.Spec.BucketClassName,
		"bucketPrefix", bucketClaim.Spec.BucketPrefix,
	)

	err := b.provisionBucketClaimOperation(ctx, bucketClaim)
	if err != nil {
		switch err {
		case util.ErrInvalidBucketClass:
			klog.ErrorS(util.ErrInvalidBucketClass,
				"bucketClaim", bucketClaim.Name,
				"ns", bucketClaim.Namespace,
				"bucketClassName", bucketClaim.Spec.BucketClassName)
		case util.ErrBucketAlreadyExists:
			klog.V(3).InfoS("Bucket already exists",
				"bucketClaim", bucketClaim.Name,
				"ns", bucketClaim.Namespace,
			)
			return nil
		default:
			klog.ErrorS(err,
				"name", bucketClaim.Name,
				"ns", bucketClaim.Namespace,
				"err", err)
		}
		return err
	}

	klog.V(3).InfoS("Add BucketClaim success",
		"name", bucketClaim.Name,
		"ns", bucketClaim.Namespace)
	return nil
}

// update processes any updates  made to the bucket request
func (b *bucketClaimListener) Update(ctx context.Context, old, new *v1alpha1.BucketClaim) error {
	klog.V(3).InfoS("Update BucketClaim",
		"name", old.Name,
		"ns", old.Namespace)
	return nil
}

// Delete processes a bucket for which bucket request is deleted
func (b *bucketClaimListener) Delete(ctx context.Context, bucketClaim *v1alpha1.BucketClaim) error {
	klog.V(3).Infof("Delete BucketClaim  %v",
		"name", bucketClaim.Name,
		"ns", bucketClaim.Namespace)

	return nil
}

// provisionBucketClaimOperation attempts to provision a bucket for a given bucketClaim.
// Return values
//    nil - BucketClaim successfully processed
//    ErrInvalidBucketClass - BucketClass does not exist          [requeue'd with exponential backoff]
//    ErrBucketAlreadyExists - BucketClaim already processed
//    non-nil err - Internal error                                [requeue'd with exponential backoff]
func (b *bucketClaimListener) provisionBucketClaimOperation(ctx context.Context, bucketClaim *v1alpha1.BucketClaim) error {
	bucketClassName := b.getBucketClass(bucketClaim)
	bucketClass, err := b.BucketClasses().Get(ctx, bucketClassName, metav1.GetOptions{})
	if err != nil {
		klog.ErrorS(err, "Get Bucketclass Error", "name", bucketClassName)
		return util.ErrInvalidBucketClass
	}

	if bucketClaim.Status.BucketReady {
		return util.ErrBucketAlreadyExists
	}

	name = bucketClassName + string(bucketClaim.GetUID())

	// create bucket
	bucket := &v1alpha1.Bucket{}
	bucket.Name = name
	bucket.Status.BucketReady = false
	bucket.Spec.DriverName = bucketClass.DriverName
	bucket.Spec.BucketClassName = bucketClassName
	bucket.Spec.DeletionPolicy = bucketClass.DeletionPolicy
	bucket.Spec.BucketClaim = &v1.ObjectReference{
		Name:      bucketClaim.Name,
		Namespace: bucketClaim.Namespace,
		UID:       bucketClaim.ObjectMeta.UID,
	}
	bucket.Spec.Protocols = *bucketClass.Protocol.DeepCopy()
	bucket.Spec.Parameters = util.CopySS(bucketClass.Parameters)

	bucket, err = b.Buckets().Create(ctx, bucket, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		klog.ErrorS(err, "name", bucket.Name)
		return err
	}

	controllerutil.AddFinalizer(bucketClaim, finalizer)

	bucketClaim.Status.BucketName = bucket.Name
	bucketClaim.Status.BucketAvailable = true
	_, err = b.BucketClaims(bucketClaim.Namespace).UpdateStatus(ctx, bucketClaim, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	klog.Infof("Finished creating Bucket %v", bucket.Name)
	return nil
}

// getBucketClass returns BucketClassName. If no bucket class was in the request it returns empty
// TODO this methods can be more sophisticate to address bucketClass overrides using annotations just like SC.
func (b *bucketClaimListener) getBucketClass(bucketClaim *v1alpha1.BucketClaim) string {
	if bucketClaim.Spec.BucketClassName != "" {
		return bucketClaim.Spec.BucketClassName
	}

	return ""
}

// cloneTheBucket clones a bucket to a different namespace when a BR is for brownfield.
func (b *bucketClaimListener) cloneTheBucket(bucketClaim *v1alpha1.BucketClaim) error {
	klog.InfoS("Cloning Bucket", "name", bucketClaim.Status.BucketName)
	return util.ErrNotImplemented
}

func (b *bucketClaimListener) InitializeKubeClient(k kubeclientset.Interface) {
	b.kubeClient = k
}

func (b *bucketClaimListener) InitializeBucketClient(bc bucketclientset.Interface) {
	b.bucketClient = bc
}

func (b *bucketClaimListener) Buckets() objectstoragev1alpha1.BucketInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().Buckets()
	}
	panic("uninitialized listener")
}

func (b *bucketClaimListener) BucketClasses() objectstoragev1alpha1.BucketClassInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketClasses()
	}
	panic("uninitialized listener")
}

func (b *bucketClaimListener) BucketClaims(namespace string) objectstoragev1alpha1.BucketClaimInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketClaims(namespace)
	}
	panic("uninitialized listener")
}
