package bucketrequest

import (
	"context"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	"github.com/kubernetes-sigs/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	bucketclientset "github.com/kubernetes-sigs/container-object-storage-interface-api/clientset"
	bucketcontroller "github.com/kubernetes-sigs/container-object-storage-interface-api/controller"
	"github.com/kubernetes-sigs/container-object-storage-interface-controller/pkg/util"
	kubeclientset "k8s.io/client-go/kubernetes"

	"github.com/golang/glog"
)

type bucketRequestListener struct {
	kubeClient   kubeclientset.Interface
	bucketClient bucketclientset.Interface
}

func NewListener() bucketcontroller.BucketRequestListener {
	return &bucketRequestListener{}
}

func (b *bucketRequestListener) InitializeKubeClient(k kubeclientset.Interface) {
	b.kubeClient = k
}

func (b *bucketRequestListener) InitializeBucketClient(bc bucketclientset.Interface) {
	b.bucketClient = bc
}

// Add creates a bucket in response to a bucketrequest
func (b *bucketRequestListener) Add(ctx context.Context, obj *v1alpha1.BucketRequest) error {
	glog.V(3).Infof("Add called for BucketRequest %s", obj.Name)
	bucketRequest := obj
	err := b.provisionBucketRequestOperation(ctx, bucketRequest)
	if err != nil {
		// Provisioning is 100% finished / not in progress.
		switch err {
		case util.ErrInvalidBucketClass:
			glog.V(1).Infof("BucketClass specified does not exist while processing BucketRequest %v.", bucketRequest.Name)
			err = nil
		case util.ErrBucketAlreadyExists:
			glog.V(1).Infof("Bucket already exist for this bucket request %v.", bucketRequest.Name)
			err = nil
		default:
			glog.V(1).Infof("Error occurred processing BucketRequest %v: %v", bucketRequest.Name, err)
		}
		return err
	}

	glog.V(1).Infof("BucketRequest %v is successfully processed.", bucketRequest.Name)
	return nil
}

// update processes any updates  made to the bucket request
func (b *bucketRequestListener) Update(ctx context.Context, old, new *v1alpha1.BucketRequest) error {
	glog.V(3).Infof("Update called for BucketRequest  %v", old.Name)
	return nil
}

// Delete processes a bucket for which bucket request is deleted
func (b *bucketRequestListener) Delete(ctx context.Context, obj *v1alpha1.BucketRequest) error {
	glog.V(3).Infof("Delete called for BucketRequest  %v", obj.Name)
	return nil
}

// provisionBucketRequestOperation attempts to provision a bucket for the given bucketRequest.
// Returns nil error only when the bucket was provisioned, an error it set appropriately if not.
// Returns a normal error when the bucket was not provisioned and provisioning should be retried (requeue the bucketRequest),
// or the special error errBucketAlreadyExists, errInvalidBucketClass, when provisioning was impossible and
// no further attempts to provision should be tried.
func (b *bucketRequestListener) provisionBucketRequestOperation(ctx context.Context, bucketRequest *v1alpha1.BucketRequest) error {
	// Most code here is identical to that found in controller.go of kube's  controller...
	bucketClassName := b.GetBucketClass(bucketRequest)

	bucketClass, err := b.bucketClient.ObjectstorageV1alpha1().BucketClasses().Get(ctx, bucketClassName, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("error getting bucketclass: [%v] %v", bucketClassName, err)
		return util.ErrInvalidBucketClass
	}

	name := bucketRequest.Spec.BucketPrefix
	if name != "" {
		name = name + "-"
	}
	name = name + string(bucketRequest.GetUID())

	bucket, err := b.bucketClient.ObjectstorageV1alpha1().Buckets().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		// anything other than 404
		if !errors.IsNotFound(err) {
			glog.Errorf("error fetching bucket: %v", err)
			return err
		}
	} else { // if bucket found
		return nil
	}

	// create bucket
	bucket = &v1alpha1.Bucket{}

	bucket.Name = name
	bucket.Spec.Provisioner = bucketClass.Provisioner
	bucket.Spec.RetentionPolicy = bucketClass.RetentionPolicy
	bucket.Spec.AnonymousAccessMode = bucketClass.AnonymousAccessMode
	bucket.Spec.BucketClassName = bucketClass.Name
	bucket.Spec.BucketRequest = &v1.ObjectReference{
		Name:      bucketRequest.Name,
		Namespace: bucketRequest.Namespace,
		UID:       bucketRequest.ObjectMeta.UID}
	bucket.Spec.AllowedNamespaces = util.CopyStrings(bucketClass.AllowedNamespaces)
	bucket.Spec.Parameters = util.CopySS(bucketClass.Parameters)

	// TODO have a switch statement to populate appropriate protocol based on BR.Protocol
	bucket.Spec.Protocol.RequestedProtocol = bucketRequest.Spec.Protocol

	bucket, err = b.bucketClient.ObjectstorageV1alpha1().Buckets().Create(context.Background(), bucket, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			return nil
		}
		glog.V(5).Infof("Error occurred when creating Bucket %v", err)
		return err
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		bucketRequest.Spec.BucketInstanceName = bucket.Name
		_, err := b.bucketClient.ObjectstorageV1alpha1().BucketRequests(bucketRequest.Namespace).Update(ctx, bucketRequest, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	glog.Infof("Finished creating Bucket %v", bucket.Name)
	return nil
}

// GetBucketClass returns BucketClassName. If no bucket class was in the request it returns empty
// TODO this methods can be more sophisticate to address bucketClass overrides using annotations just like SC.
func (b *bucketRequestListener) GetBucketClass(bucketRequest *v1alpha1.BucketRequest) string {

	if bucketRequest.Spec.BucketClassName != "" {
		return bucketRequest.Spec.BucketClassName
	}

	return ""
}

func (b *bucketRequestListener) FindBucket(ctx context.Context, br *v1alpha1.BucketRequest) *v1alpha1.Bucket {
	bucketList, err := b.bucketClient.ObjectstorageV1alpha1().Buckets().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}
	if len(bucketList.Items) > 0 {
		for _, bucket := range bucketList.Items {
			if strings.HasPrefix(bucket.Name, br.Spec.BucketPrefix) &&
				bucket.Spec.BucketClassName == br.Spec.BucketClassName &&
				bucket.Spec.BucketRequest.Name == br.Name &&
				bucket.Spec.BucketRequest.Namespace == br.Namespace &&
				bucket.Spec.BucketRequest.UID == br.ObjectMeta.UID {
				return &bucket
			}
		}
	}
	return nil
}

// cloneTheBucket clones a bucket to a different namespace when a BR is for brownfield.
func (b *bucketRequestListener) cloneTheBucket(bucketRequest *v1alpha1.BucketRequest) error {
	glog.V(1).Infof("Clone called for Bucket %s", bucketRequest.Spec.BucketInstanceName)
	return util.ErrNotImplemented
}
