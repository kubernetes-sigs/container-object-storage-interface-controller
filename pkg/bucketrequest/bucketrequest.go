package bucketrequest

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	glog.V(1).Infof("add called for bucket %s", obj.Name)
	bucketRequest := obj
	err := b.provisionBucketRequestOperation(ctx, bucketRequest)
	if err != nil {
		// Provisioning is 100% finished / not in progress.
		switch err {
		case util.ErrInvalidBucketClass:
			glog.V(5).Infof("Bucket Class specified does not exist. Stop provisioning, removing bucketRequest %s from bucketRequests in progress", bucketRequest.UID)
			err = nil
		case util.ErrBucketAlreadyExists:
			glog.V(5).Infof("Bucket already exist for this bucket request. Stop provisioning, removing bucketRequest %s from bucketRequests in progress", bucketRequest.UID)
			err = nil
		default:
			glog.V(2).Infof("Final error received, removing buckerRequest %s from bucketRequests in progress", bucketRequest.UID)
		}
		return err
	}

	glog.V(5).Infof("BucketRequest processing succeeded, removing bucketRequest %s from bucketRequests in progress", bucketRequest.UID)
	return nil
}

// update processes any updates  made to the bucket request
func (b *bucketRequestListener) Update(ctx context.Context, old, new *v1alpha1.BucketRequest) error {
	glog.V(1).Infof("update called for bucket %v", old)
	return nil
}

// Delete processes a bucket for which bucket request is deleted
func (b *bucketRequestListener) Delete(ctx context.Context, obj *v1alpha1.BucketRequest) error {
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

	//  A previous doProvisionBucketRequest may just have finished while we were waiting for
	//  the locks. Check that bucket (with deterministic name) hasn't been provisioned
	//  yet.
	bucket := b.FindBucket(ctx, bucketRequest)
	if bucket != nil {
		// bucket has been already provisioned, nothing to do.
		glog.Info("Bucket already exists", bucket.Name)
		return util.ErrBucketAlreadyExists
	}

	bucketClass, err := b.bucketClient.ObjectstorageV1alpha1().BucketClasses().Get(ctx, bucketClassName, metav1.GetOptions{})
	if bucketClass == nil {
		// bucketclass does not exist in order to create a bucket
		return util.ErrInvalidBucketClass
	}

	glog.Infof("creating bucket for bucketrequest %v", bucketRequest.Name)

	// create bucket
	bucket = &v1alpha1.Bucket{}
	bucket.Name = fmt.Sprintf("%s%s", bucketRequest.Spec.BucketPrefix, util.GetUUID())
	bucket.Spec.Provisioner = bucketClass.Provisioner
	bucket.Spec.RetentionPolicy = bucketClass.RetentionPolicy
	bucket.Spec.AnonymousAccessMode = bucketClass.AnonymousAccessMode
	bucket.Spec.BucketClassName = bucketClass.Name
	bucket.Spec.BucketRequest = &v1alpha1.BucketRequestReference{
		Name:      bucketRequest.Name,
		Namespace: bucketRequest.Namespace,
		UID:       bucketRequest.ObjectMeta.UID}
	bucket.Spec.AllowedNamespaces = util.CopyStrings(bucketClass.AllowedNamespaces)
	bucket.Spec.Parameters = util.CopySS(bucketClass.Parameters)

	// TODO have a switch statement to populate appropriate protocol based on BR.Protocol
	bucket.Spec.Protocol.RequestedProtocol = bucketRequest.Spec.Protocol

	bucket, err = b.bucketClient.ObjectstorageV1alpha1().Buckets().Create(context.Background(), bucket, metav1.CreateOptions{})
	if err != nil {
		glog.V(5).Infof("Error occurred when creating bucket %v", err)
		return err
	}

	glog.Infof("Finished creating bucket %v", bucket.Name)
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
	glog.V(1).Infof("clone called for bucket %s", bucketRequest.Spec.BucketInstanceName)
	return util.ErrNotImplemented
}

// logOperation format and prints logs
func logOperation(operation, format string, a ...interface{}) string {
	return fmt.Sprintf(fmt.Sprintf("%s: %s", operation, format), a...)
}
