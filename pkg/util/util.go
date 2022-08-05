package util

import (
	"context"
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	types "sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage/v1alpha1"
	bucketclientset "sigs.k8s.io/container-object-storage-interface-api/client/clientset/versioned"

	"k8s.io/klog/v2"
)

func CopySS(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	copy := make(map[string]string, len(m))
	for k, v := range m {
		copy[k] = v
	}
	return copy
}

// GetBuckets will wait and fetch expected number of buckets created by the test
// This is used by bucket request unit tests
func GetBuckets(ctx context.Context, client bucketclientset.Interface, numExpected int) *types.BucketList {
	bucketList, err := client.ObjectstorageV1alpha1().Buckets().List(ctx, metav1.ListOptions{})
	if len(bucketList.Items) > 0 {
		return bucketList
	}
	numtimes := 0
	for numtimes < 10 {
		bucketList, err = client.ObjectstorageV1alpha1().Buckets().List(ctx, metav1.ListOptions{})
		if len(bucketList.Items) >= numExpected {
			return bucketList
		} else {
			klog.Errorf("Failed to fetch the bucket created %v", err)
		}
		numtimes++
		<-time.After(time.Duration(numtimes) * time.Second)
	}
	return &types.BucketList{}
}

// Validates the content of the bucket against bucket request and backet class
// This is used by bucket request unit tests
func ValidateBucket(bucket types.Bucket, bucketClaim types.BucketClaim, bucketClass types.BucketClass) bool {
	return (bucketClaim.Status.BucketName == bucket.ObjectMeta.Name &&
		bucket.Spec.BucketClassName == bucketClaim.Spec.BucketClassName &&
		bucket.Spec.BucketClaim.Name == bucketClaim.ObjectMeta.Name &&
		bucket.Spec.BucketClaim.Namespace == bucketClaim.ObjectMeta.Namespace &&
		bucket.Spec.BucketClaim.UID == bucketClaim.ObjectMeta.UID &&
		bucket.Spec.BucketClassName == bucketClass.ObjectMeta.Name &&
		reflect.DeepEqual(bucket.Spec.Parameters, bucketClass.Parameters) &&
		bucket.Spec.DriverName == bucketClass.DriverName &&
		bucket.Spec.DeletionPolicy == bucketClass.DeletionPolicy)
}

// Deletes any bucket api object or an array of bucket or bucket access objects.
// This is used by bucket request and bucket access request unit tests
func DeleteObjects(ctx context.Context, client bucketclientset.Interface, objs ...interface{}) {
	for _, obj := range objs {
		switch t := obj.(type) {
		case types.Bucket:
			client.ObjectstorageV1alpha1().Buckets().Delete(ctx, obj.(types.Bucket).Name, metav1.DeleteOptions{})
		case types.BucketClaim:
			client.ObjectstorageV1alpha1().BucketClaims(obj.(types.BucketClaim).Namespace).Delete(ctx, obj.(types.BucketClaim).Name, metav1.DeleteOptions{})
		case types.BucketClass:
			client.ObjectstorageV1alpha1().BucketClasses().Delete(ctx, obj.(types.BucketClass).Name, metav1.DeleteOptions{})
		case []types.Bucket:
			for _, a := range obj.([]types.Bucket) {
				DeleteObjects(ctx, client, a)
			}
		default:
			klog.Errorf("Unknown Obj of type %v", t)
		}
	}
}

// CreateBucketClaim creates a bucket claim object or return an existing bucket request object
// This is used by bucket claim unit tests
func CreateBucketClaim(ctx context.Context, client bucketclientset.Interface, bc *types.BucketClaim) (*types.BucketClaim, error) {
	bc, err := client.ObjectstorageV1alpha1().BucketClaims(bc.Namespace).Create(ctx, bc, metav1.CreateOptions{})
	if (err != nil) && apierrors.IsAlreadyExists(err) {
		bc, err = client.ObjectstorageV1alpha1().BucketClaims(bc.Namespace).Get(ctx, bc.Name, metav1.GetOptions{})
	}
	return bc, err
}

// CreateBucketClass creates a bucket class object or return an existing bucket class object
// This is used by bucket claim unit tests
func CreateBucketClass(ctx context.Context, client bucketclientset.Interface, bc *types.BucketClass) (*types.BucketClass, error) {
	bc, err := client.ObjectstorageV1alpha1().BucketClasses().Create(ctx, bc, metav1.CreateOptions{})
	if (err != nil) && apierrors.IsAlreadyExists(err) {
		bc, err = client.ObjectstorageV1alpha1().BucketClasses().Get(ctx, bc.Name, metav1.GetOptions{})
	}
	return bc, err
}
