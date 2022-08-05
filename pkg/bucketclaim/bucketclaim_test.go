package bucketclaim

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	types "sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage/v1alpha1"
	bucketclientset "sigs.k8s.io/container-object-storage-interface-api/client/clientset/versioned/fake"

	"sigs.k8s.io/container-object-storage-interface-controller/pkg/util"
)

var classGoldParameters = map[string]string{
	"param1": "value1",
	"param2": "value2",
}

var goldClass = types.BucketClass{
	TypeMeta: metav1.TypeMeta{
		APIVersion: "objectstorage.k8s.io/v1alpha1",
		Kind:       "BucketClass",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name: "classgold",
	},
	DriverName: "sample.cosi.driver",
	Parameters: classGoldParameters,
	DeletionPolicy: types.DeletionPolicyDelete,
}

var bucketClaim1 = types.BucketClaim{
	TypeMeta: metav1.TypeMeta{
		APIVersion: "objectstorage.k8s.io/v1alpha1",
		Kind:       "BucketClaim",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name:      "bucketclaim1",
		Namespace: "default",
		UID:       "12345-67890",
	},
	Spec: types.BucketClaimSpec{
		BucketClassName: "classgold",
		Protocols: []types.Protocol{types.ProtocolAzure, types.ProtocolS3},
	},
}

var bucketClaim2 = types.BucketClaim{
	TypeMeta: metav1.TypeMeta{
		APIVersion: "objectstorage.k8s.io/v1alpha1",
		Kind:       "BucketClaim",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name:      "bucketclaim2",
		Namespace: "default",
		UID:       "abcde-fghijk",
	},
	Spec: types.BucketClaimSpec{
		BucketClassName: "classgold",
		Protocols: []types.Protocol{types.ProtocolAzure, types.ProtocolGCP},
	},
}

// Test basic add functionality
func TestAddBR(t *testing.T) {
	runCreateBucket(t, "add")
}

// Test add with multipleBRs
func TestAddWithMultipleBR(t *testing.T) {
	runCreateBucketWithMultipleBR(t, "addWithMultipleBR")
}

// Test add idempotency
func TestAddBRIdempotency(t *testing.T) {
	runCreateBucketIdempotency(t, "addWithMultipleBR")
}

func runCreateBucket(t *testing.T, name string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := bucketclientset.NewSimpleClientset()
	kubeClient := fake.NewSimpleClientset()

	listener := NewBucketClaimListener()
	listener.InitializeKubeClient(kubeClient)
	listener.InitializeBucketClient(client)

	bucketclass, err := util.CreateBucketClass(ctx, client, &goldClass)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketClass: %v", err)
	}

	bucketClaim, err := util.CreateBucketClaim(ctx, client, &bucketClaim1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketClaim: %v", err)
	}

	listener.Add(ctx, bucketClaim)

	bucketList := util.GetBuckets(ctx, client, 1)
	defer util.DeleteObjects(ctx, client, *bucketClaim, *bucketclass, bucketList.Items)

	if len(bucketList.Items) != 1 {
		t.Fatalf("Expecting a single Bucket created but found %v", len(bucketList.Items))
	}
	bucket := bucketList.Items[0]

	bucketClaim, err = client.ObjectstorageV1alpha1().BucketClaims(bucketClaim.Namespace).Get(ctx, bucketClaim.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when reading BucketClaim: %v", err)
	}

	if util.ValidateBucket(bucket, *bucketClaim, *bucketclass) {
		return
	} else {
		t.Fatalf("Failed to compare the resulting Bucket with the BucketClaim %v and BucketClass %v", bucketClaim, bucketclass)
	}
}

func runCreateBucketWithMultipleBR(t *testing.T, name string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := bucketclientset.NewSimpleClientset()
	kubeClient := fake.NewSimpleClientset()

	listener := NewBucketClaimListener()
	listener.InitializeKubeClient(kubeClient)
	listener.InitializeBucketClient(client)

	bucketclass, err := util.CreateBucketClass(ctx, client, &goldClass)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketClass: %v", err)
	}

	bucketClaim, err := util.CreateBucketClaim(ctx, client, &bucketClaim1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketClaim: %v", err)
	}

	bucketClaim2, err := util.CreateBucketClaim(ctx, client, &bucketClaim2)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketClaim: %v", err)
	}

	listener.Add(ctx, bucketClaim)
	listener.Add(ctx, bucketClaim2)

	bucketList := util.GetBuckets(ctx, client, 2)
	defer util.DeleteObjects(ctx, client, *bucketClaim, *bucketClaim2, *bucketclass, bucketList.Items)
	if len(bucketList.Items) != 2 {
		t.Fatalf("Expecting two Buckets created but found %v", len(bucketList.Items))
	}
	bucket := bucketList.Items[0]
	bucket2 := bucketList.Items[1]

	bucketClaim, err = client.ObjectstorageV1alpha1().BucketClaims(bucketClaim.Namespace).Get(ctx, bucketClaim.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when reading BucketClaim: %v", err)
	}
	bucketClaim2, err = client.ObjectstorageV1alpha1().BucketClaims(bucketClaim2.Namespace).Get(ctx, bucketClaim2.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when reading BucketClaim: %v", err)
	}

	if (util.ValidateBucket(bucket, *bucketClaim, *bucketclass) && util.ValidateBucket(bucket2, *bucketClaim2, *bucketclass)) ||
		(util.ValidateBucket(bucket2, *bucketClaim, *bucketclass) && util.ValidateBucket(bucket, *bucketClaim2, *bucketclass)) {
		return
	} else {
		t.Fatalf("Failed to compare the resulting Bucket with the BucketClaim %v and BucketClass %v", bucketClaim, bucketclass)
	}
}

func runCreateBucketIdempotency(t *testing.T, name string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := bucketclientset.NewSimpleClientset()
	kubeClient := fake.NewSimpleClientset()

	listener := NewBucketClaimListener()
	listener.InitializeKubeClient(kubeClient)
	listener.InitializeBucketClient(client)

	bucketclass, err := util.CreateBucketClass(ctx, client, &goldClass)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketClass: %v", err)
	}

	bucketClaim, err := util.CreateBucketClaim(ctx, client, &bucketClaim1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketClaim: %v", err)
	}

	listener.Add(ctx, bucketClaim)

	bucketList := util.GetBuckets(ctx, client, 1)
	defer util.DeleteObjects(ctx, client, *bucketClaim, *bucketclass, bucketList.Items)

	if len(bucketList.Items) != 1 {
		t.Errorf("Expecting a single Bucket created but found %v", len(bucketList.Items))
	}
	bucket := bucketList.Items[0]

	bucketClaim, err = client.ObjectstorageV1alpha1().BucketClaims(bucketClaim.Namespace).Get(ctx, bucketClaim.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when reading BucketClaim: %v", err)
	}

	if util.ValidateBucket(bucket, *bucketClaim, *bucketclass) {
		return
	} else {
		t.Fatalf("Failed to compare the resulting Bucket with the BucketClaim %v and BucketClass %v", bucketClaim, bucketclass)
		// call the add directly the second time
	}

	listener.Add(ctx, bucketClaim)

	bucketList = util.GetBuckets(ctx, client, 1)
	if len(bucketList.Items) != 1 {
		t.Fatalf("Expecting a single Bucket created but found %v", len(bucketList.Items))
	}
}
