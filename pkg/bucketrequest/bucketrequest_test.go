package bucketrequest

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes/fake"
	bucketclientset "sigs.k8s.io/container-object-storage-interface-api/clientset/fake"

	"github.com/kubernetes-sigs/container-object-storage-interface-controller/pkg/util"
	types "sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
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
	AllowedNamespaces:    []string{"default", "cosins"},
	Parameters:           classGoldParameters,
	Protocol:             types.Protocol{Name: "s3"},
	IsDefaultBucketClass: false,
}

var bucketRequest1 = types.BucketRequest{
	TypeMeta: metav1.TypeMeta{
		APIVersion: "objectstorage.k8s.io/v1alpha1",
		Kind:       "BucketRequest",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name:      "bucketrequest1",
		Namespace: "default",
		UID:       "12345-67890",
	},
	Spec: types.BucketRequestSpec{
		BucketPrefix:    "cosi",
		BucketClassName: "classgold",
	},
}

var bucketRequest2 = types.BucketRequest{
	TypeMeta: metav1.TypeMeta{
		APIVersion: "objectstorage.k8s.io/v1alpha1",
		Kind:       "BucketRequest",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name:      "bucketrequest2",
		Namespace: "default",
		UID:       "abcde-fghijk",
	},
	Spec: types.BucketRequestSpec{
		BucketPrefix:    "cosi",
		BucketClassName: "classgold",
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

	listener := NewListener()
	listener.InitializeKubeClient(kubeClient)
	listener.InitializeBucketClient(client)

	bucketclass, err := util.CreateBucketClass(ctx, client, &goldClass)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketClass: %v", err)
	}

	bucketrequest, err := util.CreateBucketRequest(ctx, client, &bucketRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketRequest: %v", err)
	}

	listener.Add(ctx, bucketrequest)

	bucketList := util.GetBuckets(ctx, client, 1)
	defer util.DeleteObjects(ctx, client, *bucketrequest, *bucketclass, bucketList.Items)

	if len(bucketList.Items) != 1 {
		t.Fatalf("Expecting a single Bucket created but found %v", len(bucketList.Items))
	}
	bucket := bucketList.Items[0]

	bucketrequest, err = client.ObjectstorageV1alpha1().BucketRequests(bucketrequest.Namespace).Get(ctx, bucketrequest.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when reading BucketRequest: %v", err)
	}

	if util.ValidateBucket(bucket, *bucketrequest, *bucketclass) {
		return
	} else {
		t.Fatalf("Failed to compare the resulting Bucket with the BucketRequest %v and BucketClass %v", bucketrequest, bucketclass)
	}
}

func runCreateBucketWithMultipleBR(t *testing.T, name string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := bucketclientset.NewSimpleClientset()
	kubeClient := fake.NewSimpleClientset()

	listener := NewListener()
	listener.InitializeKubeClient(kubeClient)
	listener.InitializeBucketClient(client)

	bucketclass, err := util.CreateBucketClass(ctx, client, &goldClass)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketClass: %v", err)
	}

	bucketrequest, err := util.CreateBucketRequest(ctx, client, &bucketRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketRequest: %v", err)
	}

	bucketrequest2, err := util.CreateBucketRequest(ctx, client, &bucketRequest2)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketRequest: %v", err)
	}

	listener.Add(ctx, bucketrequest)
	listener.Add(ctx, bucketrequest2)

	bucketList := util.GetBuckets(ctx, client, 2)
	defer util.DeleteObjects(ctx, client, *bucketrequest, *bucketrequest2, *bucketclass, bucketList.Items)
	if len(bucketList.Items) != 2 {
		t.Fatalf("Expecting two Buckets created but found %v", len(bucketList.Items))
	}
	bucket := bucketList.Items[0]
	bucket2 := bucketList.Items[1]

	bucketrequest, err = client.ObjectstorageV1alpha1().BucketRequests(bucketrequest.Namespace).Get(ctx, bucketrequest.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when reading BucketRequest: %v", err)
	}
	bucketrequest2, err = client.ObjectstorageV1alpha1().BucketRequests(bucketrequest2.Namespace).Get(ctx, bucketrequest2.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when reading BucketRequest: %v", err)
	}

	if (util.ValidateBucket(bucket, *bucketrequest, *bucketclass) && util.ValidateBucket(bucket2, *bucketrequest2, *bucketclass)) ||
		(util.ValidateBucket(bucket2, *bucketrequest, *bucketclass) && util.ValidateBucket(bucket, *bucketrequest2, *bucketclass)) {
		return
	} else {
		t.Fatalf("Failed to compare the resulting Bucket with the BucketRequest %v and BucketClass %v", bucketrequest, bucketclass)
	}
}

func runCreateBucketIdempotency(t *testing.T, name string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := bucketclientset.NewSimpleClientset()
	kubeClient := fake.NewSimpleClientset()

	listener := NewListener()
	listener.InitializeKubeClient(kubeClient)
	listener.InitializeBucketClient(client)

	bucketclass, err := util.CreateBucketClass(ctx, client, &goldClass)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketClass: %v", err)
	}

	bucketrequest, err := util.CreateBucketRequest(ctx, client, &bucketRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketRequest: %v", err)
	}

	listener.Add(ctx, bucketrequest)

	bucketList := util.GetBuckets(ctx, client, 1)
	defer util.DeleteObjects(ctx, client, *bucketrequest, *bucketclass, bucketList.Items)

	if len(bucketList.Items) != 1 {
		t.Errorf("Expecting a single Bucket created but found %v", len(bucketList.Items))
	}
	bucket := bucketList.Items[0]

	bucketrequest, err = client.ObjectstorageV1alpha1().BucketRequests(bucketrequest.Namespace).Get(ctx, bucketrequest.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when reading BucketRequest: %v", err)
	}

	if util.ValidateBucket(bucket, *bucketrequest, *bucketclass) {
		return
	} else {
		t.Fatalf("Failed to compare the resulting Bucket with the BucketRequest %v and BucketClass %v", bucketrequest, bucketclass)
		// call the add directly the second time
	}

	listener.Add(ctx, bucketrequest)

	bucketList = util.GetBuckets(ctx, client, 1)
	if len(bucketList.Items) != 1 {
		t.Fatalf("Expecting a single Bucket created but found %v", len(bucketList.Items))
	}
}
