package bucketaccessrequest

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes/fake"
	bucketclientset "sigs.k8s.io/container-object-storage-interface-api/clientset/fake"

	"github.com/kubernetes-sigs/container-object-storage-interface-controller/pkg/util"
	types "sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
)

var sa1 = v1.ServiceAccount{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "sa1",
		Namespace: "default",
	},
}

var sa2 = v1.ServiceAccount{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "sa2",
		Namespace: "default",
	},
}

var cosiConfigMap = v1.ConfigMap{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "testconfigmap",
		Namespace: "default",
		Labels: map[string]string{
			"cosi-configmap": "test-cred1",
		},
	},
	Data: map[string]string{
		"profile":  "profile1",
		"certfile": "cert1",
	},
}

var classGoldAccessParameters = map[string]string{
	"param1": "value1",
	"param2": "value2",
}

var goldAccessClass = types.BucketAccessClass{
	TypeMeta: metav1.TypeMeta{
		APIVersion: "objectstorage.k8s.io/v1alpha1",
		Kind:       "BucketAccessClass",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name: "classaccessgold",
	},
	PolicyActionsConfigMap: &v1.ObjectReference{Name: "testconfigmap", Namespace: "default"},
	Parameters:             classGoldAccessParameters,
}

var bucketRequest1 = types.BucketRequest{
	TypeMeta: metav1.TypeMeta{
		APIVersion: "objectstorage.k8s.io/v1alpha1",
		Kind:       "BucketRequest",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name:      "bucketrequest1",
		Namespace: "default",
		UID:       "br-12345",
	},
	Spec: types.BucketRequestSpec{
		BucketPrefix:    "cosi",
		BucketClassName: "classgold",
	},
	Status: types.BucketRequestStatus{
		BucketName:      "cosi1234567890",
		BucketAvailable: true,
	},
}

var bucketAccessRequest1 = types.BucketAccessRequest{
	TypeMeta: metav1.TypeMeta{
		APIVersion: "objectstorage.k8s.io/v1alpha1",
		Kind:       "BucketAccessRequest",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name:      "bucketaccessrequest1",
		Namespace: "default",
		UID:       "bar-12345",
	},
	Spec: types.BucketAccessRequestSpec{
		ServiceAccountName:    "sa1",
		BucketRequestName:     "bucketrequest1",
		BucketAccessClassName: "classaccessgold",
	},
}

var bucketAccessRequest2 = types.BucketAccessRequest{
	TypeMeta: metav1.TypeMeta{
		APIVersion: "objectstorage.k8s.io/v1alpha1",
		Kind:       "BucketAccessRequest",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name:      "bucketaccessrequest2",
		Namespace: "default",
		UID:       "bar-67890",
	},
	Spec: types.BucketAccessRequestSpec{
		ServiceAccountName:    "sa2",
		BucketRequestName:     "bucketrequest1",
		BucketAccessClassName: "classaccessgold",
	},
}

// Test basic add functionality
func TestAddBAR(t *testing.T) {
	runCreateBucketAccess(t, "add")
}

// Test add with multipleBRs
func TestAddWithMultipleBAR(t *testing.T) {
	runCreateBucketWithMultipleBA(t, "addWithMultipleBAR")
}

// Test add idempotency
func TestAddBARIdempotency(t *testing.T) {
	runCreateBucketIdempotency(t, "addBARIdempotency")
}

// Test  delete BAR
func TestDeleteBAR(t *testing.T) {
	runDeleteBucketAccessRequest(t, "deleteBAR")
}

// Test  delete BAR Idempotency
func TestDeleteBARIdempotency(t *testing.T) {
	runDeleteBucketAccessRequestIdempotency(t, "deleteBARIdempotency")
}

func runCreateBucketAccess(t *testing.T, name string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := bucketclientset.NewSimpleClientset()
	kubeClient := fake.NewSimpleClientset()

	listener := NewListener()
	listener.InitializeKubeClient(kubeClient)
	listener.InitializeBucketClient(client)

	_, err := kubeClient.CoreV1().ServiceAccounts(sa1.Namespace).Create(ctx, &sa1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error occurred when creating ServiceAccount: %v", err)
	}
	defer kubeClient.CoreV1().ServiceAccounts(sa1.Namespace).Delete(ctx, sa1.Name, metav1.DeleteOptions{})

	_, err = kubeClient.CoreV1().ConfigMaps(cosiConfigMap.Namespace).Create(ctx, &cosiConfigMap, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error occurred when creating ConfigMap: %v", err)
	}
	defer kubeClient.CoreV1().ConfigMaps(cosiConfigMap.Namespace).Delete(ctx, cosiConfigMap.Name, metav1.DeleteOptions{})

	bucketaccessclass, err := util.CreateBucketAccessClass(ctx, client, &goldAccessClass)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketAccessClass: %v", err)
	}

	bucketrequest, err := util.CreateBucketRequest(ctx, client, &bucketRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketRequest: %v", err)
	}

	bucketaccessrequest, err := util.CreateBucketAccessRequest(ctx, client, &bucketAccessRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketAccessRequest: %v", err)
	}

	listener.Add(ctx, bucketaccessrequest)

	bucketAccessList := util.GetBucketAccesses(ctx, client, 1)
	defer util.DeleteObjects(ctx, client, *bucketrequest, *bucketaccessrequest, *bucketaccessclass, bucketAccessList.Items)

	if len(bucketAccessList.Items) != 1 {
		t.Fatalf("Expecting a single BucketAccess created but found %v", len(bucketAccessList.Items))
	}
	bucketaccess := bucketAccessList.Items[0]

	bucketaccessrequest, err = client.ObjectstorageV1alpha1().BucketAccessRequests(bucketaccessrequest.Namespace).Get(ctx, bucketaccessrequest.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when updating BucketAccessRequest: %v", err)
	}

	if util.ValidateBucketAccess(bucketaccess, *bucketaccessrequest, *bucketaccessclass) {
		return
	} else {
		t.Fatalf("Failed to compare the resulting BucketAccess with the BucketAccessRequest %v and BucketAccessClass %v", bucketaccessrequest, bucketaccessclass)
	}
}

func runCreateBucketWithMultipleBA(t *testing.T, name string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := bucketclientset.NewSimpleClientset()
	kubeClient := fake.NewSimpleClientset()

	listener := NewListener()
	listener.InitializeKubeClient(kubeClient)
	listener.InitializeBucketClient(client)

	_, err := kubeClient.CoreV1().ServiceAccounts(sa1.Namespace).Create(ctx, &sa1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error occurred when creating ServiceAccount: %v", err)
	}
	defer kubeClient.CoreV1().ServiceAccounts(sa1.Namespace).Delete(ctx, sa1.Name, metav1.DeleteOptions{})

	_, err = kubeClient.CoreV1().ServiceAccounts(sa2.Namespace).Create(ctx, &sa2, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error occurred when creating ServiceAccount: %v", err)
	}
	defer kubeClient.CoreV1().ServiceAccounts(sa2.Namespace).Delete(ctx, sa2.Name, metav1.DeleteOptions{})

	_, err = kubeClient.CoreV1().ConfigMaps(cosiConfigMap.Namespace).Create(ctx, &cosiConfigMap, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error occurred when creating ConfigMap: %v", err)
	}
	defer kubeClient.CoreV1().ConfigMaps(cosiConfigMap.Namespace).Delete(ctx, cosiConfigMap.Name, metav1.DeleteOptions{})

	bucketaccessclass, err := util.CreateBucketAccessClass(ctx, client, &goldAccessClass)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketAccessClass: %v", err)
	}

	bucketrequest, err := util.CreateBucketRequest(ctx, client, &bucketRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketRequest: %v", err)
	}

	bucketaccessrequest, err := util.CreateBucketAccessRequest(ctx, client, &bucketAccessRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketAccessRequest: %v", err)
	}

	bucketaccessrequest2, err := util.CreateBucketAccessRequest(ctx, client, &bucketAccessRequest2)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketAccessRequest: %v", err)
	}

	listener.Add(ctx, bucketaccessrequest)
	listener.Add(ctx, bucketaccessrequest2)

	bucketAccessList := util.GetBucketAccesses(ctx, client, 2)
	defer util.DeleteObjects(ctx, client, *bucketrequest, *bucketaccessrequest, *bucketaccessrequest2, *bucketaccessclass, bucketAccessList.Items)

	if len(bucketAccessList.Items) != 2 {
		t.Fatalf("Expecting a single BucketAccess created but found %v", len(bucketAccessList.Items))
	}
	bucketaccess := bucketAccessList.Items[0]
	bucketaccess2 := bucketAccessList.Items[1]

	bucketaccessrequest, err = client.ObjectstorageV1alpha1().BucketAccessRequests(bucketaccessrequest.Namespace).Get(ctx, bucketaccessrequest.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when updating BucketAccessRequest: %v", err)
	}
	bucketaccessrequest2, err = client.ObjectstorageV1alpha1().BucketAccessRequests(bucketaccessrequest2.Namespace).Get(ctx, bucketaccessrequest2.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when updating BucketAccessRequest: %v", err)
	}

	if (util.ValidateBucketAccess(bucketaccess, *bucketaccessrequest, *bucketaccessclass) && util.ValidateBucketAccess(bucketaccess2, *bucketaccessrequest2, *bucketaccessclass)) ||
		(util.ValidateBucketAccess(bucketaccess2, *bucketaccessrequest, *bucketaccessclass) && util.ValidateBucketAccess(bucketaccess, *bucketaccessrequest2, *bucketaccessclass)) {
		return
	} else {
		t.Fatalf("Failed to compare the resulting BucketAccess with the BucketAccessRequest %v and BucketAccessClass %v", bucketaccessrequest, bucketaccessclass)
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

	_, err := kubeClient.CoreV1().ServiceAccounts(sa1.Namespace).Create(ctx, &sa1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error occurred when creating ServiceAccount: %v", err)
	}
	defer kubeClient.CoreV1().ServiceAccounts(sa1.Namespace).Delete(ctx, sa1.Name, metav1.DeleteOptions{})

	_, err = kubeClient.CoreV1().ConfigMaps(cosiConfigMap.Namespace).Create(ctx, &cosiConfigMap, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error occurred when creating ConfigMap: %v", err)
	}
	defer kubeClient.CoreV1().ConfigMaps(cosiConfigMap.Namespace).Delete(ctx, cosiConfigMap.Name, metav1.DeleteOptions{})

	bucketaccessclass, err := util.CreateBucketAccessClass(ctx, client, &goldAccessClass)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketAccessClass: %v", err)
	}

	bucketrequest, err := util.CreateBucketRequest(ctx, client, &bucketRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketRequest: %v", err)
	}

	bucketaccessrequest, err := util.CreateBucketAccessRequest(ctx, client, &bucketAccessRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketAccessRequest: %v", err)
	}

	listener.Add(ctx, bucketaccessrequest)

	bucketAccessList := util.GetBucketAccesses(ctx, client, 1)
	defer util.DeleteObjects(ctx, client, *bucketrequest, *bucketaccessrequest, *bucketaccessclass, bucketAccessList.Items)

	if len(bucketAccessList.Items) != 1 {
		t.Fatalf("Expecting a single BucketAccess created but found %v", len(bucketAccessList.Items))
	}
	bucketaccess := bucketAccessList.Items[0]

	bucketaccessrequest, err = client.ObjectstorageV1alpha1().BucketAccessRequests(bucketaccessrequest.Namespace).Get(ctx, bucketaccessrequest.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when updating BucketAccessRequest: %v", err)
	}

	if !util.ValidateBucketAccess(bucketaccess, *bucketaccessrequest, *bucketaccessclass) {
		t.Fatalf("Failed to compare the resulting BucketAccess with the BucketAccessRequest %v and BucketAccessClass %v", bucketaccessrequest, bucketaccessclass)
	}

	// call the add directly the second time
	listener.Add(ctx, bucketaccessrequest)
	bucketAccessList = util.GetBucketAccesses(ctx, client, 1)
	if len(bucketAccessList.Items) != 1 {
		t.Fatalf("Expecting a single BucketAccess created but found %v", len(bucketAccessList.Items))
	}
}

func runDeleteBucketAccessRequest(t *testing.T, name string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := bucketclientset.NewSimpleClientset()
	kubeClient := fake.NewSimpleClientset()

	listener := NewListener()
	listener.InitializeKubeClient(kubeClient)
	listener.InitializeBucketClient(client)

	_, err := kubeClient.CoreV1().ServiceAccounts(sa1.Namespace).Create(ctx, &sa1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error occurred when creating ServiceAccount: %v", err)
	}
	defer kubeClient.CoreV1().ServiceAccounts(sa1.Namespace).Delete(ctx, sa1.Name, metav1.DeleteOptions{})

	_, err = kubeClient.CoreV1().ConfigMaps(cosiConfigMap.Namespace).Create(ctx, &cosiConfigMap, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error occurred when creating ConfigMap: %v", err)
	}
	defer kubeClient.CoreV1().ConfigMaps(cosiConfigMap.Namespace).Delete(ctx, cosiConfigMap.Name, metav1.DeleteOptions{})

	bucketaccessclass, err := util.CreateBucketAccessClass(ctx, client, &goldAccessClass)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketAccessClass: %v", err)
	}

	bucketrequest, err := util.CreateBucketRequest(ctx, client, &bucketRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketRequest: %v", err)
	}

	bucketaccessrequest, err := util.CreateBucketAccessRequest(ctx, client, &bucketAccessRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketAccessRequest: %v", err)
	}

	listener.Add(ctx, bucketaccessrequest)

	bucketAccessList := util.GetBucketAccesses(ctx, client, 1)
	defer util.DeleteObjects(ctx, client, *bucketrequest, *bucketaccessrequest, *bucketaccessclass, bucketAccessList.Items)

	if len(bucketAccessList.Items) != 1 {
		t.Fatalf("Expecting a single BucketAccess created but found %v", len(bucketAccessList.Items))
	}
	bucketaccess := bucketAccessList.Items[0]

	bucketaccessrequest2, err := client.ObjectstorageV1alpha1().BucketAccessRequests(bucketaccessrequest.Namespace).Get(ctx, bucketaccessrequest.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when updating BucketAccessRequest: %v", err)
	}

	if !util.ValidateBucketAccess(bucketaccess, *bucketaccessrequest, *bucketaccessclass) {
		t.Fatalf("Failed to compare the resulting BucketAccess with the BucketAccessRequest %v and BucketAccessClass %v", bucketaccessrequest, bucketaccessclass)
	}

	//peform delete and see if the bucketAccessRequest can be deleted
	err = client.ObjectstorageV1alpha1().BucketAccessRequests(bucketaccessrequest2.Namespace).Delete(ctx, bucketaccessrequest2.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Error occurred when deleting BucketAccessRequest: %v", err)
	}

	// force update for the finalizer
	old := bucketaccessrequest
	now := metav1.Now()
	bucketaccessrequest2.ObjectMeta.DeletionTimestamp = &now
	listener.Update(ctx, old, bucketaccessrequest2)

	// there should not be a corresponding BucketAccess
	bucketAccessList = util.GetBucketAccesses(ctx, client, 0)
	if len(bucketAccessList.Items) > 0 {
		t.Fatalf("Expecting BucketAccess object be deleted but found %v", bucketAccessList.Items)
	}
}

func runDeleteBucketAccessRequestIdempotency(t *testing.T, name string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := bucketclientset.NewSimpleClientset()
	kubeClient := fake.NewSimpleClientset()

	listener := NewListener()
	listener.InitializeKubeClient(kubeClient)
	listener.InitializeBucketClient(client)

	_, err := kubeClient.CoreV1().ServiceAccounts(sa1.Namespace).Create(ctx, &sa1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error occurred when creating ServiceAccount: %v", err)
	}
	defer kubeClient.CoreV1().ServiceAccounts(sa1.Namespace).Delete(ctx, sa1.Name, metav1.DeleteOptions{})

	_, err = kubeClient.CoreV1().ConfigMaps(cosiConfigMap.Namespace).Create(ctx, &cosiConfigMap, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error occurred when creating ConfigMap: %v", err)
	}
	defer kubeClient.CoreV1().ConfigMaps(cosiConfigMap.Namespace).Delete(ctx, cosiConfigMap.Name, metav1.DeleteOptions{})

	bucketaccessclass, err := util.CreateBucketAccessClass(ctx, client, &goldAccessClass)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketAccessClass: %v", err)
	}

	bucketrequest, err := util.CreateBucketRequest(ctx, client, &bucketRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketRequest: %v", err)
	}

	bucketaccessrequest, err := util.CreateBucketAccessRequest(ctx, client, &bucketAccessRequest1)
	if err != nil {
		t.Fatalf("Error occurred when creating BucketAccessRequest: %v", err)
	}

	listener.Add(ctx, bucketaccessrequest)

	bucketAccessList := util.GetBucketAccesses(ctx, client, 1)
	defer util.DeleteObjects(ctx, client, *bucketrequest, *bucketaccessrequest, *bucketaccessclass, bucketAccessList.Items)

	if len(bucketAccessList.Items) != 1 {
		t.Fatalf("Expecting a single BucketAccess created but found %v", len(bucketAccessList.Items))
	}
	bucketaccess := bucketAccessList.Items[0]

	bucketaccessrequest2, err := client.ObjectstorageV1alpha1().BucketAccessRequests(bucketaccessrequest.Namespace).Get(ctx, bucketaccessrequest.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error occurred when updating BucketAccessRequest: %v", err)
	}

	if !util.ValidateBucketAccess(bucketaccess, *bucketaccessrequest, *bucketaccessclass) {
		t.Fatalf("Failed to compare the resulting BucketAccess with the BucketAccessRequest %v and BucketAccessClass %v", bucketaccessrequest, bucketaccessclass)
	}

	//peform delete and see if the bucketAccessRequest can be deleted
	err = client.ObjectstorageV1alpha1().BucketAccessRequests(bucketaccessrequest2.Namespace).Delete(ctx, bucketaccessrequest2.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Error occurred when deleting BucketAccessRequest: %v", err)
	}

	// force update for the finalizer
	old := bucketaccessrequest
	now := metav1.Now()
	bucketaccessrequest2.ObjectMeta.DeletionTimestamp = &now
	listener.Update(ctx, old, bucketaccessrequest2)

	//there should not be a corresponding BucketAccess
	bucketAccessList = util.GetBucketAccesses(ctx, client, 0)
	if len(bucketAccessList.Items) > 0 {
		t.Fatalf("Expecting BucketAccess object be deleted but found %v", bucketAccessList.Items)
	}

	//Create a duplicate update
	listener.Update(ctx, old, bucketaccessrequest2)
	//there should not be a corresponding BucketAccess
	bucketAccessList = util.GetBucketAccesses(ctx, client, 0)
	if len(bucketAccessList.Items) > 0 {
		t.Fatalf("Expecting BucketAccess object be deleted but found %v", bucketAccessList.Items)
	}

	// call the delete directly the second time
	listener.Delete(ctx, bucketaccessrequest)
	bucketAccessList = util.GetBucketAccesses(ctx, client, 0)
	if len(bucketAccessList.Items) != 0 {
		t.Fatalf("Expecting a single BucketAccess created but found %v", len(bucketAccessList.Items))
	}
}
