package util

import (
	"errors"
)

const (
	BucketClaimFinalizer = "cosi.objectstorage.k8s.io/bucketclaim-protection"
)

var (
	// Error codes that the central controller will return
	ErrBucketAlreadyExists        = errors.New("A bucket already existing that matches the bucket request")
	ErrInvalidBucketClass         = errors.New("Cannot find bucket class with the name specified in the bucket request")
	ErrBucketAccessAlreadyExists  = errors.New("A bucket access already existing that matches the bucket access request")
	ErrInvalidBucketAccessClass   = errors.New("Cannot find bucket access class with the name specified in the bucket access request")
	ErrInvalidBucketRequest       = errors.New("Invalid bucket request specified in the bucket access request")
	ErrInvalidBucketAccessRequest = errors.New("Invalid bucket access request specified")
	ErrWaitForBucketProvisioning  = errors.New("Bucket instance specified in the bucket request is not available to provision bucket access")
	ErrBCUnavailable              = errors.New("BucketClass is not available")
	ErrNotImplemented             = errors.New("Operation Not Implemented")
	ErrNilConfigMap               = errors.New("ConfigMap cannot be nil")
)