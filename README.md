# Container Object Storage Controller

Container Object Storage Interface (COSI) controller is responsible to manage lifecycle of COSI objects.
Specifically, this controller monitors the lifecycle of the user-facing CRDs:

- BucketRequest - Represents a request to provision a Bucket
- BucketAccessRequest - Represents a request to access a Bucket

and generates the associated CRDs:

- Bucket - Represents a Bucket or its equivalent in the storage backend
- BucketAccess - Represents a access token or service account in the storage backend

## Developer Guide

Before diving into the code of this repo, we suggest that you familiarize yourself with:

- The Spec of CRDs in [objectstorage.k8s.io/v1alpha1/types.go](https://github.com/kubernetes-sigs/container-object-storage-interface-api/blob/master/apis/objectstorage.k8s.io/v1alpha1/types.go)
- The Spec of the COSI objects [sigs.k8s.io/container-object-storage-interface-spec](https://github.com/kubernetes-sigs/container-object-storage-interface-spec)

A good starting point towards understanding the functionality of this repo would be to study the tests:

- [BucketRequest Test](./pkg/bucketrequest/bucketrequest_test.go)
- [BucketAccessRequest Test](./pkg/bucketaccessrequest/bucketaccessrequest_test.go)

### Build and Test

In order to build and generate a Docker image execute:
```bash
make container
```

In order to run the tests execute:
```bash
make test
```

## References

 - [Documentation](https://container-object-storage-interface.github.io/)
 - [Deployment Guide](https://container-object-storage-interface.github.io/docs/deployment-guide)
 - [Weekly Meetings](https://container-object-storage-interface.github.io/docs/community/weekly-meetings)
 - [Roadmap](https://github.com/orgs/kubernetes-sigs/projects/8)
## Community, discussion, contribution, and support

You can reach the maintainers of this project at:

 - [#sig-storage-cosi](https://kubernetes.slack.com/messages/sig-storage-cosi) slack channel
 - [container-object-storage-interface](https://groups.google.com/g/container-object-storage-interface-wg?pli=1) mailing list
### Code of conduct

Participation in the Kubernetes community is governed by the [Kubernetes Code of Conduct](code-of-conduct.md).
