# Deployment guide for Container Object Storage Interface (COSI) Controller On Kubernetes

This document describes steps for Kubernetes administrators to setup Container Object Storage Interface Controller (COSI) Controller onto a Kubernetes cluster.

COSI controller can be setup using the [kustomization file](https://github.com/kubernetes-sigs/container-object-storage-interface-controller/blob/master/kustomization.yaml) from the [container-object-storage-interface-controller](https://github.com/kubernetes-sigs/container-object-storage-interface-controller) repository with following command:

```sh
kubectl create -k github.com/kubernetes-sigs/container-object-storage-interface-controller
```
The output should look like the following:
```sh
serviceaccount/objectstorage-controller-sa created
role.rbac.authorization.k8s.io/objectstorage-controller created
clusterrole.rbac.authorization.k8s.io/objectstorage-controller-role created
rolebinding.rbac.authorization.k8s.io/objectstorage-controller created
clusterrolebinding.rbac.authorization.k8s.io/objectstorage-controller created
deployment.apps/objectstorage-controller created
```

The controller will be deployed in the `default` namespace.

