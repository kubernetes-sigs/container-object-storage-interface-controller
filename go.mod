module github.com/kubernetes-sigs/container-object-storage-interface-controller

go 1.15

require (
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/kubernetes-sigs/container-object-storage-interface-api v0.0.0-20201210173615-0c3244fa34b9
	github.com/spf13/cobra v1.1.1
	github.com/spf13/viper v1.7.1
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	k8s.io/api v0.19.4
	k8s.io/apiextensions-apiserver v0.19.4
	k8s.io/apimachinery v0.19.4
	k8s.io/client-go v0.19.4
	sigs.k8s.io/controller-tools v0.4.1
)
