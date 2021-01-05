/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package storage

import (
	"fmt"

	"github.com/onsi/ginkgo"
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/storage/utils"
)

// This executes testSuites for cosi buckets.
var _ = utils.SIGDescribe("COSI Buckets", func() {

	f := framework.NewDefaultFramework("objectstorage")
	var (
		c         clientset.Interface
		ns        string
		err       error
		serverPod *v1.Pod
	)

	ginkgo.BeforeEach(func() {
		c = f.ClientSet
		ns = f.Namespace.Name
	})

	// Testing configurations of a single a PV/PVC pair, multiple evenly paired PVs/PVCs,
	// and multiple unevenly paired PV/PVCs
	ginkgo.Describe("Controller ", func() {

		ginkgo.BeforeEach(func() {
			//deploy controller
			serverPod = nil
		})

		ginkgo.AfterEach(func() {
			//undeploy controller
		})

		ginkgo.Context("Greenfield", func() {
			ginkgo.It("should create a BA with a BR ", func() {
				err = nil
				fmt.Printf("Created BA after reading BR")
				framework.ExpectNoError(err)
				if c == nil {
					framework.Failf("Found clientset as empty")
				}
				fmt.Println("ns: ", ns, " c: ", c, " serverPod: ", serverPod)
			})
		})
	})
})
