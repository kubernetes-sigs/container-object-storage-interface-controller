/*
Copyright 2018 The Kubernetes Authors.
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

package e2e

import (
	"k8s.io/kubernetes/test/e2e/framework/ginkgowrapper"
	"testing"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/klog"

	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	"github.com/onsi/gomega"
)

// RunE2ETests checks configuration parameters (specified through flags) and then runs
// E2E tests using the Ginkgo runner.
// This function is called on each Ginkgo node in parallel mode.
func RunE2ETests(t *testing.T) {
	gomega.RegisterFailHandler(ginkgowrapper.Fail)
	klog.Infof("Starting e2e run %q on Ginkgo node %d", uuid.NewUUID(), config.GinkgoConfig.ParallelNode)
	ginkgo.RunSpecs(t, "ObjectStorage e2e suite")
}
