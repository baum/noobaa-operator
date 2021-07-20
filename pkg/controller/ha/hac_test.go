package hac_test

import (
	"context"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

var _ = Describe("High Availability (HA) integration test", func() {
	// Expect a 5nodes KIND cluster,
	// see .travis/install-5nodes-kind-cluster.sh
	const (
		nodesNum = 5
	)

	// HAC test variables and functions
	var (
		ctx            = context.Background() // TODO context ;)
		nodes          *v1.NodeList           // Initial cluster nodes list
		pods           *v1.PodList            // NooBaa pods running in the cluster
		nodeToKill     *string                // Node selected to be killed
		podsToEvictMap = map[string]bool{}    // Set of NooBaa pods expected
		                                      // to be evicted as a result of test
		err            error                  // Test err

		podByPrefix = func(pref string) *v1.Pod {
			for _, p := range pods.Items {
				if strings.HasPrefix(p.Name, pref) {
					return &p
				}
			}
			return nil
		}

		nodeByPodPrefix = func(pref string) string {
			if p := podByPrefix(pref); p != nil {
				return p.Spec.NodeName
			}
			return ""
		}
	)

	// - Verify the integratation test cluster environment
	// - Choose a cluster node to stop
	// - Calculate the set of NooBaa pods to be evicted
	Context("Verify K8S cluster", func() {

		// Verify 5 nodes cluster
		Specify("Require 5 node cluster", func() {
			nodes, err = clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			Expect(err).ToNot(HaveOccurred())
			Expect(nodes).ToNot(BeNil())

			for _, n := range nodes.Items {
				logger.Printf("found node %q", n.Name)
			}

			Expect(len(nodes.Items)).To(BeIdenticalTo(nodesNum), "kind 5 noodes cluster is expected")
		})

		// Verify NooBaa installation
		Specify("Require NooBaa pods", func() {
			labelOption := metav1.ListOptions{LabelSelector: "app=noobaa"}
			pods, err = clientset.CoreV1().Pods(metav1.NamespaceDefault).List(ctx, labelOption)
			Expect(err).ToNot(HaveOccurred())
			Expect(pods).ToNot(BeNil())

			for _, p := range pods.Items {
				logger.Printf("found NooBaa pod %v on node %v", p.Name, p.Spec.NodeName)
			}

			Expect(len(pods.Items)).ToNot(BeIdenticalTo(0))
		})

		// Select a node to stop and
		// calculate list of NooBaa pods expected to be evicted
		Specify("Require a node to kill", func() {

			// Select a node to stop such as:
			// - populated by NooBaa pod
			// - operator not running on this node
			podsToEvictPrefix := []string{
				"noobaa-core",
				"noobaa-endpoint",
				"noobaa-" + metav1.NamespaceDefault + "-backing-store",
			}
			operatorPref := "noobaa-operator"
			for _, pref := range podsToEvictPrefix {
				candidateNode := nodeByPodPrefix(pref)
				if len(candidateNode) > 0 && nodeByPodPrefix(operatorPref) != candidateNode {
					nodeToKill = &candidateNode
				}
			}
			Expect(nodeToKill).ToNot(BeNil())
			Expect(len(*nodeToKill) > 0).To(BeTrue())

			logger.Printf("node to kill %q", *nodeToKill)

			// Calculate a set of NooBaa pods
			// expected to be deleted by the operator
			listOption := metav1.ListOptions{LabelSelector: "app=noobaa", FieldSelector: "spec.nodeName=" + (*nodeToKill)}
			podsToEvict, err := clientset.CoreV1().Pods(metav1.NamespaceDefault).List(ctx, listOption)
			Expect(err).ToNot(HaveOccurred())
			Expect(podsToEvict).ToNot(BeNil())

			logger.Printf("Pods to be evicted")
			for _, p := range podsToEvict.Items {
				logger.Printf("   %q", p.Name)
				podsToEvictMap[p.Name] = true
			}

			Expect(len(podsToEvictMap)).NotTo(BeIdenticalTo(0))
		})
	})

	Context("Pod Failure", func() {
		/*
		podsToDeletePrefix := []string{
			"noobaa-core",
			"noobaa-endpoint",
			"noobaa-" + metav1.NamespaceDefault + "-backing-store",
		}

		listOption := metav1.ListOptions{LabelSelector: "app=noobaa"}
		*/
		Expect(clientset).NotTo(BeNil())
		/*

		noobaaPodWatch, err := clientset.CoreV1().Pods(metav1.NamespaceDefault).Watch(ctx, listOption)

		Expect(err).ToNot(HaveOccurred())
		return
		noobaaCrd := "noobaas.noobaa.io"
		//const noobaaCR = "noobaa"
		opts := metav1.ListOptions{}
		opts.Watch = true
		noobaaCrdWatch, err := clientset.RESTClient().Get().
		                            Namespace(metav1.NamespaceDefault).
		                            Resource(noobaaCrd).
									VersionedParams(&opts, scheme.ParameterCodec).
                                    Watch(ctx)
											
		for _, pref := range podsToDeletePrefix {
			p := podByPrefix(pref)
			Expect(p).NotTo(BeNil())

			var gracePeriod int64 = 0
			deleteOpts := metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}
			err := clientset.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, deleteOpts)
			Expect(err).ToNot(HaveOccurred())

			timeoutDuration := 5 * time.Minute
			testStarted := time.Now()
			timeoutTime := testStarted.Add(timeoutDuration)

			logger.Printf("Waiting for the pod to be deleted %v", p.Name)
			var (
				podDeleted *time.Time
				podCreated *time.Time
				noobaaReady *time.Time
				noobaaNotReady *time.Time
			)

			for timeoutTime.After(time.Now()) {
				select {
				case podEvent := <-noobaaPodWatch.ResultChan():
					if podEvent.Type == watch.Deleted {
						deletedPod := podEvent.Object.(*v1.Pod)
						if deletedPod.Name == p.Name {
							ts := time.Now()
							podDeleted = &ts
							//break
						}
					} else if podEvent.Type == watch.Added {
						createdPod := podEvent.Object.(*v1.Pod)
						if strings.HasPrefix(createdPod.Name, pref) {
							ts := time.Now()
							podCreated = &ts
							//break
						}
					}

				case crEvent := <-noobaaCrdWatch.ResultChan():
					if crEvent.Type != watch.Modified {
						continue
					}
					noobaaCr := crEvent.Object.(*nbv1.NooBaa)
					conditions := noobaaCr.Status.Conditions
					for _, cond := range conditions {
						
						if cond.Status == v1.ConditionTrue {
							if noobaaNotReady == nil {
								continue
							}
							ts := time.Now()
							noobaaReady = &ts
						} else {
							ts := time.Now()
							noobaaNotReady = &ts
						}
					}
				case <-time.After(time.Until(timeoutTime)):
				}
			}
			logger.Printf("pref %v pod name %v", pref, p.Name)

			Expect(podDeleted).NotTo(BeNil())
			Expect(podCreated).NotTo(BeNil())
			logger.Printf("Deleted %v Created %v", *podDeleted, *podCreated)
			if noobaaNotReady != nil {
				Expect(noobaaReady).NotTo(BeNil())
				logger.Printf("NotReady %v Ready %v", *noobaaNotReady, *noobaaReady)
			}
			
		}
		*/
	})

	// Node failure flow:
	// - Initiate a node failue by stopping worker node container
	// - Wait for NooBaa pods eviction from the failing node
	Context("Node Failure", func() {

		// Initiate node failure
		Specify("Require docker stop success", func() {
			Expect(nodeToKill).ToNot(BeNil())
			Expect(len(*nodeToKill) > 0).To(BeTrue())

			cmd := exec.Command("docker", "stop", *nodeToKill)
			err = cmd.Run()
			Expect(err).ToNot(HaveOccurred())
		})

		// Verify NooBaa pods were evicted
		Specify("Require all pods to be deleted", func() {
			Expect(nodeToKill).ToNot(BeNil())
			Expect(len(*nodeToKill) > 0).To(BeTrue())

			listOption := metav1.ListOptions{LabelSelector: "app=noobaa", FieldSelector: "spec.nodeName=" + (*nodeToKill)}
			w, err := clientset.CoreV1().Pods(metav1.NamespaceDefault).Watch(ctx, listOption)
			Expect(err).ToNot(HaveOccurred())

			timeoutDuration := 5 * time.Minute
			timeoutTime := time.Now().Add(timeoutDuration)

			logger.Printf("Waiting for pods to be evicted %v", podsToEvictMap)
			for len(podsToEvictMap) > 0 && timeoutTime.After(time.Now()) {
				select {
				case e := <-w.ResultChan():
					if e.Type != watch.Deleted {
						continue
					}
					pod := e.Object.(*v1.Pod)
					delete(podsToEvictMap, pod.Name)
					logger.Printf("evicted  %q", pod.Name)
				case <-time.After(time.Until(timeoutTime)):
				}
			}

			Expect(len(podsToEvictMap)).To(BeIdenticalTo(0))
		})
	})
})
