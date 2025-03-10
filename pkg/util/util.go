package util

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode"

	monitoringv1 "github.com/coreos/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/hashicorp/vault/api"
	vaultApi "github.com/hashicorp/vault/api"
	obv1 "github.com/kube-object-storage/lib-bucket-provisioner/pkg/apis/objectbucket.io/v1alpha1"
	nbapis "github.com/noobaa/noobaa-operator/v2/pkg/apis"
	nbv1 "github.com/noobaa/noobaa-operator/v2/pkg/apis/noobaa/v1alpha1"
	"github.com/noobaa/noobaa-operator/v2/pkg/bundle"
	routev1 "github.com/openshift/api/route/v1"
	secv1 "github.com/openshift/api/security/v1"
	cloudcredsv1 "github.com/openshift/cloud-credential-operator/pkg/apis/cloudcredential/v1"
	conditionsv1 "github.com/openshift/custom-resource-status/conditions/v1"
	operv1 "github.com/operator-framework/api/pkg/operators/v1alpha1"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

const (
	oAuthWellKnownEndpoint = "https://openshift.default.svc/.well-known/oauth-authorization-server"
	rootSecretPath         = "NOOBAA_ROOT_SECRET_PATH"
	ibmRegion              = "ibm-cloud.kubernetes.io/region"
	vaultCaCert            = "VAULT_CACERT"
	vaultClientCert        = "VAULT_CLIENT_CERT"
	vaultClientKey         = "VAULT_CLIENT_KEY"
	vaultAddr              = "VAULT_ADDR"
	vaultCaPath            = "VAULT_CAPATH"
	vaultBackendPath       = "VAULT_BACKEND_PATH"
	kmsProvider            = "KMS_PROVIDER"
	defaultVaultBackendPath = "secret/"
)

// OAuth2Endpoints holds OAuth2 endpoints information.
type OAuth2Endpoints struct {
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
}

var (
	ctx        = context.TODO()
	log        = logrus.WithContext(ctx)
	lazyConfig *rest.Config
	lazyClient client.Client

	// InsecureHTTPTransport is a global insecure http transport
	InsecureHTTPTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
)

func init() {
	Panic(apiextv1.AddToScheme(scheme.Scheme))
	Panic(nbapis.AddToScheme(scheme.Scheme))
	Panic(obv1.AddToScheme(scheme.Scheme))
	Panic(monitoringv1.AddToScheme(scheme.Scheme))
	Panic(cloudcredsv1.AddToScheme(scheme.Scheme))
	Panic(operv1.AddToScheme(scheme.Scheme))
	Panic(cephv1.AddToScheme(scheme.Scheme))
	Panic(routev1.AddToScheme(scheme.Scheme))
	Panic(secv1.AddToScheme(scheme.Scheme))
	Panic(autoscalingv1.AddToScheme(scheme.Scheme))
}

// KubeConfig loads kubernetes client config from default locations (flags, user dir, etc)
func KubeConfig() *rest.Config {
	if lazyConfig == nil {
		var err error
		lazyConfig, err = config.GetConfig()
		if err != nil {
			log.Fatalf("KubeConfig: %v", err)
		}
	}
	return lazyConfig
}

// MapperProvider creates RESTMapper
func MapperProvider(config *rest.Config) (meta.RESTMapper, error) {
	return meta.NewLazyRESTMapperLoader(func() (meta.RESTMapper, error) {
		dc, err := discovery.NewDiscoveryClientForConfig(config)
		if err != nil {
			return nil, err
		}
		return NewFastRESTMapper(dc, func(g *metav1.APIGroup) bool {
			if g.Name == "" ||
				g.Name == "apps" ||
				g.Name == "noobaa.io" ||
				g.Name == "objectbucket.io" ||
				g.Name == "operator.openshift.io" ||
				g.Name == "route.openshift.io" ||
				g.Name == "cloudcredential.openshift.io" ||
				g.Name == "security.openshift.io" ||
				g.Name == "monitoring.coreos.com" ||
				g.Name == "ceph.rook.io" ||
				g.Name == "autoscaling" ||
				g.Name == "batch" ||
				strings.HasSuffix(g.Name, ".k8s.io") {
				return true
			}
			return false
		}), nil
	}), nil
}

// KubeClient resturns a controller-runtime client
// We use a lazy mapper and a specialized implementation of fast mapper
// in order to avoid lags when running a CLI client to a far away cluster.
func KubeClient() client.Client {
	if lazyClient == nil {
		config := KubeConfig()
		mapper, _ := MapperProvider(config)
		var err error
		lazyClient, err = client.New(config, client.Options{Mapper: mapper, Scheme: scheme.Scheme})
		if err != nil {
			log.Fatalf("KubeClient: %v", err)
		}
	}
	return lazyClient
}

// KubeObject loads a text yaml/json to a kubernets object.
func KubeObject(text string) runtime.Object {
	// Decode text (yaml/json) to kube api object
	deserializer := serializer.NewCodecFactory(scheme.Scheme).UniversalDeserializer()
	obj, group, err := deserializer.Decode([]byte(text), nil, nil)
	// obj, group, err := scheme.Codecs.UniversalDecoder().Decode([]byte(text), nil, nil)
	Panic(err)
	// not sure if really needed, but set it anyway
	obj.GetObjectKind().SetGroupVersionKind(*group)
	SecretResetStringDataFromData(obj)
	return obj
}

// KubeApply will check if the object exists and will create/update accordingly
// and report the object status.
func KubeApply(obj runtime.Object) bool {
	klient := KubeClient()
	objKey := ObjectKey(obj)
	gvk := obj.GetObjectKind().GroupVersionKind()
	clone := obj.DeepCopyObject()
	err := klient.Get(ctx, objKey, clone)
	if err == nil {
		err = klient.Update(ctx, obj)
		if err == nil {
			log.Printf("✅ Updated: %s %q\n", gvk.Kind, objKey.Name)
			return false
		}
	}
	if meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
		log.Printf("❌ CRD Missing: %s %q\n", gvk.Kind, objKey.Name)
		return false
	}
	if errors.IsNotFound(err) {
		err = klient.Create(ctx, obj)
		if err == nil {
			log.Printf("✅ Created: %s %q\n", gvk.Kind, objKey.Name)
			return true
		}
	}
	if errors.IsConflict(err) {
		log.Printf("❌ Conflict: %s %q: %s\n", gvk.Kind, objKey.Name, err)
		return false
	}
	Panic(err)
	return false
}

// KubeCreateSkipExisting will check if the object exists and will create/skip accordingly
// and report the object status.
func KubeCreateSkipExisting(obj runtime.Object) bool {
	klient := KubeClient()
	objKey := ObjectKey(obj)
	gvk := obj.GetObjectKind().GroupVersionKind()
	clone := obj.DeepCopyObject()
	err := klient.Get(ctx, objKey, clone)
	if err == nil {
		log.Printf("✅ Already Exists: %s %q\n", gvk.Kind, objKey.Name)
		return false
	}
	if meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
		log.Printf("❌ CRD Missing: %s %q\n", gvk.Kind, objKey.Name)
		return false
	}
	if errors.IsNotFound(err) {
		err = klient.Create(ctx, obj)
		if err == nil {
			log.Printf("✅ Created: %s %q\n", gvk.Kind, objKey.Name)
			return true
		}
		if errors.IsNotFound(err) {
			log.Printf("❌ Namespace Missing: %s %q: kubectl create ns %s\n",
				gvk.Kind, objKey.Name, objKey.Namespace)
			return false
		}
	}
	if errors.IsConflict(err) {
		log.Printf("❌ Conflict: %s %q: %s\n", gvk.Kind, objKey.Name, err)
		return false
	}
	if errors.IsForbidden(err) {
		log.Printf("❌ Forbidden: %s %q: %s\n", gvk.Kind, objKey.Name, err)
		return false
	}
	if errors.IsInvalid(err) {
		log.Printf("❌ Invalid: %s %q: %s\n", gvk.Kind, objKey.Name, err)
		return false
	}
	Panic(err)
	return false
}

// KubeCreateOptional will check if the object exists and will create/skip accordingly
// It detects the situation of a missing CRD and reports it as an optional feature.
func KubeCreateOptional(obj runtime.Object) bool {
	klient := KubeClient()
	objKey := ObjectKey(obj)
	gvk := obj.GetObjectKind().GroupVersionKind()
	clone := obj.DeepCopyObject()
	err := klient.Get(ctx, objKey, clone)
	if err == nil {
		log.Printf("✅ Already Exists: %s %q\n", gvk.Kind, objKey.Name)
		return false
	}
	if meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
		log.Printf("⬛ (Optional) CRD Unavailable: %s %q\n", gvk.Kind, objKey.Name)
		return false
	}
	if errors.IsNotFound(err) {
		err = klient.Create(ctx, obj)
		if err == nil {
			log.Printf("✅ Created: %s %q\n", gvk.Kind, objKey.Name)
			return true
		}
		if errors.IsNotFound(err) {
			log.Printf("❌ Namespace Missing: %s %q: kubectl create ns %s\n",
				gvk.Kind, objKey.Name, objKey.Namespace)
			return false
		}
	}
	if errors.IsConflict(err) {
		log.Printf("❌ Conflict: %s %q: %s\n", gvk.Kind, objKey.Name, err)
		return false
	}
	if errors.IsForbidden(err) {
		log.Printf("❌ Forbidden: %s %q: %s\n", gvk.Kind, objKey.Name, err)
		return false
	}
	if errors.IsInvalid(err) {
		log.Printf("❌ Invalid: %s %q: %s\n", gvk.Kind, objKey.Name, err)
		return false
	}
	Panic(err)
	return false
}

// KubeDelete deletes an object and reports the object status.
func KubeDelete(obj runtime.Object, opts ...client.DeleteOption) bool {
	klient := KubeClient()
	objKey := ObjectKey(obj)
	gvk := obj.GetObjectKind().GroupVersionKind()
	deleted := false
	conflicted := false

	err := klient.Delete(ctx, obj, opts...)
	if err == nil {
		deleted = true
		log.Printf("🗑️  Deleting: %s %q\n", gvk.Kind, objKey.Name)
	} else if errors.IsConflict(err) {
		conflicted = true
		log.Printf("🗑️  Conflict (OK): %s %q: %s\n", gvk.Kind, objKey.Name, err)
	} else if errors.IsNotFound(err) {
		return true
	}

	time.Sleep(10 * time.Millisecond)

	err = wait.PollImmediateInfinite(time.Second, func() (bool, error) {
		err := klient.Delete(ctx, obj, opts...)
		if err == nil {
			if !deleted {
				deleted = true
				log.Printf("🗑️  Deleting: %s %q\n", gvk.Kind, objKey.Name)
			}
			return false, nil
		}
		if errors.IsConflict(err) {
			if !conflicted {
				conflicted = true
				log.Printf("🗑️  Conflict (OK): %s %q: %s\n", gvk.Kind, objKey.Name, err)
			}
			return false, nil
		}
		if meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
			log.Printf("🗑️  CRD Missing (OK): %s %q\n", gvk.Kind, objKey.Name)
			return true, nil
		}
		if errors.IsNotFound(err) {
			log.Printf("🗑️  Deleted : %s %q\n", gvk.Kind, objKey.Name)
			return true, nil
		}
		return false, err
	})
	Panic(err)
	return deleted
}

// KubeDeleteAllOf deletes an list of objects and reports the status.
func KubeDeleteAllOf(obj runtime.Object, opts ...client.DeleteAllOfOption) bool {
	klient := KubeClient()
	gvk := obj.GetObjectKind().GroupVersionKind()
	deleted := false

	err := klient.DeleteAllOf(ctx, obj, opts...)
	if err == nil {
		deleted = true
		log.Printf("🗑️  Deleting All of type: %s\n", gvk.Kind)
	} else if errors.IsNotFound(err) {
		return true
	}
	Panic(err)
	return deleted
}

// KubeUpdate updates an object and reports the object status.
func KubeUpdate(obj runtime.Object) bool {
	klient := KubeClient()
	objKey := ObjectKey(obj)
	gvk := obj.GetObjectKind().GroupVersionKind()
	err := klient.Update(ctx, obj)
	if err == nil {
		log.Printf("✅ Updated: %s %q\n", gvk.Kind, objKey.Name)
		return true
	}
	if meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
		log.Printf("❌ CRD Missing: %s %q\n", gvk.Kind, objKey.Name)
		return false
	}
	if errors.IsConflict(err) {
		log.Printf("❌ Conflict: %s %q: %s\n", gvk.Kind, objKey.Name, err)
		return false
	}
	if errors.IsNotFound(err) {
		log.Printf("❌ Not Found: %s %q\n", gvk.Kind, objKey.Name)
		return false
	}
	Panic(err)
	return false
}

// KubeCheck checks if the object exists and reports the object status.
func KubeCheck(obj runtime.Object) bool {
	name, kind, err := KubeGet(obj)
	if err == nil {
		SecretResetStringDataFromData(obj)
		log.Printf("✅ Exists: %s %q\n", kind, name)
		return true
	}
	if meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
		log.Printf("❌ CRD Missing: %s %q\n", kind, name)
		return false
	}
	if errors.IsNotFound(err) {
		log.Printf("❌ Not Found: %s %q\n", kind, name)
		return false
	}
	if errors.IsConflict(err) {
		log.Printf("❌ Conflict: %s %q: %s\n", kind, name, err)
		return false
	}
	Panic(err)
	return false
}

// KubeCheckOptional checks if the object exists and reports the object status.
// It detects the situation of a missing CRD and reports it as an optional feature.
func KubeCheckOptional(obj runtime.Object) bool {
	name, kind, err := KubeGet(obj)
	if err == nil {
		log.Printf("✅ (Optional) Exists: %s %q\n", kind, name)
		return true
	}
	if meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
		log.Printf("⬛ (Optional) CRD Unavailable: %s %q\n", kind, name)
		return false
	}
	if errors.IsNotFound(err) {
		log.Printf("⬛ (Optional) Not Found: %s %q\n", kind, name)
		return false
	}
	if errors.IsConflict(err) {
		log.Printf("❌ (Optional) Conflict: %s %q: %s\n", kind, name, err)
		return false
	}
	Panic(err)
	return false
}

// KubeCheckQuiet checks if the object exists fills the given object if found.
// returns true if the object was found. It does not print any status
func KubeCheckQuiet(obj runtime.Object) bool {
	_, _, err := KubeGet(obj)
	if err == nil {
		return true
	}
	if meta.IsNoMatchError(err) ||
		runtime.IsNotRegisteredError(err) ||
		errors.IsNotFound(err) ||
		errors.IsConflict(err) {
		return false
	}
	Panic(err)
	return false
}

// KubeGet gets a runtime.Object, fills the given object and returns the name and kind
// returns error on failure
func KubeGet(obj runtime.Object) (name string, kind string, err error) {
	klient := KubeClient()
	objKey := ObjectKey(obj)
	name = objKey.Name
	gvk := obj.GetObjectKind().GroupVersionKind()
	kind = gvk.Kind
	err = klient.Get(ctx, objKey, obj)
	return name, kind, err
}

// KubeList returns a list of objects.
func KubeList(list runtime.Object, options ...client.ListOption) bool {
	klient := KubeClient()
	gvk := list.GetObjectKind().GroupVersionKind()
	err := klient.List(ctx, list, options...)
	if err == nil {
		return true
	}
	if meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
		log.Printf("❌ CRD Missing: %s: %s\n", gvk.Kind, err)
		return false
	}
	if errors.IsNotFound(err) {
		log.Printf("❌ Not Found: %s\n", gvk.Kind)
		return false
	}
	if errors.IsConflict(err) {
		log.Printf("❌ Conflict: %s: %s\n", gvk.Kind, err)
		return false
	}
	Panic(err)
	return false
}

// RemoveFinalizer modifies the object and removes the finalizer
func RemoveFinalizer(obj metav1.Object, finalizer string) bool {
	finalizers := obj.GetFinalizers()
	if finalizers == nil {
		return false
	}
	// see https://github.com/golang/go/wiki/SliceTricks#filter-in-place
	n := 0
	found := false
	for _, f := range finalizers {
		if f == finalizer {
			found = true
		} else {
			finalizers[n] = f
			n++
		}
	}
	finalizers = finalizers[:n]
	obj.SetFinalizers(finalizers)
	return found
}

// AddFinalizer adds the finalizer to the object if it doesn't contains it already
func AddFinalizer(obj metav1.Object, finalizer string) bool {
	if !Contains(finalizer, obj.GetFinalizers()) {
		finalizers := append(obj.GetFinalizers(), finalizer)
		obj.SetFinalizers(finalizers)
		return true
	}
	return false
}

// GetPodStatusLine returns a one liner status for a pod
func GetPodStatusLine(pod *corev1.Pod) string {
	s := fmt.Sprintf("Phase=%q. ", pod.Status.Phase)
	for i := range pod.Status.Conditions {
		c := &pod.Status.Conditions[i]
		if c.Status != corev1.ConditionTrue {
			s += fmt.Sprintf("%s (%s). ", c.Reason, c.Message)
		}
	}
	return s
}

// GetPodLogs info
func GetPodLogs(pod corev1.Pod) (map[string]io.ReadCloser, error) {
	allContainers := append(pod.Spec.InitContainers, pod.Spec.Containers...)
	config := KubeConfig()
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Printf(`Could not create %s, reason: %s\n`, pod.Name, err)
		return nil, err
	}

	containerMap := make(map[string]io.ReadCloser)
	for _, container := range allContainers {
		podLogOpts := corev1.PodLogOptions{Container: container.Name}
		req := clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &podLogOpts)
		podLogs, err := req.Stream(ctx)
		if err != nil {
			log.Printf(`Could not read logs %s container %s, reason: %s\n`, pod.Name, container.Name, err)
			continue
		}
		containerMap[container.Name] = podLogs
	}
	return containerMap, nil
}

// SaveStreamToFile info
func SaveStreamToFile(body io.ReadCloser, path string) error {
	if body == nil {
		return nil
	}
	defer body.Close()
	f, err := os.Create(path)
	if err != nil {
		log.Errorf(`Could not save stream to file: %s, reason: %s\n`, path, err)
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, body); err != nil {
		log.Errorf(`Could not write to file: %s, reason: %s\n`, path, err)
		return err
	}
	return nil
}

// SaveCRsToFile info
func SaveCRsToFile(crs runtime.Object, path string) error {
	if crs == nil {
		return nil
	}

	f, err := os.Create(path)
	if err != nil {
		log.Printf(`Could not create file %s, reason: %s\n`, path, err)
		return err
	}

	defer f.Close()
	p := printers.YAMLPrinter{}
	err = p.PrintObj(crs, f)
	if err != nil {
		log.Printf(`Could not write yaml to file %s, reason: %s\n`, path, err)
		return err
	}

	return nil
}

// GetContainerStatusLine returns a one liner status for a container
func GetContainerStatusLine(cont *corev1.ContainerStatus) string {
	s := ""
	if cont.RestartCount > 0 {
		s += fmt.Sprintf("RestartCount=%d. ", cont.RestartCount)
	}
	if cont.State.Waiting != nil {
		s += fmt.Sprintf("%s (%s). ", cont.State.Waiting.Reason, cont.State.Waiting.Message)
	}
	if cont.State.Terminated != nil {
		s += fmt.Sprintf("%s (%s). ", cont.State.Terminated.Reason, cont.State.Terminated.Message)
	}
	if s == "" {
		s = "starting..."
	}
	return s
}

// Panic is conviniently calling panic only if err is not nil
func Panic(err error) {
	if err != nil {
		reason := errors.ReasonForError(err)
		log.Panicf("☠️  Panic Attack: [%s] %s", reason, err)
	}
}

// LogError prints the error to the log and continue
func LogError(err error) {
	if err != nil {
		log.Errorf("Error discovered: %s", err)
	}
}

// IgnoreError do nothing if err is not nil
func IgnoreError(err error) {
}

// InitLogger initializes the logrus logger with defaults
func InitLogger() {
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetFormatter(&logrus.TextFormatter{
		// FullTimestamp: true,
	})
}

// Logger returns a default logger
func Logger() *logrus.Entry {
	return log
}

// Context returns a default Context
func Context() context.Context {
	return ctx
}

// CurrentNamespace reads the current namespace from the kube config
func CurrentNamespace() string {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	ns, _, _ := kubeConfig.Namespace()
	// ignoring errors and just return empty string if config is missing or invalid
	return ns
}

// PersistentError is an error type that tells the reconcile to avoid requeueing.
type PersistentError struct {
	Reason  string
	Message string
}

// Error function makes PersistentError implement error interface
func (e *PersistentError) Error() string { return e.Message }

// assert implement error interface
var _ error = &PersistentError{}

// NewPersistentError returns a new persistent error.
func NewPersistentError(reason string, message string) *PersistentError {
	return &PersistentError{Reason: reason, Message: message}
}

// IsPersistentError checks if the provided error is persistent.
func IsPersistentError(err error) bool {
	_, isPersistent := err.(*PersistentError)
	return isPersistent
}

// CombineErrors takes a list of errors and combines them to one.
// Generally it will return the first non-nil error,
// but if a persistent error is found it will be returned
// instead of non-persistent errors.
func CombineErrors(errs ...error) error {
	combined := error(nil)
	for _, err := range errs {
		if err == nil {
			continue
		}
		if combined == nil {
			combined = err
			continue
		}
		if IsPersistentError(err) && !IsPersistentError(combined) {
			combined = err
		}
	}
	return combined
}

// SecretResetStringDataFromData reads the secret data into string data
// to streamline the paths that use the secret values as strings.
func SecretResetStringDataFromData(obj runtime.Object) {
	secret, isSecret := obj.(*corev1.Secret)
	if !isSecret {
		return
	}
	secret.StringData = map[string]string{}
	for key, val := range secret.Data {
		secret.StringData[key] = string(val)
	}
	secret.Data = map[string][]byte{}
}

// ObjectKey returns the objects key (namespace + name)
func ObjectKey(obj runtime.Object) client.ObjectKey {
	objKey, err := client.ObjectKeyFromObject(obj)
	Panic(err)
	return objKey
}

// RandomBase64 creates a random buffer with numBytes and returns it encoded in base64
// Returned string length is 4*math.Ceil(numBytes/3)
func RandomBase64(numBytes int) string {
	randomBytes := make([]byte, numBytes)
	_, err := rand.Read(randomBytes)
	Panic(err)
	return base64.StdEncoding.EncodeToString(randomBytes)
}

// RandomHex creates a random buffer with numBytes and returns it encoded in hex
// Returned string length is 2*numBytes
func RandomHex(numBytes int) string {
	randomBytes := make([]byte, numBytes)
	_, err := rand.Read(randomBytes)
	Panic(err)
	return hex.EncodeToString(randomBytes)
}

// SetAvailableCondition updates the status conditions to available state
func SetAvailableCondition(conditions *[]conditionsv1.Condition, reason string, message string) {
	currentTime := metav1.NewTime(time.Now())
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionAvailable,
		Status:            corev1.ConditionTrue,
		Reason:            reason,
		Message:           message,
	})
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionProgressing,
		Status:            corev1.ConditionFalse,
		Reason:            reason,
		Message:           message,
	})
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionDegraded,
		Status:            corev1.ConditionFalse,
		Reason:            reason,
		Message:           message,
	})
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionUpgradeable,
		Status:            corev1.ConditionTrue,
		Reason:            reason,
		Message:           message,
	})
}

// SetProgressingCondition updates the status conditions to in-progress state
func SetProgressingCondition(conditions *[]conditionsv1.Condition, reason string, message string) {
	currentTime := metav1.NewTime(time.Now())
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionAvailable,
		Status:            corev1.ConditionFalse,
		Reason:            reason,
		Message:           message,
	})
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionProgressing,
		Status:            corev1.ConditionTrue,
		Reason:            reason,
		Message:           message,
	})
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionDegraded,
		Status:            corev1.ConditionFalse,
		Reason:            reason,
		Message:           message,
	})
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionUpgradeable,
		Status:            corev1.ConditionFalse,
		Reason:            reason,
		Message:           message,
	})
}

// SetErrorCondition updates the status conditions to error state
func SetErrorCondition(conditions *[]conditionsv1.Condition, reason string, message string) {
	currentTime := metav1.NewTime(time.Now())
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		//LastHeartbeatTime should be set by the custom-resource-status just like lastTransitionTime
		// Setting it here temporarity
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionAvailable,
		Status:            corev1.ConditionUnknown,
		Reason:            reason,
		Message:           message,
	})
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionProgressing,
		Status:            corev1.ConditionFalse,
		Reason:            reason,
		Message:           message,
	})
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionDegraded,
		Status:            corev1.ConditionTrue,
		Reason:            reason,
		Message:           message,
	})
	conditionsv1.SetStatusCondition(conditions, conditionsv1.Condition{
		LastHeartbeatTime: currentTime,
		Type:              conditionsv1.ConditionUpgradeable,
		Status:            corev1.ConditionUnknown,
		Reason:            reason,
		Message:           message,
	})
}

// IsAWSPlatform returns true if this cluster is running on AWS
func IsAWSPlatform() bool {
	nodesList := &corev1.NodeList{}
	if ok := KubeList(nodesList); !ok || len(nodesList.Items) == 0 {
		Panic(fmt.Errorf("failed to list kubernetes nodes"))
	}
	isAWS := strings.HasPrefix(nodesList.Items[0].Spec.ProviderID, "aws")
	return isAWS
}

// IsAzurePlatform returns true if this cluster is running on Azure
func IsAzurePlatform() bool {
	nodesList := &corev1.NodeList{}
	if ok := KubeList(nodesList); !ok || len(nodesList.Items) == 0 {
		Panic(fmt.Errorf("failed to list kubernetes nodes"))
	}
	isAzure := strings.HasPrefix(nodesList.Items[0].Spec.ProviderID, "azure")
	return isAzure
}

// IsGCPPlatform returns true if this cluster is running on GCP
func IsGCPPlatform() bool {
	nodesList := &corev1.NodeList{}
	if ok := KubeList(nodesList); !ok || len(nodesList.Items) == 0 {
		Panic(fmt.Errorf("failed to list kubernetes nodes"))
	}
	isGCP := strings.HasPrefix(nodesList.Items[0].Spec.ProviderID, "gce")
	return isGCP
}

// IsIBMPlatform returns true if this cluster is running on IBM Cloud
func IsIBMPlatform() bool {
	nodesList := &corev1.NodeList{}
	if ok := KubeList(nodesList); !ok || len(nodesList.Items) == 0 {
		Panic(fmt.Errorf("failed to list kubernetes nodes"))
	}
	isIBM := strings.HasPrefix(nodesList.Items[0].Spec.ProviderID, "ibm")
	if isIBM {
		// Incase of Satellite cluster is deplyed in user provided infrastructure
		if strings.Contains(nodesList.Items[0].Spec.ProviderID, "/sat-") {
			isIBM = false
		}
	}
	return isIBM
}

// GetIBMRegion returns the cluster's region in IBM Cloud
func GetIBMRegion() (string, error) {
	nodesList := &corev1.NodeList{}
	if ok := KubeList(nodesList); !ok || len(nodesList.Items) == 0 {
		return "", fmt.Errorf("failed to list kubernetes nodes")
	}
	labels := nodesList.Items[0].GetLabels()
	region := labels[ibmRegion]
	return region, nil
}

// GetAWSRegion parses the region from a node's name
func GetAWSRegion() (string, error) {
	// parse the node name to get AWS region according to this:
	// https://docs.aws.amazon.com/en_pv/vpc/latest/userguide/vpc-dns.html#vpc-dns-hostnames
	var mapValidAWSRegions = map[string]string{
		"compute-1":      "us-east-1",
		"ec2":            "us-east-1",
		"us-east-1":      "us-east-1",
		"us-east-2":      "us-east-2",
		"us-west-1":      "us-west-1",
		"us-west-2":      "us-west-2",
		"ca-central-1":   "ca-central-1",
		"eu-central-1":   "eu-central-1",
		"eu-west-1":      "eu-west-1",
		"eu-west-2":      "eu-west-2",
		"eu-west-3":      "eu-west-3",
		"eu-north-1":     "eu-north-1",
		"ap-east-1":      "ap-east-1",
		"ap-northeast-1": "ap-northeast-1",
		"ap-northeast-2": "ap-northeast-2",
		"ap-northeast-3": "ap-northeast-3",
		"ap-southeast-1": "ap-southeast-1",
		"ap-southeast-2": "ap-southeast-2",
		"ap-south-1":     "ap-south-1",
		"me-south-1":     "me-south-1",
		"sa-east-1":      "sa-east-1",
		"us-gov-west-1":  "us-gov-west-1",
		"us-gov-east-1":  "us-gov-east-1",
	}
	nodesList := &corev1.NodeList{}
	if ok := KubeList(nodesList); !ok || len(nodesList.Items) == 0 {
		return "", fmt.Errorf("Failed to list kubernetes nodes")
	}
	nameSplit := strings.Split(nodesList.Items[0].Name, ".")
	if len(nameSplit) < 2 {
		return "", fmt.Errorf("Unexpected node name format: %q", nodesList.Items[0].Name)
	}
	awsRegion := mapValidAWSRegions[nameSplit[1]]
	if awsRegion == "" {
		return "", fmt.Errorf("The parsed AWS region is invalid: %q", awsRegion)
	}
	return awsRegion, nil
}

// IsValidS3BucketName checks the name according to
// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
func IsValidS3BucketName(name string) bool {
	validBucketNameRegex := regexp.MustCompile(`^(([a-z0-9]|[a-z0-9][a-z0-9-]*[a-z0-9])\.)*([a-z0-9]|[a-z0-9][a-z0-9-]*[a-z0-9])$`)
	return validBucketNameRegex.MatchString(name)
}

// GetFlagStringOrPrompt returns flag value but if empty will promtp to read from stdin
func GetFlagStringOrPrompt(cmd *cobra.Command, flag string) string {
	str, _ := cmd.Flags().GetString(flag)
	if str != "" {
		return str
	}
	fmt.Printf("Enter %s: ", flag)
	_, err := fmt.Scan(&str)
	Panic(err)
	if str == "" {
		log.Fatalf(`❌ Missing %s %s`, flag, cmd.UsageString())
	}
	return str
}

// GetFlagStringOrPromptPassword is like GetFlagStringOrPrompt
// but does not show the input characters on the terminal
// to avoid leaking secret data in shell history
func GetFlagStringOrPromptPassword(cmd *cobra.Command, flag string) string {
	str, _ := cmd.Flags().GetString(flag)
	if str != "" {
		return str
	}
	fmt.Printf("Enter %s: ", flag)
	bytes, err := terminal.ReadPassword(0)
	Panic(err)
	str = string(bytes)
	fmt.Printf("[got %d characters]\n", len(str))
	return str
}

// PrintThisNoteWhenFinishedApplyingAndStartWaitLoop is a common log task
func PrintThisNoteWhenFinishedApplyingAndStartWaitLoop() {
	log := Logger()
	log.Printf("NOTE:")
	log.Printf("  - This command has finished applying changes to the cluster.")
	log.Printf("  - From now on, it only loops and reads the status, to monitor the operator work.")
	log.Printf("  - You may Ctrl-C at any time to stop the loop and watch it manually.")
}

// DiscoverOAuthEndpoints uses a well known url to get info on the cluster oauth2 endpoints
func DiscoverOAuthEndpoints() (*OAuth2Endpoints, error) {
	client := http.Client{
		Timeout:   120 * time.Second,
		Transport: InsecureHTTPTransport,
	}

	res, err := client.Get(oAuthWellKnownEndpoint)
	defer func() {
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
	}()
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	endpoints := OAuth2Endpoints{}
	err = json.Unmarshal(body, &endpoints)
	if err != nil {
		return nil, err
	}

	return &endpoints, nil
}

// IsStringGraphicOrSpacesCharsOnly returns true only if all the chars are graphic or spaces
func IsStringGraphicOrSpacesCharsOnly(s string) bool {
	for _, c := range s {
		if !unicode.IsGraphic(c) && !unicode.IsSpace(c) {
			return false
		}
	}
	return true
}

// VerifyCredsInSecret throws fatal error when a given secret doesn't contain the mandatory properties
func VerifyCredsInSecret(secretName string, namespace string, mandatoryProperties []string) {
	secret := KubeObject(bundle.File_deploy_internal_secret_empty_yaml).(*corev1.Secret)
	secret.Name = secretName
	secret.Namespace = namespace
	if !KubeCheck(secret) {
		log.Fatalf("secret %q does not exist", secretName)
	}
	for _, p := range mandatoryProperties {
		cred, ok := secret.StringData[p]
		if cred == "" || !ok {
			log.Fatalf("❌ secret %q does not contain property %q", secret.Name, p)
		}
	}
}

// Tar takes a source and variable writers and walks 'source' writing each file
// found to the tar writer; the purpose for accepting multiple writers is to allow
// for multiple outputs (for example a file, or md5 hash)
func Tar(src string, writers ...io.Writer) error {
	// ensure the src actually exists before trying to tar it
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("Unable to tar files - %v", err.Error())
	}

	mw := io.MultiWriter(writers...)

	gzw := gzip.NewWriter(mw)
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	// walk path
	return filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {

		// return on any error
		if err != nil {
			return err
		}

		// return on non-regular files (thanks to [kumo](https://medium.com/@komuw/just-like-you-did-fbdd7df829d3) for this suggested update)
		if !fi.Mode().IsRegular() {
			return nil
		}

		// create a new dir/file header
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}

		// update the name to correctly reflect the desired destination when untaring
		header.Name = strings.TrimPrefix(strings.Replace(file, src, "", -1), string(filepath.Separator))

		// write the header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// open files for taring
		f, err := os.Open(file)
		if err != nil {
			return err
		}

		defer f.Close()

		// copy file data into tar writer
		if _, err := io.Copy(tw, f); err != nil {
			return err
		}

		return nil
	})
}

// WriteYamlFile writes a yaml file from the given objects
func WriteYamlFile(name string, obj runtime.Object, moreObjects ...runtime.Object) error {
	p := printers.YAMLPrinter{}

	file, err := os.Create(name)
	if err != nil {
		return err
	}
	defer file.Close()

	err = p.PrintObj(obj, file)
	if err != nil {
		return err
	}

	for i := range moreObjects {
		err = p.PrintObj(moreObjects[i], file)
		if err != nil {
			return err
		}
	}

	return nil
}

// Contains checks if string array arr contains string s
func Contains(s string, arr []string) bool {
	for _, b := range arr {
		if b == s {
			return true
		}
	}
	return false
}

// GetEnvVariable is looknig for env variable called name in env and return a pointer to the variable
func GetEnvVariable(env *[]corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range *env {
		e := &(*env)[i]
		if e.Name == name {
			return e
		}
	}
	return nil
}

// ReflectEnvVariable will add, update or remove an env variable base on the existence and value of an
// env variable with the same name on the container running this function.
func ReflectEnvVariable(env *[]corev1.EnvVar, name string) {
	if val, ok := os.LookupEnv(name); ok {
		envVar := GetEnvVariable(env, name)
		if envVar != nil {
			envVar.Value = val
		} else {
			*env = append(*env, corev1.EnvVar{
				Name:  name,
				Value: val,
			})
		}
	} else {
		for i, envVar := range *env {
			if envVar.Name == name {
				*env = append((*env)[:i], (*env)[i+1:]...)
				return
			}
		}
	}
}

// MergeEnvArrays takes two Env variables arrays and merge them into the first
func MergeEnvArrays(envA, envB *[]corev1.EnvVar) {
	existingEnvs := make(map[string]bool)

	for _, item := range *envA {
		existingEnvs[item.Name] = true
	}

	for _, item := range *envB {
		if !existingEnvs[item.Name] {
			*envA = append(*envA, item)
		}
	}
}

// EnsureCommonMetaFields ensures that the resource has all mandatory meta fields
func EnsureCommonMetaFields(object metav1.Object, finalizer string) bool {
	updated := false

	labels := object.GetLabels()
	if labels == nil {
		object.SetLabels(map[string]string{
			"app": "noobaa",
		})
		updated = true

	} else if labels["app"] != "noobaa" {
		labels["app"] = "noobaa"
		object.SetLabels(labels)
		updated = true

	}
	if AddFinalizer(object, finalizer) {
		updated = true

	}
	return updated
}

// GetWatchNamespace returns the namespace the operator should be watching for changes
// this was implemented in operator-sdk v0.17 and removed. copied from here:
// https://github.com/operator-framework/operator-sdk/blob/53b00d125fb12515cd74fb169149913b401c8995/pkg/k8sutil/k8sutil.go#L45
func GetWatchNamespace() (string, error) {
	const WatchNamespaceEnvVar = "WATCH_NAMESPACE"
	ns, found := os.LookupEnv(WatchNamespaceEnvVar)
	if !found {
		return "", fmt.Errorf("%s must be set", WatchNamespaceEnvVar)
	}
	if len(ns) == 0 {
		return "", fmt.Errorf("%s must not be empty", WatchNamespaceEnvVar)
	}
	return ns, nil
}

///////////////////////////////////
/////////// VAULT UTILS ///////////
///////////////////////////////////

// VerifyExternalSecretsDeletion checks if noobaa is on un-installation process
// if true, deletes secrets from external KMS
func VerifyExternalSecretsDeletion(kms nbv1.KeyManagementServiceSpec, namespace string, uid string) error {

	if len(kms.ConnectionDetails) == 0 {
		log.Infof("deleting root key locally")
		return nil
	}

	if !IsVaultKMS(kms.ConnectionDetails[kmsProvider]) {
		log.Errorf("Unsupported KMS provider %v", kms.ConnectionDetails[kmsProvider])
		return fmt.Errorf("Unsupported KMS provider %v", kms.ConnectionDetails[kmsProvider])
	}

	c, err := InitVaultClient(kms.ConnectionDetails, kms.TokenSecretName, namespace)
	if err != nil {
		log.Errorf("deleting root key externally failed: init vault client: %v", err)
		return err
	}

	secretPath, err := BuildExternalSecretPath(c, kms, uid)
	if err != nil {
		log.Errorf("deleting root key externally failed: %v", err)
		return err
	}

	err = DeleteSecret(c, secretPath)
	if err != nil {
		log.Errorf("deleting root key externally failed: %v", err)
		return err
	}

	return nil
}

// InitVaultClient inits the secret store
func InitVaultClient(config map[string]string, tokenSecretName string, namespace string) (*vaultApi.Client, error) {
	// set TLS configurations
	vaultClientConfig := vaultApi.DefaultConfig()
	tlsConfig, err := GetVaultTLSConfig(config, namespace)
	if err != nil {
		return nil, fmt.Errorf(`❌ Could not init vault tls config %q in namespace %q`, config, namespace)
	}
	err = vaultClientConfig.ConfigureTLS(tlsConfig)
	if err != nil {
		return nil, fmt.Errorf(`❌ Could not configure tls for vault %q in namespace %q`, config, namespace)
	}
	client, err := vaultApi.NewClient(vaultClientConfig)
	if err != nil {
		return nil, err
	}
	addr := strings.TrimSuffix(config[vaultAddr], "\n")
	err = client.SetAddress(addr)
	if err != nil {
		return nil, err
	}

	secret := KubeObject(bundle.File_deploy_internal_secret_empty_yaml).(*corev1.Secret)
	secret.Namespace = namespace
	secret.Name = tokenSecretName

	if !KubeCheck(secret) {
		return nil, fmt.Errorf(`❌ Could not find secret %q in namespace %q`, secret.Name, secret.Namespace)
	}

	token := secret.StringData["token"]
	trimmedToken := strings.TrimSuffix(token, "\n")
	client.SetToken(trimmedToken)

	// set namespace
	vaultNamespace := config["VAULT_NAMESPACE"]
	if vaultNamespace != "" {
		client.SetNamespace(vaultNamespace)
	}
	return client, nil
}

// GetVaultTLSConfig returns tlsConfig if given
func GetVaultTLSConfig(config map[string]string, namespace string) (*api.TLSConfig, error) {
	tlsConfig := &api.TLSConfig{}
	secret := KubeObject(bundle.File_deploy_internal_secret_empty_yaml).(*corev1.Secret)
	secret.Namespace = namespace

	if tlsServerName := config["VAULT_TLS_SERVER_NAME"]; tlsServerName != "" {
		tlsConfig.TLSServerName = strings.TrimSuffix(tlsServerName, "\n")
	}
	if tlsSkipVerify := config["VAULT_SKIP_VERIFY"]; tlsSkipVerify == "true" {
		tlsConfig.Insecure = true
		err := os.Setenv("VAULT_SKIP_VERIFY", "true")
		if err != nil {
			return nil, fmt.Errorf("can not set env var %v %v", "VAULT_SKIP_VERIFY", "true")
		}
	}

	if caCertSecretName := config[vaultCaCert]; caCertSecretName != "" {
		secret.Name = caCertSecretName
		if !KubeCheckOptional(secret) {
			return nil, fmt.Errorf(`❌ Could not find secret %q in namespace %q`, secret.Name, secret.Namespace)
		}
		caFileAddr, err := writeCrtsToFile(caCertSecretName, namespace, secret.Data["cert"], vaultCaCert)
		if err != nil {
			return nil, fmt.Errorf("can not write crt %v to file %v", vaultCaCert, err)
		}
		tlsConfig.CACert = caFileAddr
	}

	if clientCertSecretName := config[vaultClientCert]; clientCertSecretName != "" {
		secret.Name = clientCertSecretName
		if !KubeCheckOptional(secret) {
			return nil, fmt.Errorf(`❌ Could not find secret %q in namespace %q`, secret.Name, secret.Namespace)
		}
		clientCertFileAddr, err := writeCrtsToFile(clientCertSecretName, namespace, secret.Data["cert"], vaultClientCert)
		if err != nil {
			return nil, fmt.Errorf("can not write crt %v to file %v", vaultClientCert, err)
		}
		tlsConfig.ClientCert = clientCertFileAddr

	}
	if clientKeySecretName := config[vaultClientKey]; clientKeySecretName != "" {
		secret.Name = clientKeySecretName
		if !KubeCheckOptional(secret) {
			return nil, fmt.Errorf(`❌ Could not find secret %q in namespace %q`, secret.Name, secret.Namespace)
		}
		clientKeyFileAddr, err := writeCrtsToFile(clientKeySecretName, namespace, secret.Data["key"], vaultClientKey)
		if err != nil {
			return nil, fmt.Errorf("can not write crt %v to file %v", vaultClientKey, err)
		}
		tlsConfig.ClientKey = clientKeyFileAddr
	}
	return tlsConfig, nil
}

// PutSecret writes the secret to the secrets store
func PutSecret(client *vaultApi.Client, secretName, secretValue, secretPath string, backendPath string) error {

	// Build Secret
	data := make(map[string]interface{})
	v2, err := isKV2(client, backendPath)
	if err != nil {
		return err
	}
	if v2 {
		data["data"] = map[string]interface{}{secretName: secretValue}
	} else {
		data[secretName] = secretValue
	}

	_, err = client.Logical().Write(secretPath, data)
	if err != nil {
		return err
	}
	return nil
}

// GetSecret reads the secret to the secrets store
func GetSecret(client *vaultApi.Client, secretName, secretPath string, backendPath string) (string, error) {
	secret, err := client.Logical().Read(secretPath)
	if err != nil {
		return "", err
	}
	if secret == nil {
		return "", nil
	}
	v2, err := isKV2(client, backendPath)
	if err != nil {
		return "", err
	}
	if v2 {
		rootKey, ok := secret.Data["data"].(map[string]interface{})
		// data property does not exist in secret (in kv2 a secret may exist but have deletion timestamp)
		if !ok {
			return "", nil
		}
		rootKeyData := rootKey[secretName]
		if rootKeyData != nil {
			if rootKeyStr, ok := rootKeyData.(string); ok {
				return rootKeyStr, nil
			}
		}
	} else {
		rootKey := secret.Data[secretName]
		if rootKey != nil {
			if rootKeyStr, ok := rootKey.(string); ok {
				return rootKeyStr, nil
			}
		}
	}
	log.Infof("get secret: found other secrets in external KMS path, but not path: %v, secret name: %v", backendPath, backendPath)
	return "", nil
}

// DeleteSecret deletes the secret from the secrets store
func DeleteSecret(client *vaultApi.Client, secretPath string) error {
	_, err := client.Logical().Delete(secretPath)
	if err != nil {
		log.Infof("DeleteSecret: err %+v", err)
		return err
	}
	log.Infof("DeleteSecret: deletion from secret path %+v succeded", secretPath)

	return nil
}

func isKV2(client *vaultApi.Client, backend string) (bool, error) {
	if backend == "" {
		backend = defaultVaultBackendPath
	}
	if !strings.HasSuffix(backend, "/") {
		backend += "/"
	}
	mounts, err := client.Sys().ListMounts()
	if err != nil {
		return false, err
	}
	kvBackend, ok := mounts[backend]
	if !ok {
		return false, fmt.Errorf("can not find backend path %v in vault", backend)
	}
	if len(kvBackend.Options) > 0 && kvBackend.Options["version"] == "2" {
		return true, nil
	}
	return false, nil
}

// BuildExternalSecretPath builds a string that specifies the root key secret path
func BuildExternalSecretPath(client *vaultApi.Client, kms nbv1.KeyManagementServiceSpec, uid string) (string, error) {
	secretPath := ""
	backendPath := kms.ConnectionDetails[vaultBackendPath]
	if backendPath != "" {
		secretPath += backendPath
		if !strings.HasSuffix(backendPath, "/") {
			secretPath += "/"
		}
	} else {
		secretPath += defaultVaultBackendPath
	}

	v2, err := isKV2(client, backendPath)
	if err != nil {
		return "", err
	}
	if v2 {
		secretPath += "data/"
	}
	secretPath += rootSecretPath
	secretPath += "/rootkeyb64-" + uid
	return secretPath, nil
}

// IsVaultKMS return true if kms provider is vault
func IsVaultKMS(provider string) bool {
	return provider == "vault"
}

// ValidateConnectionDetails return error if kms connection details are faulty
func ValidateConnectionDetails(config map[string]string, tokenSecretName string, namespace string) error {
	// validate auth token
	if tokenSecretName == "" {
		return fmt.Errorf("kms token is missing")
	}
	secret := KubeObject(bundle.File_deploy_internal_secret_empty_yaml).(*corev1.Secret)
	secret.Namespace = namespace
	secret.Name = tokenSecretName

	if !KubeCheck(secret) {
		return fmt.Errorf(`❌ Could not find secret %q in namespace %q`, secret.Name, secret.Namespace)
	}

	token, ok := secret.StringData["token"]
	if !ok || token == "" {
		return fmt.Errorf("kms token in token secret is missing")
	}
	// validate connection details
	providerType := config[kmsProvider]
	if IsVaultKMS(providerType) {
		return ValidateVaultConnectionDetails(config, tokenSecretName, namespace)
	}
	return fmt.Errorf("Unsupported kms type: %v", providerType)
}

// ValidateVaultConnectionDetails return error if vault connection details are faulty
func ValidateVaultConnectionDetails(config map[string]string, tokenName string, namespace string) error {
	if addr, ok := config[vaultAddr]; !ok || addr == "" {
		return fmt.Errorf("failed to validate vault connection details: vault address is missing")
	}
	if capPath, ok := config[vaultCaPath]; ok && capPath != "" {
		// We do not support a directory with multiple CA since we fetch a k8s Secret and read its content
		// So we operate with a single CA only
		return fmt.Errorf("failed to validate vault connection details: multiple CA is unsupported")
	}
	secret := KubeObject(bundle.File_deploy_internal_secret_empty_yaml).(*corev1.Secret)
	secret.Namespace = namespace

	vaultTLSConnectionDetailsMap := map[string]string{vaultCaCert: "cert",
		vaultClientCert: "cert", vaultClientKey: "key"}

	for tlsOption, fieldInSecret := range vaultTLSConnectionDetailsMap {
		tlsOptionSecretName, ok := config[tlsOption]
		if ok && tlsOptionSecretName != "" {
			secret.Name = tlsOptionSecretName
			if !KubeCheckOptional(secret) {
				return fmt.Errorf(`❌ Could not find secret %q in namespace %q`, secret.Name, secret.Namespace)
			}
			if tlsOptionValue, ok := secret.Data[fieldInSecret]; !ok || len(tlsOptionValue) == 0 {
				return fmt.Errorf("failed to validate vault connection details: vault %v is missing in secret %q in namespace %q",
					tlsOption, secret.Name, secret.Namespace)
			}
		}
	}
	return nil
}

func writeCrtsToFile(secretName string, namespace string, secretValue []byte, envVarName string) (string, error) {
	// check here first the env variable
	if envVar, found := os.LookupEnv(envVarName); found && envVar != "" {
		return envVar, nil
	}

	// Generate a temp file
	file, err := ioutil.TempFile("", "")
	if err != nil {
		return "", fmt.Errorf("failed to generate temp file for k8s secret %q content, %v", secretName, err)
	}

	// Write into a file
	err = ioutil.WriteFile(file.Name(), secretValue, 0444)
	if err != nil {
		return "", fmt.Errorf("failed to write k8s secret %q content to a file %v", secretName, err)
	}

	// update the env var with the path
	envVarValue := file.Name()
	envVarKey := envVarName

	err = os.Setenv(envVarKey, envVarValue)
	if err != nil {
		return "", fmt.Errorf("can not set env var %v %v", envVarKey, envVarValue)
	}
	return envVarValue, nil
}
