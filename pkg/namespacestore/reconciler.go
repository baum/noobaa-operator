package namespacestore

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"time"

	nbv1 "github.com/noobaa/noobaa-operator/v2/pkg/apis/noobaa/v1alpha1"
	"github.com/noobaa/noobaa-operator/v2/pkg/bundle"
	"github.com/noobaa/noobaa-operator/v2/pkg/nb"
	"github.com/noobaa/noobaa-operator/v2/pkg/options"
	"github.com/noobaa/noobaa-operator/v2/pkg/system"
	"github.com/noobaa/noobaa-operator/v2/pkg/util"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ModeInfo holds local information for a namespace store mode.
type ModeInfo struct {
	Phase    nbv1.NamespaceStorePhase
	Severity string
}

var nsrModeInfoMap map[string]ModeInfo

func init() {
	nsrModeInfoMap = modeInfoMap()
}

func modeInfoMap() map[string]ModeInfo {
	return map[string]ModeInfo{
		"OPTIMAL":           {nbv1.NamespaceStorePhaseReady, corev1.EventTypeNormal},
		"IO_ERRORS":         {nbv1.NamespaceStorePhaseRejected, corev1.EventTypeWarning},
		"STORAGE_NOT_EXIST": {nbv1.NamespaceStorePhaseRejected, corev1.EventTypeWarning},
		"AUTH_FAILED":       {nbv1.NamespaceStorePhaseRejected, corev1.EventTypeWarning},
	}
}

// Reconciler is the context for loading or reconciling a noobaa system
type Reconciler struct {
	Request  types.NamespacedName
	Client   client.Client
	Scheme   *runtime.Scheme
	Ctx      context.Context
	Logger   *logrus.Entry
	Recorder record.EventRecorder
	NBClient nb.Client

	NamespaceStore *nbv1.NamespaceStore
	NooBaa         *nbv1.NooBaa
	Secret         *corev1.Secret
	ServiceAccount *corev1.ServiceAccount

	SystemInfo             *nb.SystemInfo
	ExternalConnectionInfo *nb.ExternalConnectionInfo
	NamespaceResourceinfo  *nb.NamespaceResourceInfo

	AddExternalConnectionParams   *nb.AddExternalConnectionParams
	CreateNamespaceResourceParams *nb.CreateNamespaceResourceParams
}

// Own sets the object owner references to the namespacestore
func (r *Reconciler) Own(obj metav1.Object) {
	util.Panic(controllerutil.SetControllerReference(r.NamespaceStore, obj, r.Scheme))
}

// NewReconciler initializes a reconciler to be used for loading or reconciling a namespace store
func NewReconciler(
	req types.NamespacedName,
	client client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
) *Reconciler {

	r := &Reconciler{
		Request:        req,
		Client:         client,
		Scheme:         scheme,
		Recorder:       recorder,
		Ctx:            context.TODO(),
		Logger:         logrus.WithField("namespace", req.Namespace+"/"+req.Name),
		NamespaceStore: util.KubeObject(bundle.File_deploy_crds_noobaa_io_v1alpha1_namespacestore_cr_yaml).(*nbv1.NamespaceStore),
		NooBaa:         util.KubeObject(bundle.File_deploy_crds_noobaa_io_v1alpha1_noobaa_cr_yaml).(*nbv1.NooBaa),
		Secret:         util.KubeObject(bundle.File_deploy_internal_secret_empty_yaml).(*corev1.Secret),
		ServiceAccount: util.KubeObject(bundle.File_deploy_service_account_yaml).(*corev1.ServiceAccount),
	}

	// Set Namespace
	r.NamespaceStore.Namespace = r.Request.Namespace
	r.NooBaa.Namespace = r.Request.Namespace
	r.ServiceAccount.Namespace = r.Request.Namespace

	// Set Names
	r.NamespaceStore.Name = r.Request.Name
	r.NooBaa.Name = options.SystemName
	r.ServiceAccount.Name = options.SystemName

	// Set secret names to empty
	r.Secret.Namespace = ""
	r.Secret.Name = ""

	return r
}

// Reconcile reads that state of the cluster for a System object,
// and makes changes based on the state read and what is in the System.Spec.
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *Reconciler) Reconcile() (reconcile.Result, error) {

	res := reconcile.Result{}
	log := r.Logger
	log.Infof("Start reconciling namespacestore ...")

	util.KubeCheck(r.NamespaceStore)
	log.Infof("Start reconciling namespacestore after kubecheck ...")

	if r.NamespaceStore.UID == "" {
		log.Infof("NamespaceStore %q not found or deleted. Skip reconcile.", r.NamespaceStore.Name)
		return reconcile.Result{}, nil
	}

	if util.EnsureCommonMetaFields(r.NamespaceStore, nbv1.Finalizer) {
		if !util.KubeUpdate(r.NamespaceStore) {
			log.Errorf("❌ NamespaceStore %q failed to add mandatory meta fields", r.NamespaceStore.Name)

			res.RequeueAfter = 3 * time.Second
			return res, nil
		}
	}
	system.CheckSystem(r.NooBaa)
	var err error

	if err == nil {
		err = r.LoadNamespaceStoreSecret()
	}

	if err == nil {
		if r.NamespaceStore.DeletionTimestamp != nil {
			err = r.ReconcileDeletion()
		} else {
			err = r.ReconcilePhases()
		}
	}

	if err != nil {
		if perr, isPERR := err.(*util.PersistentError); isPERR {
			r.SetPhase(nbv1.NamespaceStorePhaseRejected, perr.Reason, perr.Message)
			log.Errorf("❌ Persistent Error: %s", err)
			if r.Recorder != nil {
				r.Recorder.Eventf(r.NamespaceStore, corev1.EventTypeWarning, perr.Reason, perr.Message)
			}
		} else {
			res.RequeueAfter = 3 * time.Second
			// leave current phase as is
			r.SetPhase("", "TemporaryError", err.Error())
			log.Warnf("⏳ Temporary Error: %s", err)
		}
	} else {
		mode := r.NamespaceStore.Status.Mode.ModeCode
		phaseInfo, exist := nsrModeInfoMap[mode]

		if exist && phaseInfo.Phase != r.NamespaceStore.Status.Phase {
			phaseName := fmt.Sprintf("NamespaceStorePhase%s", phaseInfo.Phase)
			desc := fmt.Sprintf("Namespace store mode: %s", mode)
			r.SetPhase(phaseInfo.Phase, desc, phaseName)
			if r.Recorder != nil {
				r.Recorder.Eventf(r.NamespaceStore, phaseInfo.Severity, phaseName, desc)
			}
		} else {
			r.SetPhase(
				nbv1.NamespaceStorePhaseReady,
				"NamespaceStorePhaseReady",
				"noobaa operator completed reconcile - namespace store is ready",
			)
			log.Infof("✅ Done")
		}
	}
	logrus.Infof("ReconcilePhases update status")

	err = r.UpdateStatus()
	// if updateStatus will fail to update the CR for any reason we will continue to requeue the reconcile
	// until the spec status will reflect the actual status of the namespacestore
	if err != nil {
		res.RequeueAfter = 3 * time.Second
		log.Warnf("⏳ Temporary Error: %s", err)
	}
	return res, nil
}

// ReconcilePhases runs the reconcile flow and populates System.Status.
func (r *Reconciler) ReconcilePhases() error {

	if err := r.ReconcilePhaseVerifying(); err != nil {
		logrus.Infof("ReconcilePhases verifying")
		return err
	}
	if err := r.ReconcilePhaseConnecting(); err != nil {
		logrus.Infof("ReconcilePhases connecting")
		return err
	}
	if err := r.ReconcilePhaseCreating(); err != nil {
		logrus.Infof("ReconcilePhases creating")
		return err
	}

	return nil
}

// SetPhase updates the status phase and conditions
func (r *Reconciler) SetPhase(phase nbv1.NamespaceStorePhase, reason string, message string) {

	c := &r.NamespaceStore.Status.Conditions

	if phase == "" {
		r.Logger.Infof("SetPhase: temporary error during phase %q", r.NamespaceStore.Status.Phase)
		util.SetProgressingCondition(c, reason, message)
		return
	}

	r.Logger.Infof("SetPhase: %s", phase)
	r.NamespaceStore.Status.Phase = phase
	switch phase {
	case nbv1.NamespaceStorePhaseReady:
		util.SetAvailableCondition(c, reason, message)
	case nbv1.NamespaceStorePhaseRejected:
		util.SetErrorCondition(c, reason, message)
	default:
		util.SetProgressingCondition(c, reason, message)
	}
}

// UpdateStatus updates the namespace store status in kubernetes from the memory
func (r *Reconciler) UpdateStatus() error {
	err := r.Client.Status().Update(r.Ctx, r.NamespaceStore)
	if err != nil {
		r.Logger.Errorf("UpdateStatus: %s", err)
		return err
	}
	r.Logger.Infof("UpdateStatus: Done")
	return nil
}

// ReconcilePhaseVerifying checks that we have the system and secret needed to reconcile
func (r *Reconciler) ReconcilePhaseVerifying() error {

	r.SetPhase(
		nbv1.NamespaceStorePhaseVerifying,
		"NamespaceStorePhaseVerifying",
		"noobaa operator started phase 1/3 - \"Verifying\"",
	)

	if r.NooBaa.UID == "" {
		return util.NewPersistentError("MissingSystem",
			fmt.Sprintf("NooBaa system %q not found or deleted", r.NooBaa.Name))
	}

	if r.Secret.Name != "" && r.Secret.UID == "" {
		if time.Since(r.NamespaceStore.CreationTimestamp.Time) < 5*time.Minute {
			return fmt.Errorf("NamespaceStore Secret %q not found, but not rejecting the young as it might be in process", r.Secret.Name)
		}
		return util.NewPersistentError("MissingSecret",
			fmt.Sprintf("NamespaceStore Secret %q not found or deleted", r.Secret.Name))
	}

	return nil
}

// ReconcilePhaseConnecting checks that we have the system and secret needed to reconcile
func (r *Reconciler) ReconcilePhaseConnecting() error {
	logrus.Infof("ReconcilePhaseConnecting")

	r.SetPhase(
		nbv1.NamespaceStorePhaseConnecting,
		"NamespaceStorePhaseConnecting",
		"noobaa operator started phase 2/3 - \"Connecting\"",
	)
	logrus.Infof("ReconcilePhaseConnecting1")
	if err := r.ReadSystemInfo(); err != nil {
		logrus.Infof("ReconcilePhaseConnecting2 err %+v", err)
		return err
	}

	return nil
}

// ReconcilePhaseCreating checks that we have the system and secret needed to reconcile
func (r *Reconciler) ReconcilePhaseCreating() error {
	logrus.Infof("ReconcilePhaseCreating")

	r.SetPhase(
		nbv1.NamespaceStorePhaseCreating,
		"NamespaceStorePhaseCreating",
		"noobaa operator started phase 3/3 - \"Creating\"",
	)

	if err := r.ReconcileExternalConnection(); err != nil {
		logrus.Infof("ReconcilePhaseCreating1 %+v", err)
		return err
	}
	if err := r.ReconcileNamespaceStore(); err != nil {
		logrus.Infof("ReconcilePhaseCreating2 %+v", err)
		return err
	}

	return nil
}

// ReconcileDeletion handles the deletion of a namespace-store using the noobaa api
func (r *Reconciler) ReconcileDeletion() error {

	// Set the phase to let users know the operator has noticed the deletion request
	if r.NamespaceStore.Status.Phase != nbv1.NamespaceStorePhaseDeleting {
		r.SetPhase(
			nbv1.NamespaceStorePhaseDeleting,
			"NamespaceStorePhaseDeleting",
			"noobaa operator started deletion",
		)
		err := r.UpdateStatus()
		if err != nil {
			return err
		}
	}

	if r.NooBaa.UID == "" {
		r.Logger.Infof("NamepsaceStore %q remove finalizer because NooBaa system is already deleted", r.NamespaceStore.Name)
		return r.FinalizeDeletion()
	}

	if err := r.ReadSystemInfo(); err != nil {
		return err
	}

	if r.NamespaceResourceinfo != nil {

		internalPoolName := ""
		for i := range r.SystemInfo.Pools {
			pool := &r.SystemInfo.Pools[i]
			if pool.ResourceType == "INTERNAL" {
				internalPoolName = pool.Name
				break
			}
		}
		for i := range r.SystemInfo.Accounts {
			account := &r.SystemInfo.Accounts[i]
			if account.DefaultResource == r.NamespaceResourceinfo.Name {
				allowedBuckets := account.AllowedBuckets
				if allowedBuckets.PermissionList == nil {
					allowedBuckets.PermissionList = []string{}
				}
				err := r.NBClient.UpdateAccountS3Access(nb.UpdateAccountS3AccessParams{
					Email:        account.Email,
					S3Access:     account.HasS3Access,
					DefaultResource:  &internalPoolName,
					AllowBuckets: &allowedBuckets,
				})
				if err != nil {
					return err
				}
			}
		}
		err := r.NBClient.DeleteNamespaceResourceAPI(nb.DeleteNamespaceResourceParams{Name: r.NamespaceResourceinfo.Name})
		if err != nil {
			if rpcErr, isRPCErr := err.(*nb.RPCError); isRPCErr {
				if rpcErr.RPCCode == "IN_USE" {
					return fmt.Errorf("DeleteNamespaceResourceAPI cannot complete because namespace store %q has buckets attached", r.NamespaceResourceinfo.Name)
				}
			}
			return err
		}
	}

	if r.ExternalConnectionInfo != nil {
		// TODO we cannot assume we are the only one using this connection...
		err := r.NBClient.DeleteExternalConnectionAPI(nb.DeleteExternalConnectionParams{Name: r.ExternalConnectionInfo.Name})
		if err != nil {
			if rpcErr, isRPCErr := err.(*nb.RPCError); isRPCErr {
				if rpcErr.RPCCode != "IN_USE" {
					return err
				}
				r.Logger.Warnf("DeleteExternalConnection cannot complete because it is IN_USE %q", r.ExternalConnectionInfo.Name)
			} else {
				return err
			}
		}
	}

	return r.FinalizeDeletion()
}

// FinalizeDeletion removed the finalizer and updates in order to let the namespace-store get reclaimed by kubernetes
func (r *Reconciler) FinalizeDeletion() error {
	util.RemoveFinalizer(r.NamespaceStore, nbv1.Finalizer)
	if !util.KubeUpdate(r.NamespaceStore) {
		return fmt.Errorf("NamespaceStore %q failed to remove finalizer %q", r.NamespaceStore.Name, nbv1.Finalizer)
	}
	return nil
}

// ReadSystemInfo loads the information from the noobaa system api,
// and prepares the structures to reconcile
func (r *Reconciler) ReadSystemInfo() error {

	sysClient, err := system.Connect(false)
	if err != nil {
		logrus.Infof("ReadSystemInfo1 err1 %+v", err)
		return err
	}
	r.NBClient = sysClient.NBClient

	systemInfo, err := r.NBClient.ReadSystemAPI()
	if err != nil {
		logrus.Infof("ReadSystemInfo1 err2 %+v", err)
		return err
	}
	r.SystemInfo = &systemInfo

	// Check if namespace resource exists
	for i := range r.SystemInfo.NamespaceResources {
		nsr := &r.SystemInfo.NamespaceResources[i]
		if nsr.Name == r.NamespaceStore.Name {
			r.NamespaceResourceinfo = nsr
			break
		}
	}

	nsr := r.NamespaceResourceinfo

	// handling namespace fs resource
	if r.NamespaceStore.Spec.Type == nbv1.NSStoreTypeNSFS {
		r.CreateNamespaceResourceParams = &nb.CreateNamespaceResourceParams{
			Name: r.NamespaceStore.Name,
			NSFSConfig: &nb.NSFSConfig{
				FsBackend: r.NamespaceStore.Spec.NSFS.FsBackend,
				FsRootPath:    r.NamespaceStore.Spec.NSFS.FsRootPath,
			},
			NamespaceStore: &nb.NamespaceStoreInfo{
				Name:      r.NamespaceStore.Name,
				Namespace: options.Namespace,
			},
		}
		return nil
	}

	conn, err := r.MakeExternalConnectionParams()
	if err != nil {
		return err
	}

	// Check that noobaa-core uses the same connection as the namespace store
	// Due to noobaa/noobaa-core#5750 the identity (access-key) is not returned in the api call so just warn for now
	// TODO Improve handling of this condition
	if nsr != nil {
		if nsr.EndpointType != conn.EndpointType ||
			nsr.Endpoint != conn.Endpoint ||
			nsr.Identity != conn.Identity {
			r.Logger.Warnf("using existing namespace resource but connection mismatch %+v namespace store %+v", conn, nsr)
		}
	}

	// Reuse an existing connection if match is found
	for i := range r.SystemInfo.Accounts {
		account := &r.SystemInfo.Accounts[i]
		for j := range account.ExternalConnections.Connections {
			c := &account.ExternalConnections.Connections[j]
			if c.EndpointType == conn.EndpointType &&
				c.Endpoint == conn.Endpoint &&
				c.Identity == conn.Identity {
				r.ExternalConnectionInfo = c
				conn.Name = c.Name
			}
		}
	}

	r.AddExternalConnectionParams = conn

	r.CreateNamespaceResourceParams = &nb.CreateNamespaceResourceParams{
		Name:         r.NamespaceStore.Name,
		Connection:   conn.Name,
		TargetBucket: GetNamespaceStoreTargetBucket(r.NamespaceStore),
		NamespaceStore: &nb.NamespaceStoreInfo{
			Name:      r.NamespaceStore.Name,
			Namespace: options.Namespace,
		},
	}

	return nil
}

// LoadNamespaceStoreSecret loads the secret to the reconciler struct
func (r *Reconciler) LoadNamespaceStoreSecret() error {
	secretRef := GetNamespaceStoreSecret(r.NamespaceStore)
	if secretRef != nil {
		r.Secret.Name = secretRef.Name
		r.Secret.Namespace = secretRef.Namespace
		if r.Secret.Namespace == "" {
			r.Secret.Namespace = r.NamespaceStore.Namespace
		}
		util.KubeCheck(r.Secret)
	}
	return nil
}

// MakeExternalConnectionParams translates the namespace store spec and secret,
// to noobaa api structures to be used for creating/updating external connetion and namespace store
func (r *Reconciler) MakeExternalConnectionParams() (*nb.AddExternalConnectionParams, error) {

	conn := &nb.AddExternalConnectionParams{
		Name: r.NamespaceStore.Name,
	}

	r.fixAlternateKeysNames()

	switch r.NamespaceStore.Spec.Type {

	case nbv1.NSStoreTypeAWSS3:
		conn.EndpointType = nb.EndpointTypeAws
		conn.Identity = r.Secret.StringData["AWS_ACCESS_KEY_ID"]
		conn.Secret = r.Secret.StringData["AWS_SECRET_ACCESS_KEY"]
		awsS3 := r.NamespaceStore.Spec.AWSS3
		u := url.URL{
			Scheme: "https",
			Host:   "s3.amazonaws.com",
		}
		if awsS3.SSLDisabled {
			u.Scheme = "http"
		}
		if awsS3.Region != "" {
			u.Host = fmt.Sprintf("s3.%s.amazonaws.com", awsS3.Region)
		}
		conn.Endpoint = u.String()

	case nbv1.NSStoreTypeS3Compatible:
		conn.EndpointType = nb.EndpointTypeS3Compat
		conn.Identity = r.Secret.StringData["AWS_ACCESS_KEY_ID"]
		conn.Secret = r.Secret.StringData["AWS_SECRET_ACCESS_KEY"]
		s3Compatible := r.NamespaceStore.Spec.S3Compatible
		if s3Compatible.SignatureVersion == nbv1.S3SignatureVersionV4 {
			conn.AuthMethod = "AWS_V4"
		} else if s3Compatible.SignatureVersion == nbv1.S3SignatureVersionV2 {
			conn.AuthMethod = "AWS_V2"
		} else if s3Compatible.SignatureVersion != "" {
			return nil, util.NewPersistentError("InvalidSignatureVersion",
				fmt.Sprintf("Invalid s3 signature version %q for namespace store %q",
					s3Compatible.SignatureVersion, r.NamespaceStore.Name))
		}
		if s3Compatible.Endpoint == "" {
			u := url.URL{
				Scheme: "https",
				Host:   "127.0.0.1:6443",
			}
			// if s3Compatible.SSLDisabled {
			// 	u.Scheme = "http"
			// 	u.Host = fmt.Sprintf("127.0.0.1:6001")
			// }
			conn.Endpoint = u.String()
		} else {
			match, err := regexp.MatchString(`^\w+://`, s3Compatible.Endpoint)
			if err != nil {
				return nil, util.NewPersistentError("InvalidEndpoint",
					fmt.Sprintf("Invalid endpoint url %q: %v", s3Compatible.Endpoint, err))
			}
			if !match {
				s3Compatible.Endpoint = "https://" + s3Compatible.Endpoint
				// if s3Options.SSLDisabled {
				// 	u.Scheme = "http"
				// }
			}
			u, err := url.Parse(s3Compatible.Endpoint)
			if err != nil {
				return nil, util.NewPersistentError("InvalidEndpoint",
					fmt.Sprintf("Invalid endpoint url %q: %v", s3Compatible.Endpoint, err))
			}
			if u.Scheme == "" {
				u.Scheme = "https"
				// if s3Options.SSLDisabled {
				// 	u.Scheme = "http"
				// }
			}
			conn.Endpoint = u.String()
		}

	case nbv1.NSStoreTypeIBMCos:
		conn.EndpointType = nb.EndpointTypeIBMCos
		conn.Identity = r.Secret.StringData["IBM_COS_ACCESS_KEY_ID"]
		conn.Secret = r.Secret.StringData["IBM_COS_SECRET_ACCESS_KEY"]
		IBMCos := r.NamespaceStore.Spec.IBMCos
		if IBMCos.SignatureVersion == nbv1.S3SignatureVersionV4 {
			conn.AuthMethod = "AWS_V4"
		} else if IBMCos.SignatureVersion == nbv1.S3SignatureVersionV2 {
			conn.AuthMethod = "AWS_V2"
		} else if IBMCos.SignatureVersion != "" {
			return nil, util.NewPersistentError("InvalidSignatureVersion",
				fmt.Sprintf("Invalid s3 signature version %q for namespace store %q",
					IBMCos.SignatureVersion, r.NamespaceStore.Name))
		}
		if IBMCos.Endpoint == "" {
			u := url.URL{
				Scheme: "https",
				Host:   "127.0.0.1:6443",
			}
			// if IBMCos.SSLDisabled {
			// 	u.Scheme = "http"
			// 	u.Host = fmt.Sprintf("127.0.0.1:6001")
			// }
			conn.Endpoint = u.String()
		} else {
			match, err := regexp.MatchString(`^\w+://`, IBMCos.Endpoint)
			if err != nil {
				return nil, util.NewPersistentError("InvalidEndpoint",
					fmt.Sprintf("Invalid endpoint url %q: %v", IBMCos.Endpoint, err))
			}
			if !match {
				IBMCos.Endpoint = "https://" + IBMCos.Endpoint
				// if s3Options.SSLDisabled {
				// 	u.Scheme = "http"
				// }
			}
			u, err := url.Parse(IBMCos.Endpoint)
			if err != nil {
				return nil, util.NewPersistentError("InvalidEndpoint",
					fmt.Sprintf("Invalid endpoint url %q: %v", IBMCos.Endpoint, err))
			}
			if u.Scheme == "" {
				u.Scheme = "https"
				// if s3Options.SSLDisabled {
				// 	u.Scheme = "http"
				// }
			}
			conn.Endpoint = u.String()
		}

	case nbv1.NSStoreTypeAzureBlob:
		conn.EndpointType = nb.EndpointTypeAzure
		conn.Endpoint = "https://blob.core.windows.net"
		conn.Identity = r.Secret.StringData["AccountName"]
		conn.Secret = r.Secret.StringData["AccountKey"]

	default:
		return nil, util.NewPersistentError("InvalidType",
			fmt.Sprintf("Invalid namespace store type %q", r.NamespaceStore.Spec.Type))
	}

	if !util.IsStringGraphicOrSpacesCharsOnly(conn.Identity) || !util.IsStringGraphicOrSpacesCharsOnly(conn.Secret) {
		return nil, util.NewPersistentError("InvalidSecret",
			fmt.Sprintf("Invalid secret containing non graphic characters (perhaps not base64 encoded?) %q", r.Secret.Name))
	}

	return conn, nil
}

func (r *Reconciler) fixAlternateKeysNames() {

	alternateAccessKeyNames := []string{"aws_access_key_id", "AccessKey"}
	alternateSecretKeyNames := []string{"aws_secret_access_key", "SecretKey"}

	if r.Secret.StringData["AWS_ACCESS_KEY_ID"] == "" {
		for _, key := range alternateAccessKeyNames {
			if r.Secret.StringData[key] != "" {
				r.Secret.StringData["AWS_ACCESS_KEY_ID"] = r.Secret.StringData[key]
				break
			}
		}
	}

	if r.Secret.StringData["AWS_SECRET_ACCESS_KEY"] == "" {
		for _, key := range alternateSecretKeyNames {
			if r.Secret.StringData[key] != "" {
				r.Secret.StringData["AWS_SECRET_ACCESS_KEY"] = r.Secret.StringData[key]
				break
			}
		}
	}
}

// ReconcileExternalConnection handles the external connection using noobaa api
func (r *Reconciler) ReconcileExternalConnection() error {
	logrus.Infof("ReconcileExternalConnection")

	// TODO we only support creation here, but not updates
	if r.ExternalConnectionInfo != nil {
		logrus.Infof("ReconcileExternalConnection1")
		return nil
	}
	if r.AddExternalConnectionParams == nil {
		logrus.Infof("ReconcileExternalConnection2")
		return nil
	}

	res, err := r.NBClient.CheckExternalConnectionAPI(*r.AddExternalConnectionParams)
	if err != nil {
		logrus.Infof("ReconcileExternalConnection3 %+v", err)
		if rpcErr, isRPCErr := err.(*nb.RPCError); isRPCErr {
			if rpcErr.RPCCode == "INVALID_SCHEMA_PARAMS" {
				return util.NewPersistentError("InvalidConnectionParams", rpcErr.Message)
			}
		}
		return err
	}

	switch res.Status {

	case nb.ExternalConnectionSuccess:
		// good
		logrus.Infof("ReconcileExternalConnection4 ExternalConnectionSuccess")

	case nb.ExternalConnectionInvalidCredentials:
		logrus.Infof("ReconcileExternalConnection5 ExternalConnectionInvalidCredentials")

		if time.Since(r.NamespaceStore.CreationTimestamp.Time) < 5*time.Minute {
			r.Logger.Infof("got invalid credentials. sometimes access keys take time to propagate inside AWS. requeuing for 5 minutes")
			return fmt.Errorf("Got InvalidCredentials. requeue again")
		}
		fallthrough
	case nb.ExternalConnectionInvalidEndpoint:
		logrus.Infof("ReconcileExternalConnection6 ExternalConnectionInvalidEndpoint")
		if time.Since(r.NamespaceStore.CreationTimestamp.Time) < 5*time.Minute {
			r.Logger.Infof("got invalid endopint. requeuing for 5 minutes to make sure it is not a temporary connection issue")
			return fmt.Errorf("got invalid endopint. requeue again")
		}
		fallthrough
	case nb.ExternalConnectionTimeSkew:
		fallthrough
	case nb.ExternalConnectionNotSupported:
		logrus.Infof("ReconcileExternalConnection7 ExternalConnectionNotSupported")
		return util.NewPersistentError(string(res.Status),
			fmt.Sprintf("NamespaceStore %q invalid external connection %q", r.NamespaceStore.Name, res.Status))

	case nb.ExternalConnectionTimeout:
		fallthrough
	case nb.ExternalConnectionUnknownFailure:
		fallthrough
	default:
		logrus.Infof("ReconcileExternalConnection8 default")

		return fmt.Errorf("CheckExternalConnection Status=%s Error=%s Message=%s",
			res.Status, res.Error.Code, res.Error.Message)
	}

	logrus.Infof("ReconcileExternalConnection8 %+v", r.AddExternalConnectionParams)
	err = r.NBClient.AddExternalConnectionAPI(*r.AddExternalConnectionParams)
	if err != nil {
		logrus.Infof("ReconcileExternalConnection9 %+v", err)
		return err
	}

	logrus.Infof("ReconcileExternalConnection10")
	return nil
}

// ReconcileNamespaceStore handles the namespace store using noobaa api
func (r *Reconciler) ReconcileNamespaceStore() error {

	if r.NamespaceResourceinfo != nil {
		return nil
	}

	if r.CreateNamespaceResourceParams != nil {
		err := r.NBClient.CreateNamespaceResourceAPI(*r.CreateNamespaceResourceParams)
		if err != nil {
			return err
		}
	}
	return nil
}
