package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/libopenstorage/secrets"
	"github.com/libopenstorage/secrets/vault"
	nbv1 "github.com/noobaa/noobaa-operator/v5/pkg/apis/noobaa/v1alpha1"
	"github.com/noobaa/noobaa-operator/v5/pkg/bundle"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

///////////////////////////////////
/////////// VAULT UTILS ///////////
///////////////////////////////////
const (
	RootSecretPath          = "NOOBAA_ROOT_SECRET_PATH"
	VaultCaCert             = "VAULT_CACERT"
	VaultClientCert         = "VAULT_CLIENT_CERT"
	VaultClientKey          = "VAULT_CLIENT_KEY"
	VaultAddr               = "VAULT_ADDR"
	VaultCaPath             = "VAULT_CAPATH"
	VaultBackendPath        = "VAULT_BACKEND_PATH"
	VaultToken              = "VAULT_TOKEN"
	VaultAuthMethod         = "VAULT_AUTH_METHOD"
	VaultSkipVerify         = "VAULT_SKIP_VERIFY"
	VaultAuthMethodK8S      = "kubernetes"
	VaultAuthKubernetesRole = "VAULT_AUTH_KUBERNETES_ROLE"
	KmsProvider             = "KMS_PROVIDER"
	KmsProviderVault        = "vault"
	defaultVaultBackendPath = "secret/"
)


// VerifyExternalSecretsDeletion checks if noobaa is on un-installation process
// if true, deletes secrets from external KMS
func VerifyExternalSecretsDeletion(kms nbv1.KeyManagementServiceSpec, namespace string, uid types.UID) error {

	if len(kms.ConnectionDetails) == 0 {
		log.Infof("deleting root key locally")
		return nil
	}

	if err := ValidateConnectionDetails(kms.ConnectionDetails, kms.TokenSecretName, namespace); err != nil {
		log.Errorf("Invalid KMS connection details %v", kms)
		return err
	}

	c, err := InitVaultClient(kms.ConnectionDetails, kms.TokenSecretName, namespace)
	if err != nil {
		log.Errorf("deleting root key externally failed: init vault client: %v", err)
		return err
	}

	secretPath := BuildExternalSecretPath(kms, string(uid))
	err = DeleteSecret(c, secretPath)
	if err != nil {
		log.Errorf("deleting root key externally failed: %v", err)
		return err
	}

	return nil
}

// InitVaultClient inits the secret store
func InitVaultClient(config map[string]string, tokenSecretName string, namespace string) (secrets.Secrets, error) {

	// create a type correct copy of the configuration
	vaultConfig := make(map[string]interface{})
	for k, v := range config {
		vaultConfig[k] = v
	}

	// create TLS files out of secrets
	// replace in vaultConfig secret names with local temp files paths
	err := tlsConfig(vaultConfig, namespace)
	if err != nil {
		return nil, fmt.Errorf(`❌ Could not init vault tls config %q in namespace %q`, config, namespace)
	}

	// veify backend path, use default value if not set
	if b, ok := config[VaultBackendPath]; !ok || b == "" {
		log.Infof("KMS: using default backend path %v", defaultVaultBackendPath)
		vaultConfig[VaultBackendPath] = defaultVaultBackendPath
	}

	// fetch vault token from the secret
	if config[VaultAuthMethod] != VaultAuthMethodK8S {
		secret := KubeObject(bundle.File_deploy_internal_secret_empty_yaml).(*corev1.Secret)
		secret.Namespace = namespace
		secret.Name = tokenSecretName
		if !KubeCheck(secret) {
			return nil, fmt.Errorf(`❌ Could not find secret %q in namespace %q`, secret.Name, secret.Namespace)
		}
		token := secret.StringData["token"]
		vaultConfig[VaultToken] = token
	}

	log.Infof("KMS vault config: %v", vaultConfig)
	return vault.New(vaultConfig)
}

// tlsConfig create temp files with TLS keys and certs
func tlsConfig(config map[string]interface{}, namespace string) (error) {
	secret := KubeObject(bundle.File_deploy_internal_secret_empty_yaml).(*corev1.Secret)
	secret.Namespace = namespace

	if caCertSecretName, ok := config[VaultCaCert]; ok {
		secret.Name = caCertSecretName.(string)
		if !KubeCheckOptional(secret) {
			return fmt.Errorf(`❌ Could not find secret %q in namespace %q`, secret.Name, secret.Namespace)
		}
		caFileAddr, err := writeCrtsToFile(secret.Name, namespace, secret.Data["cert"], VaultCaCert)
		if err != nil {
			return fmt.Errorf("can not write crt %v to file %v", VaultCaCert, err)
		}
		config[VaultCaCert] = caFileAddr
	}

	if clientCertSecretName, ok := config[VaultClientCert]; ok {
		secret.Name = clientCertSecretName.(string)
		if !KubeCheckOptional(secret) {
			return fmt.Errorf(`❌ Could not find secret %q in namespace %q`, secret.Name, secret.Namespace)
		}
		clientCertFileAddr, err := writeCrtsToFile(secret.Name, namespace, secret.Data["cert"], VaultClientCert)
		if err != nil {
			return fmt.Errorf("can not write crt %v to file %v", VaultClientCert, err)
		}
		config[VaultClientCert] = clientCertFileAddr

	}
	if clientKeySecretName, ok := config[VaultClientKey]; ok {
		secret.Name = clientKeySecretName.(string)
		if !KubeCheckOptional(secret) {
			return fmt.Errorf(`❌ Could not find secret %q in namespace %q`, secret.Name, secret.Namespace)
		}
		clientKeyFileAddr, err := writeCrtsToFile(secret.Name, namespace, secret.Data["key"], VaultClientKey)
		if err != nil {
			return fmt.Errorf("can not write crt %v to file %v", VaultClientKey, err)
		}
		config[VaultClientKey] = clientKeyFileAddr
	}
	return nil
}

// PutSecret writes the secret to the secrets store
func PutSecret(client secrets.Secrets, secretName, secretValue, secretPath string) error {
	data := make(map[string]interface{})
	data[secretName] = secretValue

	err := client.PutSecret(secretPath, data, nil)
	if err != nil {
		log.Errorf("KMS PutSecret: secret path %v value %v, error %v", secretPath, secretValue, err)
		return err
	}

	return nil
}

// GetSecret reads the secret to the secrets store
func GetSecret(client secrets.Secrets, secretName, secretPath string) (string, error) {
	s, err := client.GetSecret(secretPath, nil)
	if err != nil {
		log.Errorf("KMS GetSecret: secret path %v, error %v", secretPath, err)
		return "", err
	}

	return s[secretName].(string), nil
}

// DeleteSecret deletes the secret from the secrets store
func DeleteSecret(client secrets.Secrets, secretPath string) error {
	// see https://github.com/libopenstorage/secrets/commit/dde442ea20ec9d59c71cea5ee0f21eeffd17ed19
	// keyContext := map[string]string{secrets.DestroySecret: "true"}
	err := client.DeleteSecret(secretPath, nil)
	if err != nil {
		log.Errorf("KMS DeleteSecret: secret path %v, error %v", secretPath, err)
		return err
	}
	return nil
}

// BuildExternalSecretPath builds a string that specifies the root key secret path
func BuildExternalSecretPath(kms nbv1.KeyManagementServiceSpec, uid string) (string) {
	secretPath := RootSecretPath + "/rootkeyb64-" + uid
	return secretPath
}

// isVaultKMS return true if kms provider is vault
func isVaultKMS(provider string) bool {
	return provider == KmsProviderVault
}

func validateAuthToken(tokenSecretName, namespace string) error {
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

	return nil
}

// ValidateConnectionDetails return error if kms connection details are faulty
func ValidateConnectionDetails(config map[string]string, tokenSecretName string, namespace string) error {
	if config[VaultAuthMethod] != VaultAuthMethodK8S {
		if err := validateAuthToken(tokenSecretName, namespace); err != nil {
			return err
		}
	}

	// validate auth token
	// validate connection details
	providerType := config[KmsProvider]
	if !isVaultKMS(providerType) {
		return fmt.Errorf("Unsupported kms type: %v", providerType)
	}

	return validateVaultConnectionDetails(config, tokenSecretName, namespace)
}

// validateVaultConnectionDetails return error if vault connection details are faulty
func validateVaultConnectionDetails(config map[string]string, tokenName string, namespace string) error {
	if addr, ok := config[VaultAddr]; !ok || addr == "" {
		return fmt.Errorf("failed to validate vault connection details: vault address is missing")
	}
	if capPath, ok := config[VaultCaPath]; ok && capPath != "" {
		// We do not support a directory with multiple CA since we fetch a k8s Secret and read its content
		// So we operate with a single CA only
		return fmt.Errorf("failed to validate vault connection details: multiple CA is unsupported")
	}
	secret := KubeObject(bundle.File_deploy_internal_secret_empty_yaml).(*corev1.Secret)
	secret.Namespace = namespace

	vaultTLSConnectionDetailsMap := map[string]string{VaultCaCert: "cert",
		VaultClientCert: "cert", VaultClientKey: "key"}

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

//
// Test shared utilities
//
func kmsStatus(nb *nbv1.NooBaa, status corev1.ConditionStatus) bool {
	for _, cond := range nb.Status.Conditions {
		log.Printf("condition type %v status %v", cond.Type, cond.Status)
		if cond.Type == nbv1.ConditionTypeKMS && cond.Status == status {
			return true
		}
	}
	return false
}

// NooBaaCondStatus waits for requested NooBaa CR KSM condition status
// returns false if timeout
func NooBaaCondStatus(noobaa* nbv1.NooBaa, s corev1.ConditionStatus) bool {
	found := false

	timeout := 120 // seconds
	for i := 0; i < timeout; i++ {
		_, _, err := KubeGet(noobaa)
		Panic(err)

		if kmsStatus(noobaa, s) {
			found = true
			break
		}
		time.Sleep(time.Second)
	}

	return found
}