package kms_dev_test

import (
	"os"

	nbv1 "github.com/noobaa/noobaa-operator/v5/pkg/apis/noobaa/v1alpha1"
	"github.com/noobaa/noobaa-operator/v5/pkg/options"
	"github.com/noobaa/noobaa-operator/v5/pkg/system"
	"github.com/noobaa/noobaa-operator/v5/pkg/util"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func getMiniNooBaa() *nbv1.NooBaa {
	options.MiniEnv = true
	options.Namespace = corev1.NamespaceDefault
	nb := system.LoadSystemDefaults()
	return nb
}


func simpleKmsSpec(token, api_address string) nbv1.KeyManagementServiceSpec {
	kms := nbv1.KeyManagementServiceSpec{}
	kms.TokenSecretName = token
	kms.ConnectionDetails = map[string]string{
		util.VaultAddr : api_address,
		util.VaultBackendPath : "noobaa/",
		util.KmsProvider : util.KmsProviderVault,
	}

	return kms
}

var _ = Describe("External KMS integration test - Dev Vault deployment", func() {

	Context("Verify non-KMS NooBaa", func() {
		noobaa := getMiniNooBaa()
		Specify("Create default system", func() {
			Expect(util.KubeCreateFailExisting(noobaa)).To(BeTrue())
		})
		Specify("Verify KMS condition status", func() {
			Expect(util.NooBaaCondStatus(noobaa, nbv1.ConditionKMSK8S)).To(BeTrue())
		})
		Specify("Delete NooBaa", func() {
			Expect(util.KubeDelete(noobaa)).To(BeTrue())
		})		
	})

	api_address, api_address_found := os.LookupEnv("API_ADDRESS")
	token_secret_name, token_secret_name_found := os.LookupEnv("TOKEN_SECRET_NAME")

	Context("Verify Vault NooBaa", func() {
		noobaa := getMiniNooBaa()
		noobaa.Spec.Security.KeyManagementService = simpleKmsSpec(token_secret_name, api_address)
	
		Specify("Verify ENV", func() {
			Expect(api_address_found).To(BeTrue())
			logger.Printf("💬 Found API_ADDRESS=%v", api_address)

			Expect(token_secret_name_found).To(BeTrue())
			logger.Printf("💬 Found TOKEN_SECRET_NAME=%v", token_secret_name)
			logger.Printf("💬 KMS Spec %v", noobaa.Spec.Security.KeyManagementService)
		})
		Specify("Create Vault Noobaa", func() {
			Expect(util.KubeCreateFailExisting(noobaa)).To(BeTrue())
		})
		Specify("Verify KMS condition status Init", func() {
			Expect(util.NooBaaCondStatus(noobaa, nbv1.ConditionKMSInit)).To(BeTrue())
		})
		Specify("Restart NooBaa operator", func() {
			podList := &corev1.PodList{}
			podSelector, _ := labels.Parse("noobaa-operator=deployment")
			listOptions := client.ListOptions{Namespace: options.Namespace, LabelSelector: podSelector}
		
			Expect(util.KubeList(podList, &listOptions)).To(BeTrue())
			Expect(len(podList.Items)).To(BeEquivalentTo(1))
			Expect(util.KubeDelete(&podList.Items[0])).To(BeTrue())
		})
		Specify("Verify KMS condition status Sync", func() {
			Expect(util.NooBaaCondStatus(noobaa, nbv1.ConditionKMSSync)).To(BeTrue())
		})
		Specify("Delete NooBaa", func() {
			Expect(util.KubeDelete(noobaa)).To(BeTrue())
		})		
	})

	Context("Invalid Vault Configuration", func() {
		Specify("Ivalid Token Secret name", func() {
			noobaa := getMiniNooBaa()
			noobaa.Spec.Security.KeyManagementService = simpleKmsSpec(token_secret_name, api_address)
			noobaa.Spec.Security.KeyManagementService.TokenSecretName = "invalid"
			Expect(util.KubeCreateFailExisting(noobaa)).To(BeTrue())
			Expect(util.NooBaaCondStatus(noobaa, nbv1.ConditionKMSInvalid)).To(BeTrue())
			Expect(util.KubeDelete(noobaa)).To(BeTrue())
		})
		Specify("Ivalid KMS provider", func() {
			noobaa := getMiniNooBaa()
			noobaa.Spec.Security.KeyManagementService = simpleKmsSpec(token_secret_name, api_address)
			noobaa.Spec.Security.KeyManagementService.ConnectionDetails[util.KmsProvider] = "invalid"
			Expect(util.KubeCreateFailExisting(noobaa)).To(BeTrue())
			Expect(util.NooBaaCondStatus(noobaa, nbv1.ConditionKMSInvalid)).To(BeTrue())
			Expect(util.KubeDelete(noobaa)).To(BeTrue())
		})
	})
})
