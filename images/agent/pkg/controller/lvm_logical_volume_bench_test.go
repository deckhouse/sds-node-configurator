package controller

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	sv1 "k8s.io/api/storage/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"os"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/kubutils"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

var (
	lvgName   = "hdd-lvg-on-node-0"
	poolName  = "hhd-thin"
	lvCount   = 600
	size, err = resource.ParseQuantity("1Gi")

	resizeOn = false

	ctx   = context.Background()
	e2eCL client.Client

	resourcesSchemeFuncs = []func(*apiruntime.Scheme) error{
		v1alpha1.AddToScheme,
		clientgoscheme.AddToScheme,
		extv1.AddToScheme,
		v1.AddToScheme,
		sv1.AddToScheme,
	}
)

func BenchmarkRunThickLLVCreationSingleThread(b *testing.B) {
	b.Logf("starts the test")
	llvNames := make(map[string]bool, lvCount)

	b.StartTimer()
	for i := 0; i < lvCount; i++ {
		llv := configureTestThickLLV(fmt.Sprintf("test-llv-%d", i), lvgName)
		err := e2eCL.Create(ctx, llv)
		if err != nil {
			b.Logf("unable to create test LLV %s, err: %s", llv.Name, err.Error())
		}
		llvNames[llv.Name] = false
	}
	lvCreatedTime := b.Elapsed().Seconds()

	succeeded := 0
	for succeeded < len(llvNames) {
		llvs, err := getAllLLV(ctx, e2eCL)
		if err != nil {
			b.Error(err)
			continue
		}

		for llvName, created := range llvNames {
			if llv, found := llvs[llvName]; found {
				if llv.Status != nil {
					b.Logf("LV %s status %s", llvName, llv.Status.Phase)
				}
				if err != nil {
					b.Logf("can't check LLV %s llv", llvName)
					continue
				}

				if llv.Status != nil &&
					llv.Status.Phase == StatusPhaseCreated &&
					!created {
					succeeded++
					llvNames[llvName] = true

					if resizeOn {
						add, err := resource.ParseQuantity("1G")
						if err != nil {
							b.Logf(err.Error())
							continue
						}

						llv.Spec.Size.Add(add)
						err = e2eCL.Update(ctx, &llv)
						if err != nil {
							b.Logf(err.Error())
							continue
						}

						b.Logf("resize for LV %s succeeded", llvName)
					}
				}
			}

		}
	}
	b.Logf("[TIME] LLV resources were configured for %f", lvCreatedTime)
	b.Logf("All LLV were created for %f. Ends the test", b.Elapsed().Seconds())
}

func BenchmarkRunThinLLVCreationSingleThread(b *testing.B) {
	b.Logf("starts thin test")
	llvNames := make(map[string]bool, lvCount)

	b.StartTimer()
	for i := 0; i < lvCount; i++ {
		llv := configureTestThinLLV(fmt.Sprintf("test-llv-%d", i), lvgName, poolName)
		err := e2eCL.Create(ctx, llv)
		if err != nil {
			b.Logf("unable to create test LLV %s, err: %s", llv.Name, err.Error())
			continue
		}
		llvNames[llv.Name] = false
	}
	createdTime := b.Elapsed().Seconds()

	succeeded := 0
	for succeeded < len(llvNames) {
		llvs, err := getAllLLV(ctx, e2eCL)
		if err != nil {
			b.Error(err)
			continue
		}

		for llvName, visited := range llvNames {
			if llv, found := llvs[llvName]; found {
				if llv.Status != nil {
					b.Logf("LV %s status %s", llvName, llv.Status.Phase)
				}
				if err != nil {
					b.Logf("can't check LLV %s llv", llvName)
					continue
				}

				if llv.Status != nil &&
					llv.Status.Phase == StatusPhaseCreated &&
					!visited {
					succeeded++
					llvNames[llvName] = true

					if resizeOn {
						add, err := resource.ParseQuantity("1G")
						if err != nil {
							b.Logf(err.Error())
							continue
						}

						llv.Spec.Size.Add(add)
						err = e2eCL.Update(ctx, &llv)
						if err != nil {
							b.Logf(err.Error())
							continue
						}

						b.Logf("resize for LV %s succeeded", llvName)
					}
				}
			}

		}
	}
	b.Logf("All LLV were configured for %f. Ends the test", createdTime)
	b.Logf("All LLV were created in %f. Ends the test", b.Elapsed().Seconds())
}

func getAllLLV(ctx context.Context, cl client.Client) (map[string]v1alpha1.LVMLogicalVolume, error) {
	list := &v1alpha1.LVMLogicalVolumeList{}
	err := cl.List(ctx, list)
	if err != nil {
		return nil, err
	}

	res := make(map[string]v1alpha1.LVMLogicalVolume, len(list.Items))
	for _, lv := range list.Items {
		res[lv.Name] = lv
	}

	return res, nil
}

func configureTestThickLLV(name, lvgName string) *v1alpha1.LVMLogicalVolume {
	return &v1alpha1.LVMLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Finalizers: []string{internal.SdsNodeConfiguratorFinalizer},
		},
		Spec: v1alpha1.LVMLogicalVolumeSpec{
			ActualLVNameOnTheNode: name,
			Type:                  Thick,
			Size:                  size,
			LvmVolumeGroupName:    lvgName,
		},
	}
}

func configureTestThinLLV(name, lvgName, poolName string) *v1alpha1.LVMLogicalVolume {
	return &v1alpha1.LVMLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Finalizers: []string{internal.SdsNodeConfiguratorFinalizer},
		},
		Spec: v1alpha1.LVMLogicalVolumeSpec{
			ActualLVNameOnTheNode: name,
			Type:                  Thin,
			Size:                  size,
			LvmVolumeGroupName:    lvgName,
			Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: poolName},
		},
	}
}

func init() {
	config, err := kubutils.KubernetesDefaultConfigCreate()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		err := f(scheme)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}

	options := client.Options{
		Scheme: scheme,
	}

	e2eCL, err = client.New(config, options)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
