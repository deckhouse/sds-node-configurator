package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// prioritize handles HTTP requests for node prioritization.
func (s *scheduler) prioritize(w http.ResponseWriter, r *http.Request) {
	s.log.Debug("[prioritize] starts serving")

	var inputData ExtenderArgs
	reader := http.MaxBytesReader(w, r.Body, 10<<20)
	if err := json.NewDecoder(reader).Decode(&inputData); err != nil {
		s.log.Error(err, "[prioritize] unable to decode request")
		httpError(w, "unable to decode request", http.StatusBadRequest)
		return
	}

	s.log.Trace(fmt.Sprintf("[prioritize] input data: %+v", inputData))
	if inputData.Pod == nil {
		s.log.Error(errors.New("no pod in request"), "[prioritize] no pod provided")
		httpError(w, "no pod in request", http.StatusBadRequest)
		return
	}

	result, err := s.processPrioritizeRequest(inputData)
	if err != nil {
		s.log.Error(err, "[prioritize] prioritization failed")
		httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		s.log.Error(err, "[prioritize] unable to encode response")
		httpError(w, "internal error", http.StatusInternalServerError)
		return
	}

	s.log.Debug(fmt.Sprintf("[prioritize] completed serving for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
}

func (s *scheduler) processPrioritizeRequest(inputData ExtenderArgs) ([]HostPriority, error) {
	nodeNames, err := getNodeNames(inputData)
	if err != nil {
		return nil, fmt.Errorf("unable to get node names: %w", err)
	}

	s.log.Debug(fmt.Sprintf("[prioritize] prioritizing for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
	s.log.Trace(fmt.Sprintf("[prioritize] Pod: %+v, Nodes: %+v", inputData.Pod, nodeNames))

	shouldProcess, _, err := shouldProcessPod(s.ctx, s.client, s.log, inputData.Pod, consts.SdsReplicatedVolumeProvisioner)
	if err != nil {
		return nil, fmt.Errorf("unable to check if Pod should be processed: %w", err)
	}

	if !shouldProcess {
		s.log.Debug(fmt.Sprintf("[prioritize] Pod %s/%s should not be processed", inputData.Pod.Namespace, inputData.Pod.Name))
		return createZeroScoreNodes(nodeNames), nil
	}

	input, err := s.collectPrioritizeInput(inputData.Pod, nodeNames)
	if err != nil {
		return nil, err
	}

	return scoreNodes(s, input)
}

// collectPrioritizeInput gathers all necessary data for prioritization.
func (s *scheduler) collectPrioritizeInput(pod *v1.Pod, nodeNames []string) (*PrioritizeInput, error) {
	pvcs, err := getUsedPVC(s.ctx, s.client, s.log, pod)
	if err != nil {
		return nil, fmt.Errorf("unable to get PVCs for Pod %s/%s: %w", pod.Name, pod.Namespace, err)
	}
	if len(pvcs) == 0 {
		return nil, errors.New("no PVCs found for Pod")
	}

	scs, err := getStorageClassesUsedByPVCs(s.ctx, s.client, pvcs)
	if err != nil {
		return nil, fmt.Errorf("unable to get StorageClasses: %w", err)
	}

	managedPVCs := filterNotManagedPVC(s.log, pvcs, scs)
	if len(managedPVCs) == 0 {
		s.log.Warning(fmt.Sprintf("[prioritize] Pod %s/%s uses unmanaged PVCs", pod.Namespace, pod.Name))
		return nil, errors.New("no managed PVCs found")
	}

	pvMap, err := getPersistentVolumes(s.ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("unable to get PersistentVolumes: %w", err)
	}

	pvcRequests, err := extractRequestedSize(s.log, managedPVCs, scs, pvMap)
	if err != nil {
		return nil, fmt.Errorf("unable to extract PVC request sizes: %w", err)
	}

	storagePoolList := &srv.ReplicatedStoragePoolList{}
	if err := s.client.List(s.ctx, storagePoolList); err != nil {
		return nil, fmt.Errorf("unable to list replicated storage pools: %w", err)
	}
	storagePoolMap := make(map[string]*srv.ReplicatedStoragePool, len(storagePoolList.Items))
	for _, storagePool := range storagePoolList.Items {
		storagePoolMap[storagePool.Name] = &storagePool
	}

	return &PrioritizeInput{
		Pod:            pod,
		NodeNames:      nodeNames,
		PVCs:           managedPVCs,
		StorageClasses: scs,
		PVCRequests:    pvcRequests,
		StoragePoolMap: storagePoolMap,
		DefaultDivisor: s.defaultDivisor,
	}, nil
}

// createZeroScoreNodes returns a list of nodes with zero scores.
func createZeroScoreNodes(nodeNames []string) []HostPriority {
	scores := make([]HostPriority, 0, len(nodeNames))
	for _, nodeName := range nodeNames {
		scores = append(scores, HostPriority{
			Host:  nodeName,
			Score: 0,
		})
	}
	return scores
}

func calculateFreeSpace(
	lvg *snc.LVMVolumeGroup,
	schedulerCache *cache.CacheManager,
	pvcReq *PVCRequest,
	commonLVG *LVMVolumeGroup,
	log logger.Logger,
	pvc *corev1.PersistentVolumeClaim,
	nodeName string,
) (resource.Quantity, error) {
	var freeSpace resource.Quantity

	switch pvcReq.DeviceType {
	case consts.Thick:
		freeSpace = lvg.Status.VGFree
		log.Trace(fmt.Sprintf("[scoreNodes] LVMVolumeGroup %s free Thick space before PVC reservation: %s", lvg.Name, freeSpace.String()))
		reserved, err := schedulerCache.GetLVGThickReservedSpace(lvg.Name)
		if err != nil {
			return freeSpace, errors.New(fmt.Sprintf("[scoreNodes] unable to count reserved space for the LVMVolumeGroup %s", lvg.Name))
		}
		log.Trace(fmt.Sprintf("[scoreNodes] LVMVolumeGroup %s PVC Space reservation: %s", lvg.Name, resource.NewQuantity(reserved, resource.BinarySI)))

		freeSpace = *resource.NewQuantity(freeSpace.Value()-reserved, resource.BinarySI)
		log.Trace(fmt.Sprintf("[scoreNodes] LVMVolumeGroup %s free Thick space after PVC reservation: %s", lvg.Name, freeSpace.String()))
	case consts.Thin:
		thinPool := findMatchedThinPool(lvg.Status.ThinPools, commonLVG.Thin.PoolName)
		if thinPool == nil {
			return freeSpace, errors.New(fmt.Sprintf("[scoreNodes] unable to match Storage Class's ThinPools with the node's one, Storage Class: %s, node: %s", *pvc.Spec.StorageClassName, nodeName))
		}

		freeSpace = thinPool.AvailableSpace
	}

	return freeSpace, nil
}

// TODO pick better naming to freeSize and method name +++
func getFreeSpaceLeftAsPercent(freeSpaceBytes, requestedSpace, totalSpace int64) int64 {
	freeSpaceLeft := freeSpaceBytes - requestedSpace
	fraction := float64(freeSpaceLeft) / float64(totalSpace)
	percent := fraction * 100
	return int64(percent)
}

// TODO change divisor to multiplier +++
func getNodeScore(freeSpace int64, multiplier float64) int {
	converted := int(math.Round(math.Log2(float64(freeSpace) * multiplier)))
	switch {
	case converted < 1:
		return 1
	case converted > 10:
		return 10
	default:
		return converted
	}
}
