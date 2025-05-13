package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Middleware-функция
type Middleware func(http.Handler) http.Handler

// ApplyMiddlewares объединяет middleware в цепочку
func ApplyMiddlewares(handler http.Handler, middlewares ...Middleware) http.Handler {
	for i := range middlewares {
		handler = middlewares[i](handler)
	}
	return handler
}

func ShouldProcessPodMiddleware(ctx context.Context, cl client.Client, log *logger.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var inputData ExtenderArgs

			reader := http.MaxBytesReader(w, r.Body, 10<<20)
			if err := json.NewDecoder(reader).Decode(&inputData); err != nil {
				log.Error(err, "[ShouldProcessPodMiddleware] unable to decode filter request")
				httpError(w, "unable to decode request", http.StatusBadRequest)
				return
			}
			pod := inputData.Pod

			pvcs := &corev1.PersistentVolumeClaimList{}
			err := cl.List(ctx, pvcs)
			if err != nil {
				log.Error(err, "[shouldProcessPodMiddleware] error listing PVCs")
				http.Error(w, "error listing PVCs", http.StatusInternalServerError)
			}

			pvcMap := make(map[string]*corev1.PersistentVolumeClaim, len(pvcs.Items))
			for _, pvc := range pvcs.Items {
				pvcMap[pvc.Name] = &pvc
			}

			shouldProcess, volumes, err := shouldProcessPod(ctx, cl, pvcMap, log, pod, consts.SdsReplicatedVolumeProvisioner)
			if err != nil {
				log.Error(err, fmt.Sprintf("[shouldProcessPodMiddleware] error processing pod %s/%s: %v", pod.Namespace, pod.Name))
				http.Error(w, fmt.Sprintf("Error processing pod: %v", err), http.StatusInternalServerError)
				return
			}

			if !shouldProcess {
				log.Trace(fmt.Sprintf("[shouldProcessPodMiddleware] pod %s/%s should not be processed", pod.Namespace, pod.Name))
				result := &ExtenderFilterResult{NodeNames: inputData.NodeNames}
				if err := json.NewEncoder(w).Encode(result); err != nil {
					log.Error(err, "[ShouldProcessPodMiddleware] unable to decode filter request")
					httpError(w, "unable to decode request", http.StatusBadRequest)
					return
				}
				return
			}
			log.Trace(fmt.Sprintf("[shouldProcessPodMiddleware] pod %s/%s is eligible, matched volumes: %+v", pod.Namespace, pod.Name, volumes))
			next.ServeHTTP(w, r)
		})
	}
}
