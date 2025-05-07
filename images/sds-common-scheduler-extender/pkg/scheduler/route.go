package scheduler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type scheduler struct {
	ctx            context.Context
	log            logger.Logger
	client         client.Client
	cacheMgr       *cache.CacheManager
	defaultDivisor float64
}

func (s *scheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("=URL= %s \n", r.URL.Path)
	switch r.URL.Path {
	case "/scheduler/filter":
		s.filter(w, r)
	case "/scheduler/prioritize":
		s.prioritize(w, r)
	case "/status":
		s.status(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func NewHandler(ctx context.Context, cl client.Client, log logger.Logger, cacheMgr *cache.CacheManager, defaultDiv float64) (http.Handler, error) {
	return &scheduler{
		defaultDivisor: defaultDiv,
		log:            log,
		client:         cl,
		ctx:            ctx,
		cacheMgr:       cacheMgr,
	}, nil
}

func (s *scheduler) status(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("ok"))
	if err != nil {
		fmt.Printf("error occurs on status route, err: %s\n", err.Error())
	}
}
