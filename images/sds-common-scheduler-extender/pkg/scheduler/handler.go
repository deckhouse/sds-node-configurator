package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

type FiltererPrioritizer interface {
	Filter(inputData ExtenderArgs) (*ExtenderFilterResult, error)
	Prioritize(inputData ExtenderArgs) ([]HostPriority, error)
}

type Handler struct {
	log       *logger.Logger
	scheduler FiltererPrioritizer
}

func NewHandler(log *logger.Logger, sheduler FiltererPrioritizer) *Handler {
	return &Handler{
		log:       log,
		scheduler: sheduler,
	}
}

func (h *Handler) Filter(w http.ResponseWriter, r *http.Request) {
	h.log.Debug("[Filter] starts filtering")

	inputData, ok := r.Context().Value("inputData").(ExtenderArgs)
	if !ok {
		h.log.Error(errors.New("pod data not found in context"), "[Filter] missing pod data")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	h.log.Trace(fmt.Sprintf("[Filter] filter input data: %+v", inputData))
	if inputData.Pod == nil {
		h.log.Error(errors.New("no pod in request"), "[Filter] no pod provided for filtering")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	result, err := h.scheduler.Filter(inputData)
	if err != nil {
		h.log.Error(err, "[Filter] filtering failed")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		h.log.Error(err, "[Filter] unable to encode filter response")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	h.log.Debug(fmt.Sprintf("[filter] completed filtering for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
}

func (h *Handler) Prioritize(w http.ResponseWriter, r *http.Request) {
	h.log.Debug("[Prioritize] starts serving")

	inputData, ok := r.Context().Value("inputData").(ExtenderArgs)
	if !ok {
		h.log.Error(errors.New("pod data not found in context"), "[Prioritize] missing pod data")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	h.log.Trace(fmt.Sprintf("[Prioritize] filter input data: %+v", inputData))
	if inputData.Pod == nil {
		h.log.Error(errors.New("no pod in request"), "[Prioritize] no pod provided for filtering")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	result, err := h.scheduler.Prioritize(inputData)
	if err != nil {
		h.log.Error(err, "[Prioritize] prioritization failed")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		h.log.Error(err, "[Prioritize] unable to encode response")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	h.log.Debug(fmt.Sprintf("[Prioritize] completed serving for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
}

func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	Status(w, r)
}
