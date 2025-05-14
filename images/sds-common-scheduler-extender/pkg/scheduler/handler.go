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
	h.log.Debug("[handler] starts filtering")

	var inputData ExtenderArgs
	reader := http.MaxBytesReader(w, r.Body, 10<<20)
	if err := json.NewDecoder(reader).Decode(&inputData); err != nil {
		h.log.Error(err, "[handler] unable to decode filter request")
		httpError(w, "unable to decode request", http.StatusBadRequest)
		return
	}

	h.log.Trace(fmt.Sprintf("[handler] filter input data: %+v", inputData))
	if inputData.Pod == nil {
		h.log.Error(errors.New("no pod in request"), "[handler] no pod provided for filtering")
		httpError(w, "no pod in request", http.StatusBadRequest)
		return
	}

	result, err := h.scheduler.Filter(inputData)
	if err != nil {
		h.log.Error(err, "[handler] filtering failed")
		httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		h.log.Error(err, "[handler] unable to encode filter response")
		httpError(w, "internal error", http.StatusInternalServerError)
		return
	}

	h.log.Debug(fmt.Sprintf("[filter] completed filtering for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
}

func (h *Handler) Prioritize(w http.ResponseWriter, r *http.Request) {
	h.log.Debug("[prioritize] starts serving")

	var inputData ExtenderArgs
	reader := http.MaxBytesReader(w, r.Body, 10<<20)
	if err := json.NewDecoder(reader).Decode(&inputData); err != nil {
		h.log.Error(err, "[prioritize] unable to decode request")
		httpError(w, "unable to decode request", http.StatusBadRequest)
		return
	}

	h.log.Trace(fmt.Sprintf("[prioritize] input data: %+v", inputData))
	if inputData.Pod == nil {
		h.log.Error(errors.New("no pod in request"), "[prioritize] no pod provided")
		httpError(w, "no pod in request", http.StatusBadRequest)
		return
	}

	result, err := h.scheduler.Prioritize(inputData)
	if err != nil {
		h.log.Error(err, "[prioritize] prioritization failed")
		httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		h.log.Error(err, "[prioritize] unable to encode response")
		httpError(w, "internal error", http.StatusInternalServerError)
		return
	}

	h.log.Debug(fmt.Sprintf("[prioritize] completed serving for Pod %s/%s", inputData.Pod.Namespace, inputData.Pod.Name))
}

func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	status(w, r)
}
