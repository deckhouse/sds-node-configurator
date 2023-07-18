package blockdev

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"storage-configurator/pkg/utils/errors/scerror"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func ScanBlockDevices(ctx context.Context, kc kclient.Client, nodeName string, interval int) error {
	candiCh := make(chan Candidate)
	errCh := make(chan error)
	ticker := time.NewTicker(time.Duration(interval) * time.Second)

	go func() {
		seen := make(map[string]struct{})

		for {
			select {
			case <-ticker.C:
				candidates, err := getCandidates(nodeName)
				if err != nil {
					// only fatal error, cannot cmd
					errCh <- err
					return
				}
				// if duplicate
				for _, cand := range candidates {
					// seen[cand.UUID.String()+"+"+cand.Name]
					// if duplicate

					//if _, yes := seen[cand.Name]; yes {
					//	continue
					//}
					// seen[cand.UUID.String()+"+"+cand.Name]
					//
					seen[cand.Name] = struct{}{}
					candiCh <- cand
					//klog.Info("candidate W : ", cand)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-errCh:
			return fmt.Errorf("cannot get storage pool candidates: %w", err)

		case cand := <-candiCh:
			klog.Info("candidate : ", cand)
			if cand.SkipReason != "" {
				klog.Infof("Skip %s as it %s", cand.Name, cand.SkipReason)
				continue
			}
			// Create resource
		}
	}
}

func getCandidates(nodeName string) ([]Candidate, error) {
	var candidates []Candidate
	var candidateHandlers = []CandidateHandler{
		{
			Name:      "deviceX",
			Command:   lsblkCommand,
			ParseFunc: parseFreeBlockDev,
		},
	}
	for _, handler := range candidateHandlers {
		cmd := exec.Command(handler.Command[0], handler.Command[1:]...)

		var outs, errs bytes.Buffer
		cmd.Stdout = &outs
		cmd.Stderr = &errs

		err := cmd.Run()
		if err != nil {
			return nil, fmt.Errorf(scerror.ExeLSBLK, err, errs.String())
		}

		cs, err := handler.ParseFunc(nodeName, outs.Bytes())
		if err != nil {
			return nil, fmt.Errorf("faled to read %s devices: %s. Error was %s", handler.Name, err, err.Error())
		}
		candidates = append(candidates, cs...)
	}
	return candidates, nil
}

func parseFreeBlockDev(nodeName string, out []byte) ([]Candidate, error) {
	var devices Devices
	err := json.Unmarshal(out, &devices)
	if err != nil {
		return nil, fmt.Errorf(scerror.ParseOutlsblkError+"%w", err)
	}

	tempMapKName := make(map[string]int)
	tempMapPKName := make(map[string]int)
	var r []Candidate

	for i, j := range devices.BlockDevices {
		tempMapKName[j.KName] = i
		tempMapPKName[j.PkName] = i
	}

	for i, j := range devices.BlockDevices {
		if len(j.MountPoint) == 0 && !j.HotPlug && !strings.HasPrefix(j.Name, DRBDName) && j.Type != LoopDeviceType && len(j.FSType) == 0 {
			_, ok := tempMapPKName[j.KName]
			if !ok {
				r = append(r, Candidate{
					NodeName:   nodeName,
					Name:       buildNameDevices(devices.BlockDevices[i].Name[1:]),
					Path:       devices.BlockDevices[i].Name,
					Size:       devices.BlockDevices[i].Size,
					Model:      devices.BlockDevices[i].Model,
					MountPoint: devices.BlockDevices[i].MountPoint,
					FSType:     devices.BlockDevices[i].FSType,
				})
			}
		}
	}
	return r, nil
}

func buildNameDevices(name string) string {
	var tempName strings.Builder
	var tempBuff []byte
	tempName.WriteString(strings.Replace(name, "/", "-", -1))
	tempName.WriteString("-")
	tempName.Write(tempBuff)
	return tempName.String()
}

func newKubernetesEvent(nodeName string, involedObject v1.ObjectReference, reason, eventType, message string) v1.Event {
	eventTime := metav1.Now()

	if eventType == "" {
		eventType = v1.EventTypeNormal
	}

	event := v1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    v1.NamespaceDefault,
			GenerateName: involedObject.Name + ".",
			Labels: map[string]string{
				"app": AppName,
			},
		},
		Reason:         reason,
		Message:        message,
		InvolvedObject: involedObject,
		Source: v1.EventSource{
			Component: AppName,
			Host:      nodeName,
		},
		Count:          1,
		FirstTimestamp: eventTime,
		LastTimestamp:  eventTime,
		Type:           eventType,
	}
	return event
}

// Log and send creation event to Kubernetes
func report(ctx context.Context, kc kclient.Client, successful bool, nodeName string, involvedObject v1.ObjectReference, message string) error {
	var eventType, reason string
	if successful {
		eventType = v1.EventTypeNormal
		reason = "Created"
	} else {
		eventType = v1.EventTypeWarning
		reason = "Failed"
	}
	klog.Info(message)
	event := newKubernetesEvent(nodeName, involvedObject, reason, eventType, message)
	return kc.Create(ctx, &event)
}
