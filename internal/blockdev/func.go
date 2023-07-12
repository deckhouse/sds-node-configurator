package blockdev

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
	"os/exec"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	"storage-configurator/pkg/utils/errors/scerror"
	"strings"
	"time"
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
					if _, yes := seen[cand.Name]; yes {
						continue
					}
					// seen[cand.UUID.String()+"+"+cand.Name]
					//
					seen[cand.Name] = struct{}{}
					candiCh <- cand
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
			return fmt.Errorf("Cannot get storage pool candidates: %w", err)

		case cand := <-candiCh:
			if cand.SkipReason != "" {
				klog.Infof("Skip %s as it %s", cand.Name, cand.SkipReason)
				continue
			}
			klog.Infof("Processing %s", cand)

			// Create resource
		}
	}
}

func getCandidates(nodeName string) ([]Candidate, error) {
	var candidates []Candidate
	var candidateHandlers = []CandidateHandler{
		{
			Name:      "",
			Command:   echoCommand, // echoCommand or lsblkCommand
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
			return nil, err
		}
		cs, err := handler.ParseFunc(nodeName, outs)
		if err != nil {
			return nil, fmt.Errorf("faled to read %s devices: %s. Error was %s", handler.Name, err, errs.String())
		}
		candidates = append(candidates, cs...)
	}
	return candidates, nil
}

func parseFreeBlockDev(nodeName string, out bytes.Buffer) ([]Candidate, error) {
	var devices Devices
	err := json.Unmarshal(out.Bytes(), &devices)
	if err != nil {
		return nil, fmt.Errorf(scerror.ParseOutlsblkError+"%w", err)
	}

	tempMap := make(map[string]int)
	var r []Candidate

	for i, j := range devices.BlockDevices {
		tempMap[j.Kname] = i
		if j.Mountpoint == "" && j.Hotplug == false && !strings.HasPrefix(j.Name, DRBDName) {
			_, ok := tempMap[j.Pkname]
			if !ok {
				r = append(r, Candidate{
					NodeName: nodeName,
					Name:     buildNameDevices(devices.BlockDevices[i].Name[1:]),
					Path:     devices.BlockDevices[i].Name,
					Size:     devices.BlockDevices[i].Size,
					Model:    devices.BlockDevices[i].Model,
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
	hash := md5.Sum(tempBuff)
	tempName.WriteString(hex.EncodeToString(hash[:]))
	return tempName.String()
}

func newKubernetesEvent(nodeName string, involedObject v1.ObjectReference, reason, eventType, message string) v1.Event {
	eventTime := metav1.Now()
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
		Type:           v1.EventTypeNormal,
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
