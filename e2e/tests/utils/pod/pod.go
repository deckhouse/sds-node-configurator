package pod

import (
	"context"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func GetLogs(ctx context.Context, cs *kubernetes.Clientset, namespace, podName string, options corev1.PodLogOptions) (string, error) {
	req := cs.CoreV1().Pods(namespace).GetLogs(podName, &options)
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("[GetLogs] failed to stream logs: %v", err)
	}
	defer stream.Close()

	bytes, err := io.ReadAll(stream)
	if err != nil {
		return "", fmt.Errorf("[GetLogs] failed to read logs: %v", err)
	}

	return string(bytes), nil
}

func FindNameOnNode(ctx context.Context, k8sClient client.Client, nodeName string, options ...client.ListOption) (string, error) {
	var agentPodName string

	var podList corev1.PodList
	err := k8sClient.List(ctx, &podList, options...)
	if err != nil {
		return "", fmt.Errorf("[FindNameOnNode] failed to list pods: %v", err)
	}

	for _, pod := range podList.Items {
		if pod.Spec.NodeName != nodeName || pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		agentPodName = pod.Name
		break
	}

	return agentPodName, err
}
