package controller

import (
	"context"
	"testing"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/logger"
)

func TestRunBlockDeviceLabelsWatcher(t *testing.T) {
	ctx := context.Background()
	cl := NewFakeClient()
	log := logger.Logger{}

	t.Run("ReconcileBlockDeviceLabels_matches_selector_and_in_status_not_label_lvg", func(t *testing.T) {
		const (
			labelKey   = "test-key"
			labelValue = "test-value"

			bdName = "test-bd"
		)
		bd := &v1alpha1.BlockDevice{
			ObjectMeta: metav1.ObjectMeta{
				Name: bdName,
				Labels: map[string]string{
					labelKey: labelValue,
				},
			},
		}

		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "first-lvg",
			},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				BlockDeviceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						labelKey: labelValue,
					},
				},
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Nodes: []v1alpha1.LVMVolumeGroupNode{
					{
						Devices: []v1alpha1.LVMVolumeGroupDevice{
							{
								BlockDevice: bdName,
							},
						},
					},
				},
			},
		}

		err := cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		defer func() {
			err = cl.Delete(ctx, lvg)
			if err != nil {
				t.Error(err)
			}
		}()

		shouldRetry, err := reconcileBlockDeviceLabels(ctx, cl, log, bd)
		if assert.NoError(t, err) {
			assert.False(t, shouldRetry)

			newLvg := &v1alpha1.LVMVolumeGroup{}
			err = cl.Get(ctx, client.ObjectKey{
				Name: "first-lvg",
			}, newLvg)
			if err != nil {
				t.Error(err)
			}

			_, ok := newLvg.Labels[LVGUpdateTriggerLabel]
			assert.False(t, ok)
		}
	})

	t.Run("ReconcileBlockDeviceLabels_lvg_empty_status_not_label_lvg", func(t *testing.T) {
		const (
			labelKey   = "test-key"
			labelValue = "test-value"

			bdName = "test-bd"
		)
		bd := &v1alpha1.BlockDevice{
			ObjectMeta: metav1.ObjectMeta{
				Name: bdName,
				Labels: map[string]string{
					labelKey: labelValue,
				},
			},
		}

		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "first-lvg",
			},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				BlockDeviceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						labelKey: labelValue,
					},
				},
			},
		}

		err := cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		defer func() {
			err = cl.Delete(ctx, lvg)
			if err != nil {
				t.Error(err)
			}
		}()

		shouldRetry, err := reconcileBlockDeviceLabels(ctx, cl, log, bd)
		if assert.NoError(t, err) {
			assert.True(t, shouldRetry)
			newLvg := &v1alpha1.LVMVolumeGroup{}
			err = cl.Get(ctx, client.ObjectKey{
				Name: "first-lvg",
			}, newLvg)
			if err != nil {
				t.Error(err)
			}

			_, ok := newLvg.Labels[LVGUpdateTriggerLabel]
			assert.False(t, ok)
		}
	})

	t.Run("ReconcileBlockDeviceLabels_matches_selector_and_in_status_condition_false_label_lvg", func(t *testing.T) {
		const (
			labelKey   = "test-key"
			labelValue = "test-value"

			bdName = "test-bd"
		)
		bd := &v1alpha1.BlockDevice{
			ObjectMeta: metav1.ObjectMeta{
				Name: bdName,
				Labels: map[string]string{
					labelKey: labelValue,
				},
			},
		}

		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "first-lvg",
			},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				BlockDeviceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						labelKey: labelValue,
					},
				},
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Nodes: []v1alpha1.LVMVolumeGroupNode{
					{
						Devices: []v1alpha1.LVMVolumeGroupDevice{
							{
								BlockDevice: bdName,
							},
						},
					},
				},
				Conditions: []metav1.Condition{
					{
						Type:   internal.TypeVGConfigurationApplied,
						Status: metav1.ConditionFalse,
					},
				},
			},
		}

		err := cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		defer func() {
			err = cl.Delete(ctx, lvg)
			if err != nil {
				t.Error(err)
			}
		}()

		shouldRetry, err := reconcileBlockDeviceLabels(ctx, cl, log, bd)
		if assert.NoError(t, err) {
			assert.False(t, shouldRetry)
			newLvg := &v1alpha1.LVMVolumeGroup{}
			err = cl.Get(ctx, client.ObjectKey{
				Name: "first-lvg",
			}, newLvg)
			if err != nil {
				t.Error(err)
			}

			_, ok := newLvg.Labels[LVGUpdateTriggerLabel]
			assert.True(t, ok)
		}
	})

	t.Run("ReconcileBlockDeviceLabels_does_not_match_selector_and_not_in_status_not_label_lvg", func(t *testing.T) {
		const (
			labelKey   = "test-key"
			labelValue = "test-value"

			bdName = "test-bd"
		)
		bd := &v1alpha1.BlockDevice{
			ObjectMeta: metav1.ObjectMeta{
				Name: bdName,
				Labels: map[string]string{
					"other": "value",
				},
			},
		}

		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "first-lvg",
			},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				BlockDeviceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						labelKey: labelValue,
					},
				},
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Nodes: []v1alpha1.LVMVolumeGroupNode{
					{
						Devices: []v1alpha1.LVMVolumeGroupDevice{
							{
								BlockDevice: "other-bd",
							},
						},
					},
				},
			},
		}

		err := cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		defer func() {
			err = cl.Delete(ctx, lvg)
			if err != nil {
				t.Error(err)
			}
		}()

		shouldRetry, err := reconcileBlockDeviceLabels(ctx, cl, log, bd)
		if assert.NoError(t, err) {
			assert.False(t, shouldRetry)
			newLvg := &v1alpha1.LVMVolumeGroup{}
			err = cl.Get(ctx, client.ObjectKey{
				Name: "first-lvg",
			}, newLvg)
			if err != nil {
				t.Error(err)
			}

			_, ok := newLvg.Labels[LVGUpdateTriggerLabel]
			assert.False(t, ok)
		}
	})

	t.Run("ReconcileBlockDeviceLabels_matches_selector_but_not_in_status_label_lvg", func(t *testing.T) {
		const (
			labelKey   = "test-key"
			labelValue = "test-value"

			bdName = "test-bd"
		)
		bd := &v1alpha1.BlockDevice{
			ObjectMeta: metav1.ObjectMeta{
				Name: bdName,
				Labels: map[string]string{
					labelKey: labelValue,
				},
			},
		}

		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "first-lvg",
			},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				BlockDeviceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						labelKey: labelValue,
					},
				},
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Nodes: []v1alpha1.LVMVolumeGroupNode{
					{
						Devices: []v1alpha1.LVMVolumeGroupDevice{
							{
								BlockDevice: "other-bd",
							},
						},
					},
				},
			},
		}

		err := cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		defer func() {
			err = cl.Delete(ctx, lvg)
			if err != nil {
				t.Error(err)
			}
		}()

		shouldRetry, err := reconcileBlockDeviceLabels(ctx, cl, log, bd)
		if assert.NoError(t, err) {
			assert.False(t, shouldRetry)
			newLvg := &v1alpha1.LVMVolumeGroup{}
			err = cl.Get(ctx, client.ObjectKey{
				Name: "first-lvg",
			}, newLvg)
			if err != nil {
				t.Error(err)
			}

			_, ok := newLvg.Labels[LVGUpdateTriggerLabel]
			assert.True(t, ok)
		}
	})

	t.Run("ReconcileBlockDeviceLabels_does_not_match_selector_but_in_status_label_lvg", func(t *testing.T) {
		const (
			labelKey   = "test-key"
			labelValue = "test-value"

			bdName = "test-bd"
		)
		bd := &v1alpha1.BlockDevice{
			ObjectMeta: metav1.ObjectMeta{
				Name: bdName,
				Labels: map[string]string{
					"other-key": "other-value",
				},
			},
		}

		lvg := &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "first-lvg",
			},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				BlockDeviceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						labelKey: labelValue,
					},
				},
			},
			Status: v1alpha1.LVMVolumeGroupStatus{
				Nodes: []v1alpha1.LVMVolumeGroupNode{
					{
						Devices: []v1alpha1.LVMVolumeGroupDevice{
							{
								BlockDevice: bdName,
							},
						},
					},
				},
			},
		}

		err := cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		}

		defer func() {
			err = cl.Delete(ctx, lvg)
			if err != nil {
				t.Error(err)
			}
		}()

		shouldRetry, err := reconcileBlockDeviceLabels(ctx, cl, log, bd)
		if assert.NoError(t, err) {
			assert.False(t, shouldRetry)
			newLvg := &v1alpha1.LVMVolumeGroup{}
			err = cl.Get(ctx, client.ObjectKey{
				Name: "first-lvg",
			}, newLvg)
			if err != nil {
				t.Error(err)
			}

			_, ok := newLvg.Labels[LVGUpdateTriggerLabel]
			assert.True(t, ok)
		}
	})
}
