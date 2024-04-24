package controller

import (
	"context"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/pkg/monitoring"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLVMVolumeGroupWatcherCtrl(t *testing.T) {
	cl := NewFakeClient()
	ctx := context.Background()
	metrics := monitoring.GetMetrics("")
	namespace := "test"

	t.Run("getLVMVolumeGroup_lvg_exists_returns_correct", func(t *testing.T) {
		const name = "test_name"
		testObj := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		}

		err := cl.Create(ctx, testObj)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, testObj)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		actual, err := getLVMVolumeGroup(ctx, cl, metrics, namespace, name)

		if assert.NoError(t, err) {
			assert.NotNil(t, actual)
			assert.Equal(t, name, actual.Name)
			assert.Equal(t, namespace, actual.Namespace)
		}
	})

	t.Run("getLVMVolumeGroup_lvg_doesnt_exist_returns_nil", func(t *testing.T) {
		const name = "test_name"
		testObj := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		}

		err := cl.Create(ctx, testObj)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, testObj)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		actual, err := getLVMVolumeGroup(ctx, cl, metrics, namespace, "another-name")

		if assert.EqualError(t, err, "lvmvolumegroups.storage.deckhouse.io \"another-name\" not found") {
			assert.Nil(t, actual)
		}
	})

	t.Run("updateLVMVolumeGroupHealthStatus_new_old_health_is_operational_doesnt_update_returns_nil", func(t *testing.T) {
		const (
			name    = "test_name"
			message = "All good"
		)
		testObj := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Status: v1alpha1.LvmVolumeGroupStatus{
				Health:  Operational,
				Message: message,
			},
		}

		err := cl.Create(ctx, testObj)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, testObj)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		oldLvg, err := getLVMVolumeGroup(ctx, cl, metrics, namespace, name)
		if assert.NoError(t, err) {
			assert.Equal(t, name, oldLvg.Name)
			assert.Equal(t, Operational, oldLvg.Status.Health)
			assert.Equal(t, message, oldLvg.Status.Message)
		}

		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, name, namespace, "new message", Operational)
		assert.Nil(t, err)

		updatedLvg, err := getLVMVolumeGroup(ctx, cl, metrics, namespace, name)
		if assert.NoError(t, err) {
			assert.Equal(t, name, updatedLvg.Name)
			assert.Equal(t, Operational, updatedLvg.Status.Health)
			assert.Equal(t, message, updatedLvg.Status.Message)
		}
	})

	t.Run("updateLVMVolumeGroupHealthStatus_new_old_health_and_messages_are_the_same_doesnt_updates_returns_nil", func(t *testing.T) {
		const (
			name    = "test_name"
			message = "All bad"
		)
		testObj := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Status: v1alpha1.LvmVolumeGroupStatus{
				Health:  NonOperational,
				Message: message,
			},
		}

		err := cl.Create(ctx, testObj)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, testObj)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		oldLvg, err := getLVMVolumeGroup(ctx, cl, metrics, namespace, name)
		if assert.NoError(t, err) {
			assert.Equal(t, name, oldLvg.Name)
			assert.Equal(t, NonOperational, oldLvg.Status.Health)
			assert.Equal(t, message, oldLvg.Status.Message)
		}

		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, name, namespace, message, NonOperational)
		assert.Nil(t, err)

		updatedLvg, err := getLVMVolumeGroup(ctx, cl, metrics, namespace, name)
		if assert.NoError(t, err) {
			assert.Equal(t, name, updatedLvg.Name)
			assert.Equal(t, NonOperational, updatedLvg.Status.Health)
			assert.Equal(t, message, updatedLvg.Status.Message)
		}
	})

	t.Run("updateLVMVolumeGroupHealthStatus_new_old_health_are_nonoperational_different_messages_are_updates_message_returns_nil", func(t *testing.T) {
		const (
			name       = "test_name"
			oldMessage = "All bad1"
			newMessage = "All bad2"
		)
		testObj := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Status: v1alpha1.LvmVolumeGroupStatus{
				Health:  NonOperational,
				Message: oldMessage,
			},
		}

		err := cl.Create(ctx, testObj)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, testObj)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		oldLvg, err := getLVMVolumeGroup(ctx, cl, metrics, namespace, name)
		if assert.NoError(t, err) {
			assert.Equal(t, name, oldLvg.Name)
			assert.Equal(t, NonOperational, oldLvg.Status.Health)
			assert.Equal(t, oldMessage, oldLvg.Status.Message)
		}

		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, name, namespace, newMessage, NonOperational)
		assert.Nil(t, err)

		updatedLvg, err := getLVMVolumeGroup(ctx, cl, metrics, namespace, name)
		if assert.NoError(t, err) {
			assert.Equal(t, name, updatedLvg.Name)
			assert.Equal(t, NonOperational, updatedLvg.Status.Health)
			assert.Equal(t, newMessage, updatedLvg.Status.Message)
		}
	})

	t.Run("updateLVMVolumeGroupHealthStatus_old_health_is_nonoperational_new_health_is_operational_updates_health_and_message_returns_nil", func(t *testing.T) {
		const (
			name       = "test_name"
			oldMessage = "All bad"
			newMessage = "All good"
		)
		testObj := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Status: v1alpha1.LvmVolumeGroupStatus{
				Health:  NonOperational,
				Message: oldMessage,
			},
		}

		err := cl.Create(ctx, testObj)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, testObj)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		oldLvg, err := getLVMVolumeGroup(ctx, cl, metrics, namespace, name)
		if assert.NoError(t, err) {
			assert.Equal(t, name, oldLvg.Name)
			assert.Equal(t, NonOperational, oldLvg.Status.Health)
			assert.Equal(t, oldMessage, oldLvg.Status.Message)
		}

		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, name, namespace, newMessage, Operational)
		assert.Nil(t, err)

		updatedLvg, err := getLVMVolumeGroup(ctx, cl, metrics, namespace, name)
		if assert.NoError(t, err) {
			assert.Equal(t, name, updatedLvg.Name)
			assert.Equal(t, Operational, updatedLvg.Status.Health)
			assert.Equal(t, newMessage, updatedLvg.Status.Message)
		}
	})

	t.Run("getBlockDevice_bd_exists_returns_correct_one", func(t *testing.T) {
		const name = "test_name"

		testObj := &v1alpha1.BlockDevice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		}

		err := cl.Create(ctx, testObj)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, testObj)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		bd, err := getBlockDevice(ctx, cl, metrics, namespace, name)
		if assert.NoError(t, err) {
			assert.Equal(t, name, bd.Name)
			assert.Equal(t, namespace, bd.Namespace)
		}
	})

	t.Run("getBlockDevice_bd_doesnt_exists_returns_nil", func(t *testing.T) {
		const name = "test_name"

		testObj := &v1alpha1.BlockDevice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		}

		err := cl.Create(ctx, testObj)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, testObj)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		bd, err := getBlockDevice(ctx, cl, metrics, namespace, "another-name")
		if assert.EqualError(t, err, "blockdevices.storage.deckhouse.io \"another-name\" not found") {
			assert.Nil(t, bd)
		}
	})

	t.Run("ValidateLVMGroup_lvg_is_nil_returns_error", func(t *testing.T) {
		valid, obj, err := CheckLVMVGNodeOwnership(ctx, cl, metrics, nil, "test_ns", "test_node")
		assert.False(t, valid)
		assert.Nil(t, obj)
		assert.EqualError(t, err, "lvmVolumeGroup is nil")
	})

	t.Run("ValidateLVMGroup_type_local_selected_absent_bds_validation_fails", func(t *testing.T) {
		const lvgName = "test_name"

		lvg := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      lvgName,
				Namespace: namespace,
			},
			Spec: v1alpha1.LvmVolumeGroupSpec{
				BlockDeviceNames: []string{"test_bd"},
				Type:             Local,
			},
		}

		err := cl.Create(ctx, lvg)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, lvg)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		valid, status, err := CheckLVMVGNodeOwnership(ctx, cl, metrics, lvg, namespace, "test_node")
		assert.False(t, valid)
		if assert.NotNil(t, status) {
			assert.Equal(t, NonOperational, status.Health)
			assert.EqualError(t, err, "error getBlockDevice: blockdevices.storage.deckhouse.io \"test_bd\" not found")
		}
	})

	t.Run("ValidateLVMGroup_type_local_selected_bds_from_different_nodes_validation_fails", func(t *testing.T) {
		const (
			name     = "test_name"
			firstBd  = "first"
			secondBd = "second"
			testNode = "test_node"
		)

		bds := &v1alpha1.BlockDeviceList{
			Items: []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      firstBd,
						Namespace: namespace,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: testNode,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secondBd,
						Namespace: namespace,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: "another_node",
					},
				},
			},
		}

		var err error
		for _, bd := range bds.Items {
			err = cl.Create(ctx, &bd)
			if err != nil {
				t.Error(err)
			}
		}

		if err == nil {
			defer func() {
				for _, bd := range bds.Items {
					err = cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()
		}

		testLvg := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: v1alpha1.LvmVolumeGroupSpec{
				BlockDeviceNames: []string{firstBd, secondBd},
				Type:             Local,
			},
		}

		err = cl.Create(ctx, testLvg)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, testLvg)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		valid, status, err := CheckLVMVGNodeOwnership(ctx, cl, metrics, testLvg, namespace, testNode)
		assert.False(t, valid)
		if assert.NotNil(t, status) {
			assert.Equal(t, NonOperational, status.Health)
			assert.Equal(t, "selected block devices are from different nodes for local LVMVolumeGroup", status.Message)
		}
		assert.Nil(t, err)
	})

	t.Run("ValidateLVMGroup_type_local_validation_passes", func(t *testing.T) {
		const (
			name     = "test_name"
			firstBd  = "first"
			secondBd = "second"
			testNode = "test_node"
		)

		bds := &v1alpha1.BlockDeviceList{
			Items: []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      firstBd,
						Namespace: namespace,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: testNode,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secondBd,
						Namespace: namespace,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName: testNode,
					},
				},
			},
		}

		var err error
		for _, bd := range bds.Items {
			err = cl.Create(ctx, &bd)
			if err != nil {
				t.Error(err)
			}
		}

		if err == nil {
			defer func() {
				for _, bd := range bds.Items {
					err = cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()
		}

		testLvg := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: v1alpha1.LvmVolumeGroupSpec{
				BlockDeviceNames:      []string{firstBd, secondBd},
				Type:                  Local,
				ActualVGNameOnTheNode: "some-vg",
			},
		}

		err = cl.Create(ctx, testLvg)
		if err != nil {
			t.Error(err)
		} else {
			defer func() {
				err = cl.Delete(ctx, testLvg)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		valid, status, err := CheckLVMVGNodeOwnership(ctx, cl, metrics, testLvg, namespace, testNode)
		assert.True(t, valid)
		if assert.NotNil(t, status) {
			assert.Equal(t, "", status.Health)
			assert.Equal(t, "", status.Message)
		}
		assert.Nil(t, err)
	})

	t.Run("CreateEventLVMVolumeGroup_creates_event", func(t *testing.T) {
		const (
			name     = "test_name"
			nodeName = "test_node"
		)

		testLvg := &v1alpha1.LvmVolumeGroup{
			TypeMeta: metav1.TypeMeta{
				Kind: "test_kind",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				UID:       "test_UUID",
			},
			Spec: v1alpha1.LvmVolumeGroupSpec{
				BlockDeviceNames: []string{"absent_bd"},
				Type:             Local,
			},
		}

		err := CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonDeleting, EventActionDeleting, nodeName, testLvg)
		if assert.NoError(t, err) {
			events := &v1.EventList{}
			err = cl.List(ctx, events)
			if err != nil {
				t.Error(err)
			}

			if assert.Equal(t, 1, len(events.Items)) {
				event := events.Items[0]

				assert.Equal(t, testLvg.Name+"-", event.GenerateName)
				assert.Equal(t, nameSpaceEvent, event.Namespace)
				assert.Equal(t, EventReasonDeleting, event.Reason)
				assert.Equal(t, testLvg.Name, event.InvolvedObject.Name)
				assert.Equal(t, testLvg.Kind, event.InvolvedObject.Kind)
				assert.Equal(t, testLvg.UID, event.InvolvedObject.UID)
				assert.Equal(t, "apiextensions.k8s.io/v1", event.InvolvedObject.APIVersion)
				assert.Equal(t, v1.EventTypeNormal, event.Type)
				assert.Equal(t, EventActionDeleting, event.Action)
				assert.Equal(t, nodeName, event.ReportingInstance)
				assert.Equal(t, LVMVolumeGroupWatcherCtrlName, event.ReportingController)
				assert.Equal(t, "Event Message", event.Message)

				err = cl.Delete(ctx, &event)
				if err != nil {
					t.Error(err)
				}
			}
		}
	})

	t.Run("ValidateConsumableDevices_validation_passes", func(t *testing.T) {
		const (
			name     = "test_name"
			firstBd  = "first"
			secondBd = "second"
			testNode = "test_node"
		)

		bds := &v1alpha1.BlockDeviceList{
			Items: []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      firstBd,
						Namespace: namespace,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName:   testNode,
						Consumable: true,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secondBd,
						Namespace: namespace,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName:   testNode,
						Consumable: true,
					},
				},
			},
		}

		var err error
		for _, bd := range bds.Items {
			err = cl.Create(ctx, &bd)
			if err != nil {
				t.Error(err)
			}
		}

		if err == nil {
			defer func() {
				for _, bd := range bds.Items {
					err = cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()
		}

		testLvg := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: v1alpha1.LvmVolumeGroupSpec{
				BlockDeviceNames: []string{firstBd, secondBd},
				Type:             Shared,
			},
		}

		passed, err := ValidateConsumableDevices(ctx, cl, metrics, testLvg)
		if assert.NoError(t, err) {
			assert.True(t, passed)
		}
	})

	t.Run("ValidateConsumableDevices_validation_fails", func(t *testing.T) {
		const (
			name     = "test_name"
			firstBd  = "first"
			secondBd = "second"
			testNode = "test_node"
		)

		bds := &v1alpha1.BlockDeviceList{
			Items: []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      firstBd,
						Namespace: namespace,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName:   testNode,
						Consumable: false,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secondBd,
						Namespace: namespace,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName:   testNode,
						Consumable: true,
					},
				},
			},
		}

		var err error
		for _, bd := range bds.Items {
			err = cl.Create(ctx, &bd)
			if err != nil {
				t.Error(err)
			}
		}

		if err == nil {
			defer func() {
				for _, bd := range bds.Items {
					err = cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()
		}

		testLvg := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: v1alpha1.LvmVolumeGroupSpec{
				BlockDeviceNames: []string{firstBd, secondBd},
				Type:             Shared,
			},
		}

		passed, err := ValidateConsumableDevices(ctx, cl, metrics, testLvg)
		if assert.NoError(t, err) {
			assert.False(t, passed)
		}
	})

	t.Run("ValidateConsumableDevices_lvg_is_nil_validation_fails", func(t *testing.T) {
		passed, err := ValidateConsumableDevices(ctx, cl, metrics, nil)
		if assert.EqualError(t, err, "lvmVolumeGroup is nil") {
			assert.False(t, passed)
		}
	})

	t.Run("GetPathsConsumableDevicesFromLVMVG_lvg_is_nil_returns_error", func(t *testing.T) {
		paths, err := GetPathsConsumableDevicesFromLVMVG(ctx, cl, metrics, nil)

		if assert.EqualError(t, err, "lvmVolumeGroup is nil") {
			assert.Nil(t, paths)
		}
	})

	t.Run("GetPathsConsumableDevicesFromLVMVG_lvg_is_nil_returns_error", func(t *testing.T) {
		const (
			name       = "test_name"
			firstBd    = "first"
			secondBd   = "second"
			testNode   = "test_node"
			firstPath  = "first_path"
			secondPath = "second_path"
		)

		bds := &v1alpha1.BlockDeviceList{
			Items: []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      firstBd,
						Namespace: namespace,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName:   testNode,
						Consumable: false,
						Path:       firstPath,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secondBd,
						Namespace: namespace,
					},
					Status: v1alpha1.BlockDeviceStatus{
						NodeName:   testNode,
						Consumable: true,
						Path:       secondPath,
					},
				},
			},
		}

		var err error
		for _, bd := range bds.Items {
			err = cl.Create(ctx, &bd)
			if err != nil {
				t.Error(err)
			}
		}

		if err == nil {
			defer func() {
				for _, bd := range bds.Items {
					err = cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()
		}

		testLvg := &v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: v1alpha1.LvmVolumeGroupSpec{
				BlockDeviceNames: []string{firstBd, secondBd},
				Type:             Shared,
			},
		}

		expected := []string{firstPath, secondPath}

		actual, err := GetPathsConsumableDevicesFromLVMVG(ctx, cl, metrics, testLvg)
		if assert.NoError(t, err) {
			assert.ElementsMatch(t, expected, actual)
		}

	})
}
