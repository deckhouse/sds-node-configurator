/*
Copyright 2025 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tests

import (
	"crypto/sha1"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

var _ = Describe("BlockDevice Discovery E2E", func() {
	Context("Автоматическое обнаружение нового блочного устройства", func() {
		var (
			nodeName           string
			expectedDevicePath string
			expectedSerial     string
			expectedBDName     string
		)

		BeforeEach(func() {
			nodeName = GetNodeName()
			expectedDevicePath = GetExpectedDevicePath()
			expectedSerial = GetExpectedDeviceSerial()

			By(fmt.Sprintf("Используем ноду: %s", nodeName))
			By(fmt.Sprintf("Ожидаем устройство по пути: %s", expectedDevicePath))
			By(fmt.Sprintf("С серийным номером: %s", expectedSerial))

			// Вычисляем ожидаемое имя BlockDevice на основе серийника
			if expectedSerial != "" {
				expectedBDName = generateBlockDeviceName(nodeName, expectedSerial, "", "")
				By(fmt.Sprintf("Ожидаемое имя BlockDevice: %s", expectedBDName))
			}
		})

		It("Должен обнаружить новый неразмеченный диск и создать объект BlockDevice", func() {
			By("Шаг 1: Ожидание появления BlockDevice в кластере")

			var foundBD *v1alpha1.BlockDevice
			var blockDevicesList v1alpha1.BlockDeviceList

			// Ожидаем появления BlockDevice в течение 5 минут
			// (время может варьироваться в зависимости от интервала сканирования агента)
			Eventually(func(g Gomega) {
				err := k8sClient.List(ctx, &blockDevicesList, &client.ListOptions{})
				g.Expect(err).NotTo(HaveOccurred())

				// Ищем BlockDevice с нужным путем и нодой
				for i := range blockDevicesList.Items {
					bd := &blockDevicesList.Items[i]
					if bd.Status.NodeName == nodeName && bd.Status.Path == expectedDevicePath {
						foundBD = bd
						return
					}
				}

				g.Expect(foundBD).NotTo(BeNil(), fmt.Sprintf(
					"BlockDevice с path=%s и nodeName=%s не найден. Всего BlockDevices: %d",
					expectedDevicePath, nodeName, len(blockDevicesList.Items),
				))
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			By(fmt.Sprintf("Найден BlockDevice: %s", foundBD.Name))

			// Шаг 2: Проверка, что имя ресурса соответствует ожидаемому (если задан серийник)
			By("Шаг 2: Проверка имени BlockDevice на основе серийного номера")
			if expectedSerial != "" {
				Expect(foundBD.Name).To(Equal(expectedBDName),
					fmt.Sprintf("Имя BlockDevice не соответствует ожидаемому. "+
						"Ожидалось: %s, получено: %s", expectedBDName, foundBD.Name))
			}

			// Шаг 3: Проверка status.nodeName
			By("Шаг 3: Проверка status.nodeName")
			Expect(foundBD.Status.NodeName).To(Equal(nodeName),
				fmt.Sprintf("NodeName не соответствует ожидаемому. "+
					"Ожидалось: %s, получено: %s", nodeName, foundBD.Status.NodeName))

			// Шаг 4: Проверка status.path
			By("Шаг 4: Проверка status.path")
			Expect(foundBD.Status.Path).To(Equal(expectedDevicePath),
				fmt.Sprintf("Path не соответствует ожидаемому. "+
					"Ожидалось: %s, получено: %s", expectedDevicePath, foundBD.Status.Path))

			// Шаг 5: Проверка размера устройства
			By("Шаг 5: Проверка размера устройства (должен быть > 0)")
			Expect(foundBD.Status.Size.IsZero()).To(BeFalse(),
				"Размер устройства не должен быть равен 0")
			minSize := resource.MustParse("1Gi")
			Expect(foundBD.Status.Size.Cmp(minSize)).To(BeNumerically(">=", 0),
				fmt.Sprintf("Размер устройства должен быть >= 1Gi. Получено: %s",
					foundBD.Status.Size.String()))

			By(fmt.Sprintf("Размер устройства: %s", foundBD.Status.Size.String()))

			// Шаг 6: Проверка серийного номера
			By("Шаг 6: Проверка серийного номера")
			if expectedSerial != "" {
				Expect(foundBD.Status.Serial).To(Equal(expectedSerial),
					fmt.Sprintf("Серийный номер не соответствует ожидаемому. "+
						"Ожидалось: %s, получено: %s", expectedSerial, foundBD.Status.Serial))
			} else {
				Expect(foundBD.Status.Serial).NotTo(BeEmpty(),
					"Серийный номер устройства не должен быть пустым")
			}

			By(fmt.Sprintf("Серийный номер устройства: %s", foundBD.Status.Serial))

			// Шаг 7: Проверка, что устройство является потребляемым (consumable)
			By("Шаг 7: Проверка состояния consumable")
			Expect(foundBD.Status.Consumable).To(BeTrue(),
				"Устройство должно быть помечено как consumable для неразмеченного диска")

			// Шаг 8: Проверка типа устройства
			By("Шаг 8: Проверка типа устройства")
			Expect(foundBD.Status.Type).NotTo(BeEmpty(),
				"Тип устройства не должен быть пустым")
			By(fmt.Sprintf("Тип устройства: %s", foundBD.Status.Type))

			// Шаг 9: Проверка, что FSType пустой для неразмеченного диска
			By("Шаг 9: Проверка FSType (должен быть пустым для неразмеченного диска)")
			Expect(foundBD.Status.FSType).To(BeEmpty(),
				fmt.Sprintf("FSType должен быть пустым для неразмеченного диска, получено: %s",
					foundBD.Status.FSType))

			// Шаг 10: Проверка, что PVUuid пустой для неразмеченного диска
			By("Шаг 10: Проверка PVUuid (должен быть пустым)")
			Expect(foundBD.Status.PVUuid).To(BeEmpty(),
				"PVUuid должен быть пустым для неразмеченного диска")

			// Шаг 11: Проверка, что VGUuid пустой для неразмеченного диска
			By("Шаг 11: Проверка VGUuid (должен быть пустым)")
			Expect(foundBD.Status.VGUuid).To(BeEmpty(),
				"VGUuid должен быть пустым для неразмеченного диска")

			// Шаг 12: Проверка machineID
			By("Шаг 12: Проверка machineID")
			Expect(foundBD.Status.MachineID).NotTo(BeEmpty(),
				"MachineID не должен быть пустым")
			By(fmt.Sprintf("MachineID: %s", foundBD.Status.MachineID))

			// Итоговая информация
			By("✓ Все проверки пройдены успешно!")
			printBlockDeviceInfo(foundBD)
		})

		It("Должен корректно обрабатывать отключение устройства", func() {
			By("Примечание: Этот тест требует ручного отключения устройства")
			Skip("Автоматическое тестирование отключения устройства требует дополнительной инфраструктуры")
		})
	})
})

// generateBlockDeviceName генерирует имя BlockDevice на основе параметров
// Логика должна соответствовать функции createUniqDeviceName из discoverer.go
func generateBlockDeviceName(nodeName, serial, wwn, partUUID string) string {
	// Используем пустую модель, так как она неизвестна на этапе теста
	temp := fmt.Sprintf("%s%s%s%s%s", nodeName, wwn, "" /* model */, serial, partUUID)
	return fmt.Sprintf("dev-%x", sha1.Sum([]byte(temp)))
}

// printBlockDeviceInfo выводит подробную информацию о BlockDevice
func printBlockDeviceInfo(bd *v1alpha1.BlockDevice) {
	GinkgoWriter.Println("\n========== Информация о BlockDevice ==========")
	GinkgoWriter.Printf("Имя: %s\n", bd.Name)
	GinkgoWriter.Printf("NodeName: %s\n", bd.Status.NodeName)
	GinkgoWriter.Printf("Path: %s\n", bd.Status.Path)
	GinkgoWriter.Printf("Size: %s\n", bd.Status.Size.String())
	GinkgoWriter.Printf("Type: %s\n", bd.Status.Type)
	GinkgoWriter.Printf("Serial: %s\n", bd.Status.Serial)
	GinkgoWriter.Printf("WWN: %s\n", bd.Status.Wwn)
	GinkgoWriter.Printf("Model: %s\n", bd.Status.Model)
	GinkgoWriter.Printf("Consumable: %t\n", bd.Status.Consumable)
	GinkgoWriter.Printf("FSType: %s\n", bd.Status.FSType)
	GinkgoWriter.Printf("MachineID: %s\n", bd.Status.MachineID)
	GinkgoWriter.Printf("Rota: %t\n", bd.Status.Rota)
	GinkgoWriter.Printf("HotPlug: %t\n", bd.Status.HotPlug)
	GinkgoWriter.Println("=============================================\n")
}

