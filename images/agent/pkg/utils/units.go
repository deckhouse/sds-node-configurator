package utils

import "k8s.io/apimachinery/pkg/api/resource"

func BytesToQuantity(size int64) string {
	tmp := resource.NewQuantity(size, resource.BinarySI)
	return tmp.String()
}

func QuantityToBytes(quantity string) (int64, error) {
	b, err := resource.ParseQuantity(quantity)
	if err != nil {
		return 0, err
	}
	return b.Value(), nil
}
