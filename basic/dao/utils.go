package dao

import (
	"reflect"
	"strings"
)

func SqlInValues(size int) string {
	placeholders := make([]string, size)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return "(" + strings.Join(placeholders, ",") + ")"
}

func SqlToParams(inputs ...interface{}) []interface{} {
	var result []interface{}
	for _, input := range inputs {
		// 利用反射判断输入是否为切片
		reflectedInput := reflect.ValueOf(input)
		if reflectedInput.Kind() == reflect.Slice {
			// 遍历切片，将元素逐一添加到结果切片
			for i := 0; i < reflectedInput.Len(); i++ {
				result = append(result, reflectedInput.Index(i).Interface())
			}
		} else {
			// 非切片类型直接添加到结果切片
			result = append(result, input)
		}
	}
	return result
}
