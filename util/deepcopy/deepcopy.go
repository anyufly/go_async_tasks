package deepcopy

import (
	"fmt"
	"reflect"
)

func DeepCopy(in interface{}) interface{} {
	return deepCopy(reflect.ValueOf(in)).Interface()
}

func deepCopy(src reflect.Value) reflect.Value {
	switch src.Kind() {
	case reflect.Interface, reflect.Ptr, reflect.Map, reflect.Slice:
		if src.IsNil() {
			return src
		}
	}

	switch src.Kind() {
	case reflect.Chan, reflect.Func, reflect.UnsafePointer, reflect.Uintptr:
		panic(fmt.Sprintf("cannot deep copy kind: %s", src.Kind()))
	case reflect.Array:
		dst := reflect.New(src.Type())
		for i := 0; i < src.Len(); i++ {
			dst.Elem().Index(i).Set(deepCopy(src.Index(i)))
		}
		return dst.Elem()
	case reflect.Interface:
		return deepCopy(src.Elem())
	case reflect.Map:
		dst := reflect.MakeMap(src.Type())
		for _, k := range src.MapKeys() {
			dst.SetMapIndex(k, deepCopy(src.MapIndex(k)))
		}
		return dst
	case reflect.Ptr:
		dst := reflect.New(src.Type().Elem())
		dst.Elem().Set(deepCopy(src.Elem()))
		return dst
	case reflect.Slice:
		dst := reflect.MakeSlice(src.Type(), 0, src.Len())
		for i := 0; i < src.Len(); i++ {
			dst = reflect.Append(dst, deepCopy(src.Index(i)))
		}
		return dst
	case reflect.Struct:
		dst := reflect.New(src.Type())
		for i := 0; i < src.NumField(); i++ {
			if !dst.Elem().Field(i).CanSet() {
				// 非导出变量浅拷贝
				return src
			}
			dst.Elem().Field(i).Set(deepCopy(src.Field(i)))
		}
		return dst.Elem()
	default:
		// 基础类型numbers、strings、bool
		return src
	}
}
