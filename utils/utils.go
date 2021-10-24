package utils

import (
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
)

//lint:file-ignore U1000 do not check unused in this file

func NumMetaString(n uint64) string {
	return fmt.Sprintf("%.1f MiB", float32(n)/1024.0)
}
func NumKiloString(n uint64) string {
	return fmt.Sprintf("%.1f KiB", float32(n)/1024/1024.0)
}
func NumMetaKiloString(n uint64) string {
	if n >= 1024*1024*100 {
		return NumMetaString(n)
	} else {
		return NumKiloString(n)
	}
}

// serialization
var bsonValueReaderPool = bsonrw.NewBSONValueReaderPool()
var bsonValueWritePool = bsonrw.NewBSONValueWriterPool()

// DecodeBSON decodes bson type d to object val
// parameter val must be structure or pointer of structure
func DecodeBSON(d interface{}, val interface{}) (err error) {
	// val cannot be nil
	if val == nil {
		return fmt.Errorf("val is nil")
	}
	var reflectVal = reflect.ValueOf(val)
	// contents of val must be structure
	if reflectVal.Type().Kind() != reflect.Struct {
		return fmt.Errorf("val must be structure or pointer of structure")
	}
	return decodeBSONImpl(d, reflectVal)
}
func decodeBSONImpl(d interface{}, reflectVal reflect.Value) error {
	for reflectVal.Kind() == reflect.Ptr && reflectVal.IsNil() {
		return fmt.Errorf("cannot decode bson to a nil pointer")
	}
	reflectVal = reflect.Indirect(reflectVal)
	var addr = reflectVal.Addr().Interface()
	// marshal bson
	var bytes, err = bson.Marshal(d)
	if err != nil {
		return err
	}
	if err = bson.Unmarshal(bytes, addr); err != nil {
		return err
	}
	return nil
}

// DecodeBSONSlice decodes bson type dSlice to object val
// parameter val must be slice of structure or slice of structure pointer
func DecodeBSONSlice(dSlice []interface{}, val interface{}) (err error) {
	// get data contents
	var reflectSlice = reflect.ValueOf(val)
	for reflectSlice.Type().Kind() == reflect.Ptr {
		if reflectSlice.IsNil() {
			return fmt.Errorf("val is nil")
		}
		reflectSlice = reflectSlice.Elem()
	}
	if reflectSlice.Type().Kind() != reflect.Slice {
		return fmt.Errorf("val must be slice or pointer of slice")
	}
	// check slice len
	if len(dSlice) != reflectSlice.Len() {
		return fmt.Errorf("len of slice unmatch")
	}
	// loop slice elem
	for i, d := range dSlice {
		// get val elem interface{}
		var reflectElem = reflectSlice.Index(i)
		if reflectElem.Kind() == reflect.Ptr && reflectElem.IsNil() {
			reflectElem = reflect.New(reflectElem.Type().Elem())
			reflectSlice.Index(i).Set(reflectElem)
		}
		if err = decodeBSONImpl(d, reflectElem); err != nil {
			return err
		}
	}
	return nil
}

func reflectValueSetInt(reflectVal reflect.Value, i interface{}) bool {
	if !reflectVal.CanSet() {
		return false
	}
	switch x := i.(type) {
	case int8:
		reflectVal.SetInt(int64(x))
	case int16:
		reflectVal.SetInt(int64(x))
	case int32:
		reflectVal.SetInt(int64(x))
	case int64:
		reflectVal.SetInt(x)
	case int:
		reflectVal.SetInt(int64(x))
	case bool:
		if x {
			reflectVal.SetInt(1)
		} else {
			reflectVal.SetInt(0)
		}
	}
	return true
}
func reflectValueSetUint(reflectVal reflect.Value, i interface{}) bool {
	if !reflectVal.CanSet() {
		return false
	}
	switch x := i.(type) {
	case uint8:
		reflectVal.SetUint(uint64(x))
	case uint16:
		reflectVal.SetUint(uint64(x))
	case uint32:
		reflectVal.SetUint(uint64(x))
	case uint64:
		reflectVal.SetUint(x)
	case uint:
		reflectVal.SetUint(uint64(x))
	case bool:
		if x {
			reflectVal.SetUint(1)
		} else {
			reflectVal.SetUint(0)
		}
	}
	return true
}
func reflectValueSetFloat(reflectVal reflect.Value, f interface{}) bool {
	if !reflectVal.CanSet() {
		return false
	}
	switch x := f.(type) {
	case float32:
		reflectVal.SetFloat(float64(x))
	case float64:
		reflectVal.SetFloat(x)
	}
	return true
}
func reflectValueSetBool(reflectVal reflect.Value, b interface{}) bool {
	if !reflectVal.CanSet() {
		return false
	}
	switch x := b.(type) {
	case bool:
		reflectVal.SetBool(x)
	default:
		return false
	}
	return true
}
