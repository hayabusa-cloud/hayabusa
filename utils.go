package hybs

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"

	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

type Time time.Time

var minDateTime, maxDateTime = TimeNil(), Time(time.Unix(1<<48, 0))

func (t Time) Raw() time.Time {
	return time.Time(t)
}
func (t Time) Before(t2 Time) bool {
	return time.Time(t).Before(time.Time(t2))
}
func (t Time) After(t2 Time) bool {
	return time.Time(t).After(time.Time(t2))
}
func (t Time) Add(d time.Duration) Time {
	return Time(time.Time(t).Add(d))
}
func (t Time) AddDate(years, months, days int) Time {
	return Time(time.Time(t).AddDate(years, months, days))
}
func (t Time) Equal(u Time) bool {
	return time.Time(t).Equal(time.Time(u))
}
func (t Time) Sub(u Time) time.Duration {
	return t.Raw().Sub(u.Raw())
}
func (t Time) Unix() int64 {
	return t.Raw().Unix()
}
func (t Time) SecondAt() Time {
	hourString := t.Raw().Local().Format("2006-01-02 15:04:05")
	newTime, _ := TimeParse("2006-01-02 15:04:05", hourString)
	return Time(newTime.Raw().Local())
}
func (t Time) MinuteAt() Time {
	hourString := t.Raw().Local().Format("2006-01-02 15:04:00")
	newTime, _ := TimeParse("2006-01-02 15:04:05", hourString)
	return Time(newTime.Raw().Local())
}
func (t Time) HourAt() Time {
	hourString := t.Raw().Local().Format("2006-01-02 15:00:00")
	newTime, _ := TimeParse("2006-01-02 15:04:05", hourString)
	return Time(newTime.Raw().Local())
}
func (t Time) DayAt(offset time.Duration) Time {
	dayString := t.Raw().Local().Add(-offset).Format("2006-01-02")
	newTime, _ := TimeParse("2006-01-02", dayString)
	return Time(newTime.Add(offset).Raw().Local())
}
func (t Time) MonthAt(offset time.Duration) Time {
	monthString := t.Raw().Local().Add(-offset).Format("2006-01") + "-01"
	newTime, _ := TimeParse("2006-01-02", monthString)
	return newTime.Add(offset)
}
func (t Time) Hour() int {
	return t.Raw().Hour()
}
func (t Time) Minute() int {
	return t.Raw().Minute()
}
func (t Time) Second() int {
	return t.Raw().Second()
}
func (t Time) Truncate(d time.Duration) Time {
	return Time(t.Raw().Truncate(d))
}
func (t Time) Round(d time.Duration) Time {
	return Time(t.Raw().Round(d))
}
func (t Time) IsZero() bool {
	if t.Raw().IsZero() {
		return true
	}
	return t.Unix() == 0 && t.Raw().UnixNano() == 0
}
func (t Time) String() string {
	return time.Time(t).Local().String()
}
func (t Time) Format(layout string) string {
	return time.Time(t).Format(layout)
}
func (t Time) AppendFormat(b []byte, layout string) []byte {
	return time.Time(t).AppendFormat(b, layout)
}
func (t Time) LogString() string {
	return t.Raw().Local().Format(time.RFC3339)
}
func (t Time) Between(start *Time, end *Time) bool {
	if start != nil && start.After(t) {
		return false
	}
	if end != nil && end.Before(t) {
		return false
	}
	return true
}
func (t Time) MarshalJSON() ([]byte, error) {
	if t.IsZero() {
		return jsoniter.Marshal(-1)
	} else {
		return jsoniter.Marshal(t.Unix())
	}
}
func (t Time) MarshalBSONValue() (bsontype.Type, []byte, error) {
	if t.IsZero() {
		return bsontype.Null, nil, nil
	}
	var rawTime = time.Time(t)
	return bson.MarshalValue(rawTime)
}
func (t *Time) UnmarshalBSONValue(p bsontype.Type, data []byte) error {
	// check null pointer
	if data == nil || len(data) < 1 {
		*t = TimeNil()
		return nil
	}
	// check bson type
	if p != bsontype.DateTime {
		return fmt.Errorf("unsupported bsontype %s", p)
	}
	// decode BSON to datetime
	var bsonReader = bsonrw.NewBSONValueReader(p, data)
	var tick, err = bsonReader.ReadDateTime()
	if err != nil {
		return fmt.Errorf("unmarshal BSON datetime failed:%s", err)
	}
	var rawTime = minDateTime.Add(time.Millisecond * time.Duration(tick))
	*t = rawTime
	return nil
}
func (t *Time) Scan(value interface{}) error {
	*t = Time(value.(time.Time))
	return nil
}
func (t *Time) Value() (driver.Value, error) {
	return t.Raw(), nil
}
func TimeParse(layout string, value string) (Time, error) {
	rawTime, err := time.ParseInLocation(layout, value, time.Local)
	return Time(rawTime), err
}
func TimeParseAll(value string) (Time, error) {
	if t, err := TimeParse("2006-01-02 15:04:05", value); err == nil {
		return t, nil
	} else if t, err = TimeParse(time.RFC3339, value); err == nil {
		return t, nil
	} else if t, err = TimeParse(time.RFC3339Nano, value); err == nil {
		return t, nil
	} else if t, err = TimeParse(time.RFC822, value); err == nil {
		return t, nil
	} else if t, err = TimeParse(time.RFC822Z, value); err == nil {
		return t, nil
	} else if t, err = TimeParse(time.RFC850, value); err == nil {
		return t, nil
	} else if t, err = TimeParse(time.RFC1123, value); err == nil {
		return t, nil
	} else if t, err = TimeParse(time.RFC1123Z, value); err == nil {
		return t, nil
	} else if t, err = TimeParse(time.UnixDate, value); err == nil {
		return t, nil
	} else if t, err = TimeParse(time.RubyDate, value); err == nil {
		return t, nil
	}
	return TimeNil(), fmt.Errorf("parse datetime %s failed", value)
}
func TimeUnix(sec int64) Time {
	rawTime := time.Unix(sec, 0)
	return Time(rawTime)
}
func TimeNil() Time {
	return Time(time.Unix(0, 0))
}

type kvPair struct {
	key string
	val interface{}
}
type kvPairs []kvPair

func (dict kvPairs) Set(key string, val interface{}) (newDict kvPairs) {
	for i := 0; i < len(dict); i++ {
		if dict[i].key == key {
			dict[i].val = val
			return dict
		}
	}
	newDict = append(dict, kvPair{
		key: key,
		val: val,
	})
	return
}
func (dict kvPairs) Get(key string) (val interface{}, ok bool) {
	for i := 0; i < len(dict); i++ {
		if dict[i].key == key {
			return dict[i].val, true
		}
	}
	return nil, false
}

/* built-in error code */

type ErrorCode uint16

const (
	ErrorCodeSuccess            ErrorCode = 0
	ErrorCodeFailed             ErrorCode = 1
	ErrorCodeConnectionClosing  ErrorCode = 2
	ErrorCodeConnectionClosed   ErrorCode = 3
	ErrorCodeServerStopped      ErrorCode = 4
	ErrorCodeTooManyConnections ErrorCode = 5
	ErrorCodeConnectionTimeout  ErrorCode = 6
)

func (code ErrorCode) String() string {
	switch code {
	case ErrorCodeSuccess:
		return "success"
	case ErrorCodeFailed:
		return "failed"
	case ErrorCodeConnectionClosed:
		return "connection closed"
	case ErrorCodeServerStopped:
		return "server stopped"
	case ErrorCodeTooManyConnections:
		return "too many connections"
	case ErrorCodeConnectionTimeout:
		return "timeout"
	}
	return "unknown"
}

/* format and string */

func metaDisplay(n uint64) string {
	return fmt.Sprintf("%.1f MiB", float32(n)/1024.0)
}
func kiloDisplay(n uint64) string {
	return fmt.Sprintf("%.1f KiB", float32(n)/1024/1024.0)
}
func metaKiloDisplay(n uint64) string {
	if n >= 1024*1024*100 {
		return metaDisplay(n)
	} else {
		return kiloDisplay(n)
	}
}

/* about multi-thread */

var sleepCnt = uint8(0)

func sleep(frame ...int) {
	if len(frame) > 0 && frame[0] > 0 {
		time.Sleep(time.Millisecond * 10 * time.Duration(frame[0]))
	} else {
		sleepCnt++
		if sleepCnt&0x3f == 0 {
			time.Sleep(time.Millisecond * 10)
		}
	}
}

/* serialization */
var bsonValueReaderPool = bsonrw.NewBSONValueReaderPool()
var bsonValueWritePool = bsonrw.NewBSONValueWriterPool()

// val must be structure or pointer of structure
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

// val must be slice of structure or slice of structure pointer
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

/*
// val must be structure or pointer of structure
func DecodeBSONM(m bson.M, val interface{}) {

}

// val must be slice of structure or slice of structure pointer
func DecodeBSONMSlice(mSlice []interface{}, val interface{}) {

}
*/
func reflectValueSetInt(reflectVal reflect.Value, i interface{}) bool {
	if !reflectVal.CanSet() {
		return false
	}
	switch i.(type) {
	case int8:
		reflectVal.SetInt(int64(i.(int8)))
	case int16:
		reflectVal.SetInt(int64(i.(int16)))
	case int32:
		reflectVal.SetInt(int64(i.(int32)))
	case int64:
		reflectVal.SetInt(i.(int64))
	case int:
		reflectVal.SetInt(int64(i.(int)))
	case bool:
		if i.(bool) {
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
	switch i.(type) {
	case uint8:
		reflectVal.SetUint(uint64(i.(uint8)))
	case uint16:
		reflectVal.SetUint(uint64(i.(uint16)))
	case uint32:
		reflectVal.SetUint(uint64(i.(uint32)))
	case uint64:
		reflectVal.SetUint(i.(uint64))
	case uint:
		reflectVal.SetUint(uint64(i.(uint)))
	case bool:
		if i.(bool) {
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
	switch f.(type) {
	case float32:
		reflectVal.SetFloat(float64(f.(float32)))
	case float64:
		reflectVal.SetFloat(f.(float64))
	}
	return true
}
func reflectValueSetBool(reflectVal reflect.Value, b interface{}) bool {
	if !reflectVal.CanSet() {
		return false
	}
	switch b.(type) {
	case bool:
		reflectVal.SetBool(b.(bool))
	default:
		return false
	}
	return true
}
