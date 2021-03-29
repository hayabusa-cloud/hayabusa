package hybs

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type defaultMasterDataLoader struct {
	mux    string
	loader MasterDataLoader
	args   []string
}

var defaultMasterDataLoaderList []*defaultMasterDataLoader

// PrepareLoadMasterData prepares loading master data
func PrepareLoadMasterData(mux string, loader MasterDataLoader, args ...string) {
	defaultMasterDataLoaderList = append(defaultMasterDataLoaderList, &defaultMasterDataLoader{
		mux:    mux,
		loader: loader,
		args:   args,
	})
}

// MasterDataLoader must be a type of function like:
// func (data *DateRecordStruct, container *sync.Map)
// keys of container are id or key of records
// values of container are pointers of DataRecordStruct
type MasterDataLoader interface{} // func (interface{}, *sync.Map)

type masterDataLoaderFunc func(svr *hybsEngine) (err error)

func masterDataLoadFunc(mux string, loader MasterDataLoader, args ...string) (f masterDataLoaderFunc, err error) {
	if loader == nil {
		return nil, fmt.Errorf("function is nil")
	}
	var reflectLoader = reflect.Indirect(reflect.ValueOf(loader))
	if reflectLoader.Kind() != reflect.Func {
		return nil, fmt.Errorf("loader is not a function")
	}
	var reflectLoaderType = reflectLoader.Type()
	if reflectLoaderType.NumIn() < 2 {
		return nil, fmt.Errorf("loader function must have 2 in args")
	}
	in0Type := reflectLoaderType.In(0)
	for in0Type.Kind() == reflect.Ptr {
		in0Type = in0Type.Elem()
	}
	var in1Type = reflectLoaderType.In(1)
	for in1Type.Kind() == reflect.Ptr {
		in1Type = in1Type.Elem()
	}
	if in1Type.Name() != "Map" || in1Type.PkgPath() != "sync" {
		return nil, fmt.Errorf("in arg1 must be *sync.Map")
	}
	var filename = in0Type.Name() + ".csv"
	if len(args) > 0 {
		filename = args[0]
	}
	f, err = func(eng *hybsEngine) (err error) {
		fullFilename := filepath.Join(eng.config.CsvFilepath, mux, filename)
		// read csv file
		recordFile, err := os.OpenFile(fullFilename, os.O_RDONLY, 0400)
		defer recordFile.Close()
		if err != nil {
			return fmt.Errorf("open record file %s failed:%s", filename, err)
		}
		var csvReader = csv.NewReader(recordFile)
		if csvReader == nil {
			return fmt.Errorf("new csv reader failed:%s", filepath.Join(mux, filename))
		}
		rows, err := csvReader.ReadAll()
		if err != nil {
			return fmt.Errorf("read csv file=%s data rows failed:%s", fullFilename, err)
		}
		var dataContainer = reflect.New(in1Type)
		if len(rows) > 1 {
			for line, row := range rows[1:] {
				// unmarshal csv line
				dataRecord, err := unmarshalCsvRecord(row, in0Type)
				if err != nil {
					return fmt.Errorf("%s line:%d:%s", in0Type.Name(), line, err)
				}
				outArgs := reflectLoader.Call([]reflect.Value{dataRecord.Addr(), dataContainer})
				for _, outArg := range outArgs {
					if outArg.Type().Name() == "error" && !outArg.IsNil() {
						return fmt.Errorf("%s line:%d:%s", in0Type.Name(), line, outArg.Interface())
					}
				}
			}
		}
		eng.masterDataMgr.storeTable(in0Type.Name(), mux, dataContainer.Interface())
		// fmt.Printf("master table %s loaded\n", fullFilename)
		return err
	}, nil
	return f, err
}

func unmarshalCsvRecord(row []string, dataType reflect.Type) (dataRecord reflect.Value, err error) {
	dataVal := reflect.New(dataType).Elem()
	err = unmarshalCsvRecordSetField(&row, dataType, dataVal)
	if err != nil {
		return dataVal, err
	}
	return dataVal, err
}

func unmarshalCsvRecordSetField(cols *[]string, dataType reflect.Type, dataVal reflect.Value) error {
	if cols == nil || len(*cols) < 1 {
		return fmt.Errorf("end of row:%s", dataType.Name())
	}
	switch dataType.Kind() {
	case reflect.Ptr:
		return unmarshalCsvRecordSetField(cols, dataType.Elem(), dataVal.Elem())
	case reflect.Interface:
		return unmarshalCsvRecordSetField(cols, dataType.Elem(), dataVal.Elem())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		intVal, err := strconv.Atoi((*cols)[0])
		if err != nil {
			return fmt.Errorf("convert %s to int failed:%s", (*cols)[0], err)
		}
		dataVal.SetInt(int64(intVal))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		uintVal, err := strconv.Atoi((*cols)[0])
		if err != nil {
			return fmt.Errorf("convert %s to int failed:%s", (*cols)[0], err)
		}
		dataVal.SetUint(uint64(uintVal))
	case reflect.Bool:
		if (*cols)[0] == "true" {
			dataVal.SetBool(true)
		} else {
			dataVal.SetBool(false)
		}
	case reflect.String:
		dataVal.SetString((*cols)[0])
	case reflect.Float32, reflect.Float64:
		floatVal, err := strconv.ParseFloat((*cols)[0], 64)
		if err != nil {
			return fmt.Errorf("convert %s to float failed:%s", (*cols)[0], err)
		}
		dataVal.SetFloat(floatVal)
	case reflect.Struct:
		for fieldIndex := 0; fieldIndex < dataType.NumField(); fieldIndex++ {
			if len(*cols) < 1 {
				return fmt.Errorf("too many fields in structure")
			}
			structTag := dataType.Field(fieldIndex).Tag
			tagStr, ok := structTag.Lookup("hybs")
			if ok {
				if strings.Contains(tagStr, "record:\"ignore\"") {
					continue
				}
			}
			err := unmarshalCsvRecordSetField(cols, dataType.Field(fieldIndex).Type, dataVal.Field(fieldIndex))
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invailed data type:%s", dataType.Name())
	}
	if len(*cols) > 0 {
		*cols = (*cols)[1:]
	}
	return nil
}

type masterDataManager struct {
	*sync.Map // MUX__$mux__TYPENAME$typename->*sync.Map
}

func (mgr *masterDataManager) loadTable(name string, mux string) (*sync.Map, bool) {
	if val, ok := mgr.Load(fmt.Sprintf("MUX__%sTYPENAME__%s", name, mux)); !ok {
		return nil, false
	} else {
		return val.(*sync.Map), ok
	}
}
func (mgr *masterDataManager) storeTable(name string, mux string, container interface{}) {
	mgr.Store(fmt.Sprintf("MUX__%sTYPENAME__%s", name, mux), container)
}

type masterDataLogger interface {
	logf(format string, args ...interface{})
}
type masterDataTable struct {
	l         masterDataLogger
	mux       string
	tableName string
	key       interface{}
	container *sync.Map
}

func (table *masterDataTable) Record(key interface{}) (val interface{}, ok bool) {
	table.key = key
	if table.container == nil {
		return nil, false
	}
	val, ok = table.container.Load(key)
	if table.l != nil {
		if ok {
			table.l.logf("loaded master data key=%v", key)
			table.l.logf("%#v", val)
		} else {
			table.l.logf("master data key=%v not exists", key)
		}
	}
	return
}
func (table *masterDataTable) Range(f func(key, value interface{}) bool) {
	if table.container == nil {
		return
	}
	if table.l != nil {
		table.l.logf("for each master table:%s", table.tableName)
	}
	table.container.Range(f)
}

// MasterTable is interface masterDataTable implements
type MasterTable interface {
	Record(key interface{}) (val interface{}, ok bool)
	Range(f func(key, value interface{}) bool)
}

func init() {
	defaultMasterDataLoaderList = make([]*defaultMasterDataLoader, 0)
}
