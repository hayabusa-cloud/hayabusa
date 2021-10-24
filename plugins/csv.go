package plugins

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

type csvDataLoader struct {
	mux    string
	loader CsvDataLoader
	args   []string
}

var csvDataLoaderList = make([]*csvDataLoader, 0)

// PrepareLoadCsvData prepares loading csv data
func PrepareLoadCsvData(mux string, loader CsvDataLoader, args ...string) {
	csvDataLoaderList = append(csvDataLoaderList, &csvDataLoader{
		mux:    mux,
		loader: loader,
		args:   args,
	})
}

// CsvDataLoader must be a type of function like:
// func (data *DateRecordStruct, container *sync.Map)
// keys of container are id or key of records
// values of container are pointers of DataRecordStruct
type CsvDataLoader interface{} // func (interface{}, *sync.Map)

type csvDataLoaderFn func(folder string, engine Interface) (err error)

func csvDataLoadFunc(mux string, loader CsvDataLoader, args ...string) (f csvDataLoaderFn, err error) {
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
	f, err = func(folder string, engine Interface) (err error) {
		var fullFilepath = filepath.Join(folder, mux, filename)
		// read csv file
		recordFile, err := os.OpenFile(fullFilepath, os.O_RDONLY, 0400)
		defer func() {
			if err = recordFile.Close(); err != nil {
				fmt.Println(err.Error())
			}
		}()
		if err != nil {
			return fmt.Errorf("open record file %s error:%s", filename, err)
		}
		var csvReader = csv.NewReader(recordFile)
		if csvReader == nil {
			return fmt.Errorf("new csv reader error:%s", filepath.Join(mux, filename))
		}
		rows, err := csvReader.ReadAll()
		if err != nil {
			return fmt.Errorf("read csv file=%s data rows error:%s", fullFilepath, err)
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
		engine.MasterData().StoreTable(in0Type.Name(), mux, dataContainer.Interface())
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
			return fmt.Errorf("convert %s to int error:%s", (*cols)[0], err)
		}
		dataVal.SetInt(int64(intVal))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		uintVal, err := strconv.Atoi((*cols)[0])
		if err != nil {
			return fmt.Errorf("convert %s to int error:%s", (*cols)[0], err)
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
			return fmt.Errorf("convert %s to float error:%s", (*cols)[0], err)
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

type CsvManager struct {
	m      *sync.Map // MUX__$mux__TYPENAME$typename->*sync.Map
	fnList []csvDataLoaderFn
}

func NewCsvManager() (csvManager *CsvManager, err error) {
	csvManager = &CsvManager{
		fnList: make([]csvDataLoaderFn, 0),
	}
	for _, loader := range csvDataLoaderList {
		if fn, err := csvDataLoadFunc(loader.mux, loader.loader, loader.args...); err != nil {
			return nil, err
		} else {
			csvManager.fnList = append(csvManager.fnList, fn)
		}
	}
	return
}

func (manager *CsvManager) Load(folder string, engine Interface) (err error) {
	manager.m = &sync.Map{}
	for _, fn := range manager.fnList {
		if err = fn(folder, engine); err != nil {
			return fmt.Errorf("load csv data error:%s", err)
		}
	}
	if len(manager.fnList) > 0 {
		fmt.Printf("%d csv table(s) under %s loaded\n", len(manager.fnList), folder)
	}
	return
}

func (manager *CsvManager) Table(name string, mux string) (*sync.Map, bool) {
	if val, ok := manager.m.Load(fmt.Sprintf("MUX__%sTYPENAME__%s", mux, name)); !ok {
		return nil, false
	} else {
		return val.(*sync.Map), ok
	}
}
func (manager *CsvManager) StoreTable(name string, mux string, container interface{}) {
	manager.m.Store(fmt.Sprintf("MUX__%sTYPENAME__%s", mux, name), container)
}

type csvDataTable struct {
	logger    Logger
	mux       string
	tableName string
	key       interface{}
	container *sync.Map
}

func MasterTableWithLogger(logger Logger, mux string, tableName string, container *sync.Map) MasterTable {
	return &csvDataTable{logger: logger, mux: mux, tableName: tableName, container: container}
}

func (table *csvDataTable) Record(key interface{}) (val interface{}, ok bool) {
	table.key = key
	if table.container == nil {
		return nil, false
	}
	val, ok = table.container.Load(key)
	if table.logger != nil {
		if ok {
			table.logger.Infof("loaded csv data key=%v", key)
			table.logger.Infof("%#v", val)
		} else {
			table.logger.Infof("master csv key=%v not exists", key)
		}
	}
	return
}
func (table *csvDataTable) Range(f func(key, value interface{}) bool) {
	if table.container == nil {
		return
	}
	if table.logger != nil {
		table.logger.Infof("for each master table:%s", table.tableName)
	}
	table.container.Range(f)
}

// MasterTable is interface masterDataTable implements
type MasterTable interface {
	Record(key interface{}) (val interface{}, ok bool)
	Range(f func(key, value interface{}) bool)
}

// MasterDataManager is interface that CsvManager implements
type MasterDataManager interface {
	Table(name string, mux string) (*sync.Map, bool)
	StoreTable(name string, mux string, container interface{})
}
