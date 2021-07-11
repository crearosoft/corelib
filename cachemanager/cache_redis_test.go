package cachemanager

import (
	"reflect"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

var (
	res         = "TestVal"
	failMarshal = func() {}
)

func setup(rc *RedisCache) {
	rc.Setup("127.0.0.1:6379", "", "", 0, time.Second*3)
	rc.flushDB()
}

func TestRedisCache_Set(t *testing.T) {
	var (
		rc        = &RedisCache{}
		testName1 = "SET"
		key1      = "test_set"
		val1      = "test_set_val"
		testName2 = "SET_MarshalErr"
		key2      = "test_set_err"
		val2      = failMarshal
	)

	setup(rc)
	type args struct {
		key string
		val interface{}
	}
	tests := []struct {
		name string
		rc   *RedisCache
		args args
	}{
		{
			name: testName1,
			rc:   rc,
			args: args{key: key1, val: val1},
		},
		{
			name: testName2,
			rc:   rc,
			args: args{key: key2, val: val2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rc.Set(tt.args.key, tt.args.val)
		})
	}

	res, ok := rc.Get(key1)
	if !ok {
		t.Error("failed to get value for ", key1)
		return
	}

	if v, ok := res.(string); !ok || v != val1 {
		t.Error("invalid value for ", key1)
	}
}

func TestRedisCache_SetWithExpiration(t *testing.T) {
	var (
		rc        = &RedisCache{}
		testName1 = "SET_EXP"
		key1      = "test_set_exp"
		val1      = "test_set_exp_val"
		testName2 = "SET_EXP_MarshalErr"
		key2      = "test_set_exp_err"
		val2      = failMarshal
	)

	setup(rc)
	type args struct {
		key string
		val interface{}
		exp time.Duration
	}
	tests := []struct {
		name string
		rc   *RedisCache
		args args
	}{
		{
			name: testName1,
			rc:   rc,
			args: args{key: key1, val: val1, exp: time.Second * 2},
		},
		{
			name: testName2,
			rc:   rc,
			args: args{key: key2, val: val2, exp: time.Second * 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rc.SetWithExpiration(tt.args.key, tt.args.val, tt.args.exp)
		})
	}

	time.Sleep(time.Second * 3)
	val, ok := rc.Get(key1)
	if !ok {
		return
	}

	if v, ok := val.(string); ok || v == val1 {
		t.Error("value not expired for", key1)
	}
}

func TestRedisCache_SetNoExpiration(t *testing.T) {
	var (
		rc        = &RedisCache{}
		testName1 = "SET_NOEXP"
		key1      = "test_set_noexp"
		val1      = "test_set_noexp_val"
		testName2 = "SET_NOEXP_MarshalErr"
		key2      = "test_set_noexp_err"
		val2      = failMarshal
	)

	setup(rc)
	type args struct {
		key string
		val interface{}
	}
	tests := []struct {
		name string
		rc   *RedisCache
		args args
	}{
		{
			name: testName1,
			rc:   rc,
			args: args{key: key1, val: val1},
		},
		{
			name: testName2,
			rc:   rc,
			args: args{key: key2, val: val2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rc.SetNoExpiration(tt.args.key, tt.args.val)
		})
	}

	time.Sleep(time.Second * 3)
	_, ok := rc.Get(key1)
	if !ok {
		t.Error("value expired for", key1)
	}
}

func TestRedisCache_Get(t *testing.T) {
	var (
		rc        = &RedisCache{}
		testName1 = "GET_NOT_PRESENT"
		testName2 = "GET_PRESENT"
		key1      = "test_get_not_present"
		key2      = "test_get_present"
		val2      = "test_get_present_val"
	)

	setup(rc)
	rc.Set(key2, val2)
	type args struct {
		key string
	}
	tests := []struct {
		name  string
		rc    *RedisCache
		args  args
		want  interface{}
		want1 bool
	}{
		{
			name:  testName1,
			rc:    rc,
			args:  args{key: key1},
			want:  nil,
			want1: false,
		},
		{
			name:  testName2,
			rc:    rc,
			args:  args{key: key2},
			want:  val2,
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.rc.Get(tt.args.key)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RedisCache.Get() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("RedisCache.Get() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestRedisCache_Delete(t *testing.T) {
	var (
		rc        = &RedisCache{}
		testName1 = "DEL"
		key1      = "test_del"
		val1      = "test_del_val"
	)

	setup(rc)
	rc.Set(key1, val1)
	type args struct {
		key string
	}
	tests := []struct {
		name string
		rc   *RedisCache
		args args
	}{
		{
			name: testName1,
			rc:   rc,
			args: args{key: key1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rc.Delete(tt.args.key)
		})
	}

	if _, ok := rc.Get(key1); ok {
		t.Error("value not deleted for", key1)
	}
}

func TestRedisCache_GetItemsCount(t *testing.T) {
	var (
		rc        = &RedisCache{}
		testName1 = "GET_CNT"
		key1      = "cnt_1"
		val1      = 1
		key2      = "cnt_2"
		val2      = 2
	)

	setup(rc)
	rc.Set(key1, val1)
	rc.Set(key2, val2)
	tests := []struct {
		name string
		rc   *RedisCache
		want int
	}{
		{
			name: testName1,
			rc:   rc,
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rc.GetItemsCount(); got != tt.want {
				t.Errorf("RedisCache.GetItemsCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisCache_Purge(t *testing.T) {
	var (
		rc        = &RedisCache{}
		testName1 = "PURGE"
		key1      = "test_purge"
		val1      = "test_purge_val"
	)

	setup(rc)
	rc.Set(key1, val1)
	tests := []struct {
		name string
		rc   *RedisCache
	}{
		// TODO: Add test cases.
		{
			name: testName1,
			rc:   rc,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rc.Purge()
		})
	}

	if _, ok := rc.Get(key1); ok {
		t.Error("value not deleted for", key1)
	}
}

func TestRedisCache_Setup(t *testing.T) {
	var (
		rc        *RedisCache = nil
		testName1             = "SETUP"
	)
	type args struct {
		addr     string
		password string
		db       int
		expr     time.Duration
		prefix   string
	}
	tests := []struct {
		name string
		rc   *RedisCache
		args args
	}{
		// TODO: Add test cases.
		{
			name: testName1,
			rc:   rc,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rc.Setup(tt.args.addr, tt.args.password, tt.args.prefix, tt.args.db, time.Second*3)
		})
	}
}

// BENCHMARKS >>

func BenchmarkMarshalString(b *testing.B) {
	s := `some string`
	for i := 0; i < b.N; i++ {
		_, _ = marshal(s)
	}
}

func BenchmarkMarshalWithTypeCheckString(b *testing.B) {
	s := `some string`
	for i := 0; i < b.N; i++ {
		_, _ = marshalWithTypeCheck(s)
	}
}

func BenchmarkMarshalBytes(b *testing.B) {
	s := []byte(`some string`)
	for i := 0; i < b.N; i++ {
		_, _ = marshal(s)
	}
}

func BenchmarkMarshalWithTypeCheckBytes(b *testing.B) {
	s := []byte(`some string`)
	for i := 0; i < b.N; i++ {
		_, _ = marshalWithTypeCheck(s)
	}
}
func BenchmarkMarshalGjsonVal(b *testing.B) {
	s := gjson.Parse(`{"name":"testcase"}`).Value()
	for i := 0; i < b.N; i++ {
		_, _ = marshal(s)
	}
}

func BenchmarkMarshalWithTypeCheckGjsonVal(b *testing.B) {
	s := gjson.Parse(`{"name":"testcase"}`).Value()
	for i := 0; i < b.N; i++ {
		_, _ = marshalWithTypeCheck(s)
	}
}

func BenchmarkMarshalStruct(b *testing.B) {
	type Struct struct {
		Name string `json:"name"`
	}

	s := Struct{"test"}
	for i := 0; i < b.N; i++ {
		_, _ = marshal(s)
	}
}

func BenchmarkMarshalWithTypeCheckStruct(b *testing.B) {
	type Struct struct {
		Name string `json:"name"`
	}

	s := Struct{"test"}
	for i := 0; i < b.N; i++ {
		_, _ = marshalWithTypeCheck(s)
	}
}

func TestRedisCache_GetAll(t *testing.T) {
	tests := []struct {
		name string
		rc   *RedisCache
		want map[string]interface{}
		init func(rc *RedisCache)
	}{
		{
			name: "Get All Items",
			rc:   &RedisCache{},
			want: map[string]interface{}{
				"a": 1.24,
				"b": 1.25,
			},
			init: func(rc *RedisCache) {
				rc.Setup("127.0.0.1:6379", "", "tests", 0, time.Second*60)
				rc.flushDB()

				rc.Set("a", 1.24)
				rc.Set("b", 1.25)
			},
		},
		{
			name: "Get All Items without prefix",
			rc:   &RedisCache{},
			want: map[string]interface{}{
				"a": 5.24,
				"b": 5.25,
			},
			init: func(rc *RedisCache) {
				rc.Setup("127.0.0.1:6379", "", "", 0, time.Second*60)
				rc.flushDB()

				rc.Set("a", 5.24)
				rc.Set("b", 5.25)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.init(tt.rc)
			if got := tt.rc.GetAll(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RedisCache.GetAll() = %v, want %v", got, tt.want)
			}
		})
	}
}
