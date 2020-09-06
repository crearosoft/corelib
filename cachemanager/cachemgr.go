package cachemanager

/**
 * @author Dhawal Dyavanpalli
 * @desc Created on 2020-08-31 12:31:03 am
 * @copyright Crearosoft
 */

// Package cachemdl will help cache object into memory. It Uses LRU algo

import (
	"encoding/json"
	"os"
	"time"

	"github.com/crearosoft/corelib/loggermanager"

	"github.com/patrickmn/go-cache"
)

// CacheHelper is a struct
type CacheHelper struct {
	Cache       *cache.Cache
	Expiration  time.Duration
	CleanupTime time.Duration
	MaxEntries  int
}

type cacheOption func(*CacheHelper)

// WithMaxEntries returns the MaxEntries that can be accepted
func WithMaxEntries(i int) cacheOption {
	return func(cfg *CacheHelper) {
		cfg.MaxEntries = i
	}
}

// WithExpiration returns the expiration time
func WithExpiration(exp time.Duration) cacheOption {
	return func(cfg *CacheHelper) {
		cfg.Expiration = exp
	}
}

// WithCleanupInterval returns the expired items clean up time
func WithCleanupInterval(ivl time.Duration) cacheOption {
	return func(cfg *CacheHelper) {
		cfg.CleanupTime = ivl
	}
}

// Setup initializes fastcache cache for application. Must be called only once.
func (cacheHelper *CacheHelper) Setup(maxEntries int, expiration time.Duration, cleanupTime time.Duration) {

	cacheHelper.MaxEntries = maxEntries
	cacheHelper.Expiration = expiration
	cacheHelper.Cache = cache.New(cacheHelper.Expiration, cacheHelper.CleanupTime)

}

// SetupCache initializes fastcache cache for application and returns its instance.
func SetupCache(opts ...cacheOption) *CacheHelper {
	fc := new(CacheHelper)

	for i := range opts {
		opts[i](fc)
	}

	fc.Cache = cache.New(fc.Expiration, fc.CleanupTime)
	return fc
}

// SaveFile is the
func (cacheHelper *CacheHelper) SaveFile(fname string) error {
	f, err := os.OpenFile(fname, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	itm := cacheHelper.GetItems()
	b, err := json.Marshal(itm)
	if err != nil {
		return loggermanager.Wrap("Error while marshalling the data")
	}
	if _, err = f.Write(b); err != nil {
		return loggermanager.Wrap("Error while writing the data to file")
	}
	return nil
}

// LoadFile to load saved file
func (cacheHelper *CacheHelper) LoadFile(fname string) error {
	const BufferSize = 100
	file, err := os.Open(fname)

	defer file.Close()
	if err != nil {
		return loggermanager.Wrap("Error while reading file")
	}
	// buffer := make([]byte, BufferSize)
	// // bytesread
	// for {
	// 	_, err := file.Read(buffer)

	// 	if err != nil {
	// 		if err != io.EOF {
	// 			fmt.Println(err)
	// 		}
	// 		break
	// 	}
	// }
	itm := make(map[string]cache.Item, 0)

	dec := json.NewDecoder(file)
	// dec := json.NewDecoder(strings.NewReader(string(buffer)))
	// if err := json.Unmarshal(buffer, &itm); err != nil {
	if err = dec.Decode(&itm); err != nil {
		// log.Fatal(err)
		nc := cache.New(cacheHelper.Expiration, cacheHelper.CleanupTime)
		cacheHelper.Cache = nc
		loggermanager.LogError("Error while binding the data from file")
		return loggermanager.Wrap("Error while binding the data from file")
	}

	nc := cache.NewFrom(cacheHelper.Expiration, cacheHelper.CleanupTime, itm)
	cacheHelper.Cache = nc
	// fmt.Println(string(buffer))
	// cacheHelper.Cache.Load
	return nil
}

// Get -
func (cacheHelper *CacheHelper) Get(key string) (interface{}, bool) {
	return cacheHelper.Cache.Get(key)
}

// GetItems -
func (cacheHelper *CacheHelper) GetItems() map[string]cache.Item {
	return cacheHelper.Cache.Items()
}

// SetNoExpiration -
func (cacheHelper *CacheHelper) SetNoExpiration(key string, object interface{}) {
	cacheHelper.Cache.Set(key, object, cache.NoExpiration)
}

// Set -
func (cacheHelper *CacheHelper) Set(key string, object interface{}) {
	cacheHelper.Cache.Set(key, object, cacheHelper.Expiration)
}

// SetWithExpiration -
func (cacheHelper *CacheHelper) SetWithExpiration(key string, object interface{}, duration time.Duration) {
	cacheHelper.Cache.Set(key, object, duration)
}

// Purge -
func (cacheHelper *CacheHelper) Purge() {
	cacheHelper.Cache.Flush()
}

// Delete -
func (cacheHelper *CacheHelper) Delete(key string) {
	cacheHelper.Cache.Delete(key)
}

// GetItemsCount : Number of items in the cache
func (cacheHelper *CacheHelper) GetItemsCount() int {
	return cacheHelper.Cache.ItemCount()
}

// Type returns the cache type
func (cacheHelper *CacheHelper) Type() int {
	return TypeCache
}

// GetAll returns all keys with values present in memory. **This is not intended for production use. May hamper performance**
func (cacheHelper *CacheHelper) GetAll() map[string]interface{} {
	items := cacheHelper.Cache.Items()

	result := make(map[string]interface{}, len(items))
	for k, v := range items {
		result[k] = v.Object
	}

	return result
}
