package cachemanager

import (
	"time"
)

const (
	// TypeCache indicates fast cache as cache storage
	TypeCache = iota + 1
)

// Cache provides access to underlying cache, make sure all caches implement these methods.
type Cache interface {
	// Setters
	Set(key string, val interface{})
	SetWithExpiration(key string, val interface{}, exp time.Duration)
	SetNoExpiration(key string, val interface{})

	// Getters
	Get(key string) (interface{}, bool)
	GetAll() map[string]interface{}

	// Deletion operations
	Delete(key string)
	Purge()

	// GetItemsCount
	GetItemsCount() int

	Type() int
}
