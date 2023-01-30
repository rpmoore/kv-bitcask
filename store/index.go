package store

import (
	"sync"

	kv_bitcask "kv-bitbask"
)

type readWriteIndex struct {
	mapIndex sync.Map
}

var _ kv_bitcask.Index[uint32, *indexEntry] = new(readWriteIndex)

func newReadWriteIndex() *readWriteIndex {
	return &readWriteIndex{}
}

func (r *readWriteIndex) Get(key uint32) (*indexEntry, bool) {
	value, ok := r.mapIndex.Load(key)
	if !ok {
		return nil, false
	}
	entry, _ := value.(*indexEntry)
	return entry, true
}

func (r *readWriteIndex) Set(key uint32, value *indexEntry) error {
	r.mapIndex.Store(key, value)
	return nil
}

func (r *readWriteIndex) Range(method func(key uint32, value *indexEntry)) {
	r.mapIndex.Range(func(k any, v any) bool {
		key, _ := k.(uint32)
		value, _ := v.(*indexEntry)

		method(key, value)

		return true
	})
}

var _ kv_bitcask.Index[uint32, *indexEntry] = new(readOnlyIndex)

type readOnlyIndex struct {
	index map[uint32]*indexEntry
}

func (r *readOnlyIndex) Get(key uint32) (*indexEntry, bool) {
	return r.Get(key)
}

func (r *readOnlyIndex) Set(key uint32, value *indexEntry) error {
	r.index[key] = value
	return nil
}

func (r *readOnlyIndex) Range(method func(key uint32, value *indexEntry)) {
	for key, value := range r.index {
		method(key, value)
	}
}

func newReadOnlyIndex(index kv_bitcask.Index[uint32, *indexEntry]) *readOnlyIndex {
	localIndex := make(map[uint32]*indexEntry)

	index.Range(func(key uint32, value *indexEntry) {
		localIndex[key] = value
	})

	return &readOnlyIndex{
		index: localIndex,
	}
}
