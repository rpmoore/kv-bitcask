package kv_bitcask

import "io"

type DataFile interface {
	io.Closer
	Read(key []byte) ([]byte, error)
}

type DataFileWriter interface {
	DataFile
	Write(key []byte, value []byte) error
	CloseToWriting() (DataFile, error)
}

type Index[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V) error
	Range(func(key K, value V))
}

type ID uint64
type BitCask struct {
	currentWriter   DataFileWriter
	closedDataFiles map[ID]DataFile
}

func (b *BitCask) Get(key []byte) ([]byte, error) {
	// need to determine how we'll do the lookup
	return nil, nil
}

func (b *BitCask) Set(key []byte, value []byte) error {
	// need to record which data file the write went to
	err := b.currentWriter.Write(key, value)
	return err
}

type Buffer interface {
	io.Closer
	GetOffset() int32
	Write(data []byte) error
}

type Allocator interface {
	io.Closer
	Allocate(size int) (Buffer, error)
	Flush() error
}
