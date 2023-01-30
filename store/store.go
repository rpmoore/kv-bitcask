package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"sync"
	"time"

	"github.com/ncw/directio"
	"github.com/zhangxinngang/murmur"
	"kv-bitbask"
	"kv-bitbask/lockless"
)

var _ kv_bitcask.DataFileWriter = new(writeDataFile)

type writeDataFile struct {
	ID              kv_bitcask.ID
	writer          *os.File
	reader          *readDataFile
	lock            sync.Mutex
	clock           kv_bitcask.Clock
	index           kv_bitcask.Index[uint32, *indexEntry]
	currentOffset   uint32
	closed          bool
	lastSync        time.Time
	maxSyncDuration time.Duration
	fillBuffer      *bytes.Buffer
	allocator       kv_bitcask.Allocator
}

func (d *writeDataFile) Close() error {
	if d == nil {
		return nil
	}
	d.lock.Lock()
	defer d.lock.Unlock()

	err := d.allocator.Close()
	if err != nil {
		return err
	}

	if d.closed {
		return nil
	}

	err = d.reader.Close()
	if err != nil {
		return err
	}

	return d.writer.Close()
}

func newDataFileWithFile(id kv_bitcask.ID, clock kv_bitcask.Clock, file *os.File, fileName string) (*writeDataFile, error) {
	index := newReadWriteIndex()                                      // load from a file in the future, without this we'll have to scan the full file to rebuild the Index
	readOnly, err := newReadDataFileWithFullPath(id, fileName, index) // share the Index with the reader, but only for the current writer, readers will have dedicated indexes when they're not a part of a writer
	if err != nil {
		return nil, err
	}

	return &writeDataFile{
		writer:          file,
		reader:          readOnly,
		clock:           clock,
		index:           index,
		maxSyncDuration: time.Millisecond * 50,
		fillBuffer:      &bytes.Buffer{},
		allocator:       lockless.NewAllocator(file, 10, directio.BlockSize*100, clock),
	}, nil
}

func newDataFileDirectIO(id kv_bitcask.ID, directoryPath string, clock kv_bitcask.Clock) (*writeDataFile, error) {
	fileName := fmt.Sprintf("datafile-%d", id)
	fileName = path.Join(directoryPath, fileName)
	var writeFile *os.File
	var err error
	writeFile, err = directio.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return newDataFileWithFile(id, clock, writeFile, fileName)
}

func newDataFile(id kv_bitcask.ID, directoryPath string, clock kv_bitcask.Clock) (*writeDataFile, error) {
	fileName := fmt.Sprintf("datafile-%d", id)
	fileName = path.Join(directoryPath, fileName)
	var writeFile *os.File
	var err error
	writeFile, err = os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return newDataFileWithFile(id, clock, writeFile, fileName)
}

type indexEntry struct {
	KeyHash uint32
	Offset  int32
	Length  int32
}

type dataRecord struct {
	Timestamp int64
	CRC       uint32
	KeySize   uint32
	ValueSize uint32
	Key       []byte
	Value     []byte
}

func newDataRecord(time time.Time, key []byte, value []byte) (*dataRecord, error) {
	hasher := crc32.NewIEEE()
	_, err := hasher.Write(key)
	if err != nil {
		return nil, fmt.Errorf("failed to hash key: %w", err)
	}
	_, err = hasher.Write(value)
	if err != nil {
		return nil, fmt.Errorf("failed to hash value: %w", err)
	}
	return &dataRecord{
		Timestamp: time.Unix(),
		CRC:       hasher.Sum32(),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Key:       key,
		Value:     value,
	}, nil
}

func (d *dataRecord) KeyHash() uint32 {
	if d == nil {
		return 0
	}
	return hash(d.Key)
}

func (d *dataRecord) VerifyChecksum() error {
	if d == nil {
		return nil
	}
	hasher := crc32.NewIEEE()
	_, err := hasher.Write(d.Key)
	if err != nil {
		return err
	}
	_, err = hasher.Write(d.Value)
	if err != nil {
		return err
	}

	if hasher.Sum32() != d.CRC {
		return errors.New("invalid checkusm")
	}
	return nil
}

func (d *dataRecord) UnmarshalBinary(data []byte) error {
	readBuffer := bytes.NewBuffer(data)

	read := func(field any) func() error {
		return func() error {
			return binary.Read(readBuffer, binary.LittleEndian, field)
		}
	}

	readQueue := []func() error{
		read(&d.Timestamp),
		read(&d.CRC),
		read(&d.KeySize),
		read(&d.ValueSize),
		func() error {
			keyBytes := make([]byte, d.KeySize)

			bytesRead, err := readBuffer.Read(keyBytes)
			if err != nil {
				return err
			}
			if uint32(bytesRead) != d.KeySize {
				return fmt.Errorf("incomplete read of key exected: %d, received: %d", d.KeySize, bytesRead)
			}
			d.Key = keyBytes
			return nil
		},
		func() error {
			valueBytes := make([]byte, d.ValueSize)
			bytesRead, err := readBuffer.Read(valueBytes)
			if err != nil {
				return err
			}
			if uint32(bytesRead) != d.ValueSize {
				return fmt.Errorf("incomplete read of value exected: %d, received: %d", d.ValueSize, bytesRead)
			}
			d.Value = valueBytes
			return nil
		},
	}

	for _, op := range readQueue {
		err := op()
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *dataRecord) MarshalBinary() ([]byte, error) {
	writeBuffer := new(bytes.Buffer)

	write := func(data any) func() error {
		return func() error {
			return binary.Write(writeBuffer, binary.LittleEndian, data)
		}
	}

	writeQueue := []func() error{
		write(d.Timestamp),
		write(d.CRC),
		write(d.KeySize),
		write(d.ValueSize),
		func() error {
			writeBuffer.Write(d.Key)
			return nil
		},
		func() error {
			writeBuffer.Write(d.Value)
			return nil
		},
	}

	for _, op := range writeQueue {
		err := op()
		if err != nil {
			return nil, err
		}
	}

	return writeBuffer.Bytes(), nil
}

func (d *writeDataFile) Write(key []byte, value []byte) error {
	currentTime := d.clock.Now()
	entry, err := newDataRecord(currentTime, key, value)
	if err != nil {
		return err
	}

	// perform any marshalling and calculations outside the critical section to keep it small
	record, err := entry.MarshalBinary()
	if err != nil {
		return err
	}
	recordLen := len(record)
	keyHash := entry.KeyHash()
	newIndexEntry := &indexEntry{
		KeyHash: keyHash,
		Length:  int32(len(record)),
	}

	buffer, err := d.allocator.Allocate(recordLen)
	if err != nil {
		return err
	}
	defer buffer.Close()

	// fmt.Printf("updating Index\n")
	newIndexEntry.Offset = buffer.GetOffset()
	err = d.index.Set(keyHash, newIndexEntry)
	if err != nil {
		return err
	}

	// fmt.Printf("writing to buffer\n")
	err = buffer.Write(record)
	if err != nil {
		return err
	}

	return nil
}

func hash(key []byte) uint32 {
	return murmur.Murmur3(key)
}

func (d *writeDataFile) Read(key []byte) ([]byte, error) {
	return d.reader.Read(key)
}

func (d *writeDataFile) Flush() error {
	return d.allocator.Flush()
}

func (d *writeDataFile) CloseToWriting() (kv_bitcask.DataFile, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	err := d.writer.Close()
	if err != nil {
		return nil, err
	}
	d.closed = true

	return d.reader, nil
}
