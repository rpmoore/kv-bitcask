package kv_bitcask

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

	"github.com/zhangxinngang/murmur"
)

type BitCask struct {
	currentWriter   dataFile
	closedDataFiles map[ID]dataFile
}

func (b *BitCask) Get(key []byte) ([]byte, error) {
	// need to determine how we'll do the lookup
	return nil, nil
}

func (b *BitCask) Set(key []byte, value []byte) error {
	// need to record which data file the write went to
	_, err := b.currentWriter.Write(key, value)
	return err
}

type ID uint64

type dataFile struct {
	ID            ID
	writer        *os.File
	reader        *readDataFile
	lock          sync.RWMutex
	readerLock    sync.Mutex // adding temporarily until a read pool is created
	clock         Clock
	index         map[uint32]*indexEntry
	currentOffset uint32
}

func (d *dataFile) Close() error {
	if d == nil {
		return nil
	}
	d.lock.Lock()
	defer d.lock.Unlock()

	err := d.reader.Close()
	if err != nil {
		return err
	}

	return d.writer.Close()
}

func newDataFile(id ID, directoryPath string, clock Clock) (*dataFile, error) {
	fileName := fmt.Sprintf("datafile-%d", id)
	fileName = path.Join(directoryPath, fileName)
	var writeFile *os.File
	var err error
	writeFile, err = os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	index := make(map[uint32]*indexEntry)                         // load from a file in the future, without this we'll have to scan the full file to rebuild the index
	readOnly, err := newReadDataFileWithFullPath(fileName, index) // share the index with the reader, but only for the current writer, readers will have dedicated indexes when they're not a part of a writer
	if err != nil {
		return nil, err
	}

	return &dataFile{
		writer: writeFile,
		reader: readOnly,
		clock:  clock,
		index:  index,
	}, nil
}

type indexEntry struct {
	KeyHash uint32
	Offset  uint32
	Length  uint32
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

func (d *dataFile) Write(key []byte, value []byte) (int, error) {
	entry, err := newDataRecord(d.clock.Now(), key, value)
	if err != nil {
		return 0, err
	}

	// perform any marshalling and calculations outside the critical section to keep it small
	record, err := entry.MarshalBinary()
	if err != nil {
		return 0, err
	}
	keyHash := entry.KeyHash()
	newIndexEntry := &indexEntry{
		KeyHash: keyHash,
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	bytesWritten, err := d.writer.Write(record)
	if err != nil {
		return 0, err
	}
	err = d.writer.Sync()
	if err != nil {
		return 0, err
	}
	newIndexEntry.Length = uint32(bytesWritten)
	if bytesWritten != len(record) {
		return 0, fmt.Errorf("length of data written incorrect, expected: %d, got: %d", len(record), bytesWritten)
	}

	newIndexEntry.Offset = d.currentOffset
	d.currentOffset = d.currentOffset + uint32(bytesWritten)
	d.index[keyHash] = newIndexEntry
	return bytesWritten, err
}

func hash(key []byte) uint32 {
	return murmur.Murmur3(key)
}

func (d *dataFile) Read(key []byte) ([]byte, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	return d.reader.Read(key)
}
