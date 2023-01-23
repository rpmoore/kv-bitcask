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

	"github.com/ncw/directio"
	"github.com/zhangxinngang/murmur"
)

type BitCask struct {
	currentWriter   writeDataFile
	closedDataFiles map[ID]writeDataFile
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

type writeDataFile struct {
	ID              ID
	writer          *os.File
	reader          *readDataFile
	lock            sync.Mutex
	clock           Clock
	index           index[uint32, *indexEntry]
	currentOffset   uint32
	closed          bool
	lastSync        time.Time
	maxSyncDuration time.Duration
	fillBuffer      *bytes.Buffer
}

func (d *writeDataFile) Close() error {
	if d == nil {
		return nil
	}
	d.lock.Lock()
	defer d.lock.Unlock()

	if d.closed {
		return nil
	}

	err := d.reader.Close()
	if err != nil {
		return err
	}

	return d.writer.Close()
}

func newDataFileWithFile(id ID, clock Clock, file *os.File, fileName string) (*writeDataFile, error) {
	index := newReadWriteIndex()                                      // load from a file in the future, without this we'll have to scan the full file to rebuild the index
	readOnly, err := newReadDataFileWithFullPath(id, fileName, index) // share the index with the reader, but only for the current writer, readers will have dedicated indexes when they're not a part of a writer
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
	}, nil
}

func newDataFileDirectIO(id ID, directoryPath string, clock Clock) (*writeDataFile, error) {
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

func newDataFile(id ID, directoryPath string, clock Clock) (*writeDataFile, error) {
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

func (d *writeDataFile) Write(key []byte, value []byte) (int, error) {
	currentTime := d.clock.Now()
	entry, err := newDataRecord(currentTime, key, value)
	if err != nil {
		return 0, err
	}

	// perform any marshalling and calculations outside the critical section to keep it small
	record, err := entry.MarshalBinary()
	if err != nil {
		return 0, err
	}
	recordLen := len(record)
	keyHash := entry.KeyHash()
	newIndexEntry := &indexEntry{
		KeyHash: keyHash,
		Length:  uint32(len(record)),
	}

	d.lock.Lock()
	defer d.lock.Unlock()

	if d.fillBuffer.Len()+recordLen > directio.BlockSize {
		err = d.writeBuffer()
		if err != nil {
			return 0, err
		}
		if d.lastSync.Add(d.maxSyncDuration).Before(currentTime) {
			err = d.writer.Sync()
			if err != nil {
				return 0, err
			}
			d.lastSync = currentTime
		}
	}

	d.fillBuffer.Write(record)

	newIndexEntry.Offset = d.currentOffset
	d.currentOffset = d.currentOffset + uint32(recordLen)
	err = d.index.Set(keyHash, newIndexEntry)
	if err != nil {
		return 0, err
	}
	return recordLen, err
}

func (d *writeDataFile) writeBuffer() error {
	_, err := d.writer.Write(d.fillBuffer.Bytes())
	if err != nil {
		return err
	}
	d.fillBuffer.Reset()
	return nil
}

func hash(key []byte) uint32 {
	return murmur.Murmur3(key)
}

func (d *writeDataFile) Read(key []byte) ([]byte, error) {
	return d.reader.Read(key)
}

func (d *writeDataFile) Flush() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	err := d.writeBuffer()
	if err != nil {
		return err
	}
	err = d.writer.Sync()
	if err != nil {
		return err
	}

	d.lastSync = d.clock.Now()
	return nil
}

func (d *writeDataFile) ConvertToReadOnly() (*readDataFile, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	err := d.writer.Close()
	if err != nil {
		return nil, err
	}
	d.closed = true

	return d.reader, nil
}
