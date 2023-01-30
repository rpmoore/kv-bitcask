package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	kv_bitcask "kv-bitbask"
)

var _ kv_bitcask.DataFile = new(readDataFile)

type readDataFile struct {
	ID     kv_bitcask.ID
	reader io.ReadSeekCloser // this only allows one thread to read
	index  kv_bitcask.Index[uint32, *indexEntry]
}

func newReadDataFile(id kv_bitcask.ID, directoryPath string, index kv_bitcask.Index[uint32, *indexEntry]) (*readDataFile, error) {
	fileName := fmt.Sprintf("datafile-%d", id)
	fileName = path.Join(directoryPath, fileName)
	dataFile, err := newReadDataFileWithFullPath(id, fileName, index)
	if err != nil {
		return nil, err
	}
	dataFile.ID = id
	return dataFile, nil
}

func newReadDataFileWithFullPath(id kv_bitcask.ID, filePath string, index kv_bitcask.Index[uint32, *indexEntry]) (*readDataFile, error) {
	readFile, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &readDataFile{
		ID:     id,
		reader: readFile,
		index:  index,
	}, nil
}

func (r *readDataFile) Close() error {
	return r.reader.Close()
}

func (r *readDataFile) Read(key []byte) ([]byte, error) {
	index, err := r.lookupIndex(hash(key))
	if err != nil {
		return nil, err
	}
	recordBytes, err := r.readRecordBytes(index)
	if err != nil {
		return nil, err
	}

	record := dataRecord{}
	err = record.UnmarshalBinary(recordBytes)
	if err != nil {
		return nil, err
	}

	err = record.VerifyChecksum()
	if err != nil {
		return nil, err
	}

	return record.Value, nil
}

func (r *readDataFile) readRecordBytes(index *indexEntry) ([]byte, error) {
	record := make([]byte, index.Length)
	// use a reader pool to allow for multiple readers and a customizable number of readers

	offset, err := r.reader.Seek(int64(index.Offset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	if offset != int64(index.Offset) {
		return nil, errors.New("did not seek to correct offset")
	}
	bytesRead, err := r.reader.Read(record)
	if err != nil {
		return nil, err
	}

	if int32(bytesRead) != index.Length {
		return nil, errors.New("did not read all of the data for the record")
	}

	return record, nil
}

func (r *readDataFile) lookupIndex(hash uint32) (*indexEntry, error) {
	indexEntry, ok := r.index.Get(hash)
	if !ok {
		return nil, errors.New("not found")
	}
	return indexEntry, nil
}
