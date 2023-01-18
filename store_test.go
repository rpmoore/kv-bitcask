package kv_bitcask

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecordMarshalUnmarshal(t *testing.T) {
	key := []byte("hello")
	value := []byte("I'm the value")

	record, err := newDataRecord(time.Now(), key, value)
	require.NoError(t, err)
	binaryRecord, err := record.MarshalBinary()
	require.NoError(t, err)
	t.Logf("binaryRecord: %s", hex.EncodeToString(binaryRecord))
	t.Logf("record: %v", record)

	readRecord := &dataRecord{}
	err = readRecord.UnmarshalBinary(binaryRecord)
	require.NoError(t, err)
	require.Equal(t, record, readRecord)
}

func TestDataFileReadWrite(t *testing.T) {
	file, err := newDataFile(1, t.TempDir(), FSSTATE_WRITE, NewRealClock())
	defer file.Close()
	require.NoError(t, err)

	key := []byte("foo")
	valueOrig := []byte("I'm a value")

	t.Logf("writing")
	_, err = file.Write(key, valueOrig)
	require.NoError(t, err)

	t.Logf("reading")
	value, err := file.Read(key)
	require.NoError(t, err)
	require.Equal(t, valueOrig, value)
	t.Logf("test complete")
}

func TestTwoRecords(t *testing.T) {
	file, err := newDataFile(1, t.TempDir(), FSSTATE_WRITE, NewRealClock())
	defer file.Close()
	require.NoError(t, err)

	key := []byte("foo")
	valueOrig := []byte("I'm a value")

	t.Logf("writing")
	_, err = file.Write(key, valueOrig)
	require.NoError(t, err)

	key2 := []byte("bark")
	value2Orig := []byte("around and around we go")
	_, err = file.Write(key2, value2Orig)
	require.NoError(t, err)

	t.Logf("reading")
	value, err := file.Read(key)
	require.NoError(t, err)
	require.Equal(t, valueOrig, value)
	value2, err := file.Read(key2)
	require.NoError(t, err)
	require.Equal(t, value2Orig, value2)
	t.Logf("test complete")
}
