package kv_bitcask

import (
	"encoding/hex"
	"fmt"
	"sync"
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
	file, err := newDataFile(1, t.TempDir(), NewRealClock())
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
	file, err := newDataFile(1, t.TempDir(), NewRealClock())
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

func TestConcurrentWrites(t *testing.T) {
	file, err := newDataFile(1, t.TempDir(), NewRealClock())
	defer file.Close()
	require.NoError(t, err)

	wg := sync.WaitGroup{}

	wg.Add(2)
	go func() {
		for i := 0; i < 10; i++ {
			fmt.Printf("writing1 %d\n", i)
			_, err := file.Write([]byte(fmt.Sprintf("writer1-%d", i)), []byte("I'm a payload"))
			require.NoError(t, err)
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 10; i++ {
			fmt.Printf("writing2 %d\n", i)
			_, err := file.Write([]byte(fmt.Sprintf("writer2-%d", i)), []byte("I'm a different payload"))
			require.NoError(t, err)
		}
		wg.Done()
	}()

	wg.Wait()

	for i := 0; i < 10; i++ {
		fmt.Printf("reading writer1 %d\n", i)
		value, err := file.Read([]byte(fmt.Sprintf("writer1-%d", i)))
		require.NoError(t, err)
		require.Equal(t, value, []byte("I'm a payload"))
	}

	for i := 0; i < 10; i++ {
		fmt.Printf("reading writer2 %d\n", i)
		value, err := file.Read([]byte(fmt.Sprintf("writer2-%d", i)))
		require.NoError(t, err)
		require.Equal(t, value, []byte("I'm a different payload"))
	}
}
