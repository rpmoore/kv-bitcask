package store

import (
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"kv-bitbask"
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
	file, err := newDataFile(1, t.TempDir(), kv_bitcask.NewRealClock())
	defer file.Close()
	require.NoError(t, err)

	key := []byte("foo")
	valueOrig := []byte("I'm a value")

	fmt.Printf("writing value\n")
	err = file.Write(key, valueOrig)
	require.NoError(t, err)

	fmt.Printf("flushing\n")
	require.NoError(t, file.Flush())

	value, err := file.Read(key)
	require.NoError(t, err)
	require.Equal(t, valueOrig, value)
}

func TestTwoRecords(t *testing.T) {
	file, err := newDataFile(1, t.TempDir(), kv_bitcask.NewRealClock())
	defer file.Close()
	require.NoError(t, err)

	key := []byte("foo")
	valueOrig := []byte("I'm a value")

	t.Logf("writing")
	err = file.Write(key, valueOrig)
	require.NoError(t, err)

	key2 := []byte("bark")
	value2Orig := []byte("around and around we go")
	err = file.Write(key2, value2Orig)
	require.NoError(t, err)

	require.NoError(t, file.Flush())

	value, err := file.Read(key)
	require.NoError(t, err)
	require.Equal(t, valueOrig, value)
	value2, err := file.Read(key2)
	require.NoError(t, err)
	require.Equal(t, value2Orig, value2)
}

func TestTwoRecordsWithInnerFlush(t *testing.T) {
	file, err := newDataFile(1, t.TempDir(), kv_bitcask.NewRealClock())
	defer file.Close()
	require.NoError(t, err)

	key := []byte("foo")
	valueOrig := []byte("I'm a value")

	t.Logf("writing")
	err = file.Write(key, valueOrig)
	require.NoError(t, err)

	require.NoError(t, file.Flush())

	key2 := []byte("bark")
	value2Orig := []byte("around and around we go")
	err = file.Write(key2, value2Orig)
	require.NoError(t, err)

	require.NoError(t, file.Flush())

	value, err := file.Read(key)
	require.NoError(t, err)
	require.Equal(t, valueOrig, value)
	value2, err := file.Read(key2)
	require.NoError(t, err)
	require.Equal(t, value2Orig, value2)
}

func TestConcurrentWrites(t *testing.T) {
	file, err := newDataFile(1, t.TempDir(), kv_bitcask.NewRealClock())
	defer file.Close()
	require.NoError(t, err)

	wg := sync.WaitGroup{}

	wg.Add(2)
	go func() {
		for i := 0; i < 10; i++ {
			err := file.Write([]byte(fmt.Sprintf("writer1-%d", i)), []byte("I'm a payload"))
			require.NoError(t, err)
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 10; i++ {
			err := file.Write([]byte(fmt.Sprintf("writer2-%d", i)), []byte("I'm a different payload"))
			require.NoError(t, err)
		}
		wg.Done()
	}()

	wg.Wait()

	require.NoError(t, file.Flush())

	for i := 0; i < 10; i++ {
		value, err := file.Read([]byte(fmt.Sprintf("writer1-%d", i)))
		require.NoError(t, err)
		require.Equal(t, []byte("I'm a payload"), value)
	}

	for i := 0; i < 10; i++ {
		value, err := file.Read([]byte(fmt.Sprintf("writer2-%d", i)))
		require.NoError(t, err)
		require.Equal(t, []byte("I'm a different payload"), value)
	}
}

func BenchmarkReadWrite(b *testing.B) {
	file, err := newDataFile(1, b.TempDir(), kv_bitcask.NewRealClock())
	defer file.Close()
	require.NoError(b, err)

	b.Run("normal-io", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := file.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("I'm a record"))
			require.NoError(b, err)
		}
	})

	directFile, err := newDataFileDirectIO(2, b.TempDir(), kv_bitcask.NewRealClock())
	defer file.Close()
	require.NoError(b, err)

	b.Run("direct-io", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := directFile.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("I'm a record"))
			require.NoError(b, err)
		}
	})
}
