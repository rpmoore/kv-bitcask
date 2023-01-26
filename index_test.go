package kv_bitcask

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadWriteIndex_SetGet(t *testing.T) {
	index := newReadWriteIndex()

	require.NoError(t, index.Set(1, &indexEntry{Offset: 1}))
	require.NoError(t, index.Set(2, &indexEntry{Offset: 3}))

	value, ok := index.Get(1)
	require.True(t, ok)
	require.Equal(t, int32(1), value.Offset)
	value, ok = index.Get(2)
	require.True(t, ok)
	require.Equal(t, int32(3), value.Offset)
}
