package messagepassed

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"kv-bitbask"
)

var _ kv_bitcask.Buffer = new(buffer)

type buffer struct {
	bytes         []byte
	close         func()
	capacity      int32
	currentOffset int32
	offset        int32
	closed        bool
}

func (b *buffer) GetOffset() int32 {
	return b.offset
}

func (b *buffer) Write(data []byte) error {
	if b.closed {
		return errors.New("buffer is closed")
	}
	newBytes := int32(len(data))
	offset := b.currentOffset
	if newBytes+offset > b.capacity {
		return errors.New("write will exceed allocated capacity")
	}
	copy(b.bytes[offset:], data)
	b.currentOffset = offset + newBytes
	return nil
}

// Close marks the allocated buffer as done and allows the current backing buffer to be free to be written to disk
func (b *buffer) Close() error {
	if b.closed {
		return nil
	}
	b.close()
	b.closed = true
	return nil
}

var _ kv_bitcask.Allocator = new(goroutineAllocator)

type allocationRequest struct {
	size     int
	response chan *buffer
}

type segment struct {
	data            []byte
	buffers         []*buffer
	flush           func()
	offset          int32
	capacity        int32
	openBufferCount int32
	flushedOffset   int32
	dirty           bool
	closed          bool
	closerChan      chan bool
}

func newSegment(flush func()) *segment {
	newSeg := &segment{
		flush:      flush,
		closerChan: make(chan bool),
	}

	newSeg.reset()

	go newSeg.closer()

	return newSeg
}

func (s *segment) closer() {
	for {
		select {
		case _, ok := <-s.closerChan:
			if !ok {
				return
			}
			s.openBufferCount = s.openBufferCount - 1
			if s.closed && s.openBufferCount == 0 {
				// async flush
			}
		}

	}
}

func (s *segment) CanWrite(offset int32) bool {
	return offset < s.capacity
}

func (s *segment) NewBuffer(size int32) *buffer {
	if s.closed {
		fmt.Printf("segment is closed for new buffers\n")
		return nil
	}

	offset := s.offset
	if !s.CanWrite(offset + size) {
		// fmt.Printf("marking segment closed\n")
		s.closed = true
		return nil
	}

	s.offset = offset + size

	s.dirty = true

	// fmt.Printf("allocating new buffer\n")
	s.openBufferCount++

	// fmt.Printf("buffer stats= offset: %d, newoffset: %d, bufferCapacity: %d, real capacity: %d\n", offset, offset+size, s.capacity, len(s.data))

	buf := &buffer{
		bytes:    s.data[offset : offset+size],
		capacity: size,
		close: func() {
			s.closerChan <- true
		},
		offset: offset,
	}

	// fmt.Printf("appending\n")
	s.buffers = append(s.buffers, buf)

	// fmt.Printf("done appending\n")
	return buf
}

func (s *segment) reset() {
	s.offset = 0
	s.buffers = []*buffer{}
	s.closed = false
	s.dirty = false
	s.flushedOffset = 0
}

func (s *segment) Close() error {
	close(s.closerChan)
	return nil
}

type goroutineAllocator struct {
	allocatorChan chan allocationRequest
	tail          int
	head          int
	segments      []*segment
	file          *os.File
	flushLock     *sync.Mutex
	flushChan     chan bool
	lastFlush     int64
	clock         kv_bitcask.Clock
	closed        atomic.Bool
}

func NewAllocator(file *os.File) kv_bitcask.Allocator {
	allocator := &goroutineAllocator{
		file: file,
		//
	}

	segments := make([]*segment, 5, 5)
	for i := 0; i < 5; i++ {

		segments = append(segments, newSegment(func() {
			allocator.flushChan <- true
		}))
	}

	allocator.segments = segments

	go allocator.allocateLoop()
	go allocator.flusher()
	return allocator
}

// flusher runs asynchronously to determine when the segments should be flushed to disk
func (g *goroutineAllocator) flusher() {
	for {

	}
}

func (g *goroutineAllocator) allocateLoop() {
	for request := range g.allocatorChan {
		if g.closed.Load() {
			fmt.Printf("closing allocator\n")
			return
		}
		for {
			if (g.head+1)%len(g.segments) == g.tail {
				// sync flush
			}
			if (g.tail+(len(g.segments)/2))%len(g.segments) >= g.head {
				// async flush
			}
			// fmt.Printf("allocating new buffer\n")
			buffer := g.segments[g.head].NewBuffer(int32(request.size))
			if buffer != nil {
				request.response <- buffer
				break
			}

			g.head = (g.head + 1) % len(g.segments)
		}
	}
}

func (g *goroutineAllocator) Close() error {
	g.closed.Store(true)

	close(g.allocatorChan)

	for _, seg := range g.segments {
		seg.Close()
	}

	// flush

	return g.file.Close()
}

func (g *goroutineAllocator) Allocate(size int) (kv_bitcask.Buffer, error) {
	// should probably add a context here to force cancelling at some point
	if g.closed.Load() {
		return nil, errors.New("allocator is closed")
	}

	responseChan := make(chan *buffer)

	newAllocation := allocationRequest{
		size:     size,
		response: responseChan,
	}

	g.allocatorChan <- newAllocation

	return <-responseChan, nil

}

func (g *goroutineAllocator) Flush() error {
	// TODO implement me
	panic("implement me")
}
