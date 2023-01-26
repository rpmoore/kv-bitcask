package kv_bitcask

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type buffer struct {
	bytes         []byte
	close         func()
	capacity      int32
	currentOffset int32
	offset        int32
	closed        atomic.Bool
}

func (b *buffer) GetOffset() int32 {
	return b.offset
}

func (b *buffer) Write(data []byte) error {
	if b.closed.Load() {
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
func (b *buffer) Close() {
	if b.closed.Load() {
		return
	}
	b.close()
	b.closed.Store(true)
}

type segment struct {
	data            []byte
	offset          atomic.Int32
	capacity        int32
	flush           func()
	buffers         List[*buffer]
	closed          atomic.Bool
	openBufferCount atomic.Int32
	dirty           atomic.Bool
	flushedOffset   int32
}

func (s *segment) CanWrite(offset int32) bool {
	return offset < s.capacity
}

func (s *segment) NewBuffer(size int32) *buffer {
	if s.closed.Load() {
		fmt.Printf("segment is closed for new buffers\n")
		return nil
	}
	var offset int32
	for {
		offset = s.offset.Load()
		if !s.CanWrite(offset + size) {
			// fmt.Printf("marking segment closed\n")
			s.closed.CompareAndSwap(false, true)
			return nil
		}

		if s.offset.CompareAndSwap(offset, offset+size) {
			break
		}
	}

	s.dirty.Store(true)

	// fmt.Printf("allocating new buffer\n")
	s.openBufferCount.Add(1)

	// fmt.Printf("buffer stats= offset: %d, newoffset: %d, bufferCapacity: %d, real capacity: %d\n", offset, offset+size, s.capacity, len(s.data))

	buf := &buffer{
		bytes:    s.data[offset : offset+size],
		capacity: size,
		close: func() {
			// fmt.Printf("close called\n")
			s.openBufferCount.Add(-1)
			if s.closed.Load() && s.openBufferCount.Load() == 0 {
				fmt.Printf("flushing buffer\n")
				s.flush()
			}
		},
		offset: offset,
	}

	// fmt.Printf("appending\n")
	s.buffers.Append(buf)

	// fmt.Printf("done appending\n")
	return buf
}

func (s *segment) reset() {
	s.offset.Store(0)
	s.buffers = List[*buffer]{}
	s.closed.Store(false)
	s.dirty.Store(false)
	s.flushedOffset = 0
}

func (s *segment) String() string {
	return fmt.Sprintf("offset: %d, closed: %t, dirty: %t, flushedOffset: %d, openBufferCount: %d\n",
		s.offset.Load(), s.closed.Load(), s.dirty.Load(), s.flushedOffset, s.openBufferCount.Load())
}

// allocator is a ring buffer of segments where the first segment is the current segment being written to and when fill will be flushed to disk.
// While a segment is being flushed (asynchronously) the next segment in the ring buffer will be used. This way all writes coming into the db
// never block do to writing a segment.
type allocator struct {
	segments  []*segment
	head      atomic.Int32
	tail      atomic.Int32
	file      *os.File
	flushLock *sync.Mutex
	flushChan chan bool
	lastFlush atomic.Int64
	clock     Clock
	closed    atomic.Bool
}

func newAllocator(file *os.File, numSegments int, segmentSize int, clock Clock) *allocator {
	lock := &sync.Mutex{}
	segments := make([]*segment, numSegments, numSegments)
	flushChan := make(chan bool, 100)
	for i := 0; i < numSegments; i++ {
		segments[i] = &segment{
			data: make([]byte, segmentSize, segmentSize),
			flush: func() {
				flushChan <- false
			},
			capacity:        int32(segmentSize),
			openBufferCount: atomic.Int32{},
		}
	}

	fmt.Printf("starting allocator with %d segments of size %d\n", numSegments, segmentSize)

	alloc := &allocator{
		segments:  segments,
		file:      file,
		flushLock: lock,
		flushChan: flushChan,
		closed:    atomic.Bool{},
		clock:     clock,
	}
	go func() {
		alloc.flusher()
	}()
	return alloc
}

func (a *allocator) updateLastFlush() int64 {
	a.lastFlush.Store(a.clock.Now().Unix())
	return a.lastFlush.Load()
}

func (a *allocator) flusher() {
	fmt.Printf("starting async flusher\n")
	activeFlush := atomic.Bool{}

	flush := func() {
		defer activeFlush.Store(false)
		// fmt.Printf("running async flush\n")
		err := a.Flush()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to flush: %v", err)
		}
	}

	for {
		lastFlush := a.updateLastFlush()
		if a.closed.Load() {
			fmt.Printf("stopping flusher\n")
			return
		}
		timer := time.After(time.Minute * 1)
		select {
		case flushNow, ok := <-a.flushChan:
			if !ok {
				return
			}
			if activeFlush.Load() {
				continue
			}
			if flushNow || a.clock.Now().Add(time.Second*30).Unix() < lastFlush {
				activeFlush.Store(true)
				go flush()
			}
		case <-timer:
			activeFlush.Store(true)
			if activeFlush.Load() {
				continue
			}
			go flush()
		}

	}
}

func (a *allocator) Allocate(size int) (*buffer, error) {
	for {
		head := a.head.Load()
		tail := a.tail.Load()
		if (head+1)%int32(len(a.segments)) == tail { // this means the head has ran into the tail
			fmt.Printf("running a synchronous flush\n")
			// force a synchronous flush, want all threads to block here while flushing
			err := a.Flush()
			if err != nil {
				return nil, err
			}
		}
		if (int(tail)+(len(a.segments)/2))%len(a.segments) >= int(head) {
			a.flushChan <- true
		}
		// fmt.Printf("allocating new buffer\n")
		buffer := a.segments[head].NewBuffer(int32(size))
		if buffer != nil {
			return buffer, nil
		}
		// fmt.Printf("incrementing head\n")
		a.head.CompareAndSwap(head, (head+1)%int32(len(a.segments)))
	}
}

func (a *allocator) Flush() error {
	// fmt.Printf("flush called\n")
	a.flushLock.Lock()
	defer a.flushLock.Unlock()
	// fmt.Printf("starting flush\n")
	for {
		tail := a.tail.Load()
		segment := a.segments[tail]

		// add check to see if segment is dirty, if it is not, then return since that means there is nothing to flush

		if !segment.dirty.Load() {
			// fmt.Printf("segment not dirty, not flushing\n")
			return nil
		}

		// need to make sure partially filled segments is handled correctly when flushing

		largestOffset := segment.flushedOffset

		segment.buffers.DropUntil(func(buffer *buffer) bool {
			if !buffer.closed.Load() {
				return false
			}

			largestOffset = largestOffset + buffer.capacity
			return true
		})

		if largestOffset == 0 {
			// nothing to flush
			break
		}

		// fmt.Printf("writing up to offset: %d\n", largestOffset)
		_, err := a.file.Write(segment.data[segment.flushedOffset:largestOffset])
		if err != nil {
			return err
		}

		// all segments should be closed that are behind the head if we encounter 1 that is not closed
		// then that's the head and we should stop flushing
		if segment.closed.Load() {
			segment.reset()
			a.tail.Store((tail + 1) % int32(len(a.segments)))
		} else {
			segment.flushedOffset = largestOffset
			break
		}
	}

	a.updateLastFlush()

	return a.file.Sync()
}

func (a *allocator) Close() error {
	fmt.Printf("allocator was closed\n")
	if a.closed.Load() {
		return nil
	}
	a.closed.Store(true)
	close(a.flushChan)
	return a.Flush()
}
