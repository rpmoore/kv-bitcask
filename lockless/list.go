package lockless

import (
	"sync/atomic"
	"unsafe"
)

type List[T any] struct {
	head *node[T]
	tail *node[T]
}

type node[T any] struct {
	value T
	next  *node[T]
}

func (l *List[T]) Append(value T) {
	newNode := &node[T]{value: value}

	for {
		// fmt.Printf("adding new tail\n")

		var currentTail *node[T] = l.tail
		var currentHead *node[T] = l.head

		// currentTail := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(l.tail))))
		// currentHead := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(l.head))))

		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail)), unsafe.Pointer(currentTail), unsafe.Pointer(newNode)) {
			// fmt.Printf("new tail added\n")
			if currentHead == nil {
				atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)), nil, unsafe.Pointer(newNode))
			}
			if currentTail != nil {
				currentTail.next = newNode
			}
			return
		}
	}
}

func (l *List[T]) DropUntil(method func(value T) bool) {
	currentNode := l.head
	for {
		if currentNode == nil {
			l.head = nil // not sure if this is thread safe
			l.tail = nil
			return
		}
		if !method(currentNode.value) {
			l.head = currentNode // not sure if this is thread safe
			return
		}

		currentNode = currentNode.next
	}
}

func (l *List[T]) Range(method func(value T) bool) {
	currentNode := l.head
	for {
		if currentNode == nil {
			return
		}
		if !method(currentNode.value) {
			return
		}

		currentNode = currentNode.next
	}
}
