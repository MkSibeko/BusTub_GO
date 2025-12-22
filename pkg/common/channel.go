// Package common - Thread-safe channel implementation.
//
// Channel provides a thread-safe queue for communication between goroutines.
// This is used primarily by the DiskScheduler to queue disk I/O requests.
//
// ASSIGNMENT NOTES:
// In the C++ implementation, this is a custom thread-safe queue using mutexes
// and condition variables. In Go, we can use native channels, but this
// wrapper provides a consistent interface and allows for future extensions.
package common

import (
	"sync"
)

// Channel is a thread-safe channel for inter-goroutine communication.
// It wraps Go's native channel with additional functionality like size tracking.
//
// Type parameter T is the type of elements stored in the channel.
type Channel[T any] struct {
	// ch is the underlying Go channel.
	ch chan T

	// mu protects the closed flag.
	mu sync.Mutex

	// closed indicates if the channel has been closed.
	closed bool
}

// NewChannel creates a new channel with the specified buffer capacity.
// A capacity of 0 creates an unbuffered channel.
func NewChannel[T any](capacity int) *Channel[T] {
	return &Channel[T]{
		ch:     make(chan T, capacity),
		closed: false,
	}
}

// Put adds an element to the channel.
// This will block if the channel buffer is full.
// Returns false if the channel is closed.
func (c *Channel[T]) Put(value T) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.mu.Unlock()

	c.ch <- value
	return true
}

// Get retrieves an element from the channel.
// This will block if the channel is empty.
// Returns the value and true if successful, or zero value and false if channel is closed.
func (c *Channel[T]) Get() (T, bool) {
	value, ok := <-c.ch
	return value, ok
}

// TryGet attempts to retrieve an element without blocking.
// Returns the value, true if an element was available,
// or zero value, false if the channel is empty or closed.
func (c *Channel[T]) TryGet() (T, bool) {
	select {
	case value, ok := <-c.ch:
		return value, ok
	default:
		var zero T
		return zero, false
	}
}

// Close closes the channel.
// After closing, Put will return false and Get will eventually return false
// after all buffered elements are consumed.
func (c *Channel[T]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.closed {
		c.closed = true
		close(c.ch)
	}
}

// IsClosed returns true if the channel has been closed.
func (c *Channel[T]) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Len returns the number of elements currently in the channel buffer.
func (c *Channel[T]) Len() int {
	return len(c.ch)
}

// Cap returns the capacity of the channel buffer.
func (c *Channel[T]) Cap() int {
	return cap(c.ch)
}

// Native returns the underlying Go channel for use with select statements.
// Use with caution - direct manipulation bypasses the Channel wrapper's safety.
func (c *Channel[T]) Native() <-chan T {
	return c.ch
}
