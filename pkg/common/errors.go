// Package common - Error types for BusTub-Go.
//
// This file defines custom error types used throughout the database system.
// Each error type provides context about what went wrong and where.
//
// ASSIGNMENT NOTES:
// Unlike C++ which uses exceptions, Go uses explicit error returns.
// These error types help callers understand and handle different error conditions.
package common

import (
	"errors"
	"fmt"
)

// =============================================================================
// STANDARD ERRORS (Sentinel Errors)
// =============================================================================
// These can be checked with errors.Is()

var (
	// ErrPageNotFound indicates the requested page does not exist.
	ErrPageNotFound = errors.New("page not found")

	// ErrBufferPoolFull indicates no frames are available for a new page.
	ErrBufferPoolFull = errors.New("buffer pool is full, no evictable frames")

	// ErrPagePinned indicates a page cannot be evicted because it's still pinned.
	ErrPagePinned = errors.New("page is pinned and cannot be evicted")

	// ErrInvalidPageID indicates an invalid page ID was provided.
	ErrInvalidPageID = errors.New("invalid page ID")

	// ErrInvalidFrameID indicates an invalid frame ID was provided.
	ErrInvalidFrameID = errors.New("invalid frame ID")

	// ErrDuplicateKey indicates a duplicate key insertion in a unique index.
	ErrDuplicateKey = errors.New("duplicate key")

	// ErrKeyNotFound indicates a key was not found in an index.
	ErrKeyNotFound = errors.New("key not found")

	// ErrOutOfBounds indicates an access beyond valid bounds.
	ErrOutOfBounds = errors.New("index out of bounds")

	// ErrTableNotFound indicates the requested table does not exist.
	ErrTableNotFound = errors.New("table not found")

	// ErrIndexNotFound indicates the requested index does not exist.
	ErrIndexNotFound = errors.New("index not found")

	// ErrTransactionAborted indicates the transaction has been aborted.
	ErrTransactionAborted = errors.New("transaction aborted")

	// ErrDeadlock indicates a deadlock was detected.
	ErrDeadlock = errors.New("deadlock detected")

	// ErrLockConflict indicates a lock could not be acquired.
	ErrLockConflict = errors.New("lock conflict")

	// ErrIOError indicates a disk I/O error occurred.
	ErrIOError = errors.New("I/O error")

	// ErrCorruptedPage indicates page data is corrupted.
	ErrCorruptedPage = errors.New("corrupted page data")

	// ErrNotImplemented indicates a feature is not yet implemented.
	ErrNotImplemented = errors.New("not implemented")
)

// =============================================================================
// WRAPPED ERRORS (Contextual Errors)
// =============================================================================
// These wrap standard errors with additional context.

// PageError wraps an error with page-specific context.
type PageError struct {
	PageID PageID
	Op     string // Operation that failed (read, write, delete, etc.)
	Err    error
}

func (e *PageError) Error() string {
	return fmt.Sprintf("page %d: %s: %v", e.PageID, e.Op, e.Err)
}

func (e *PageError) Unwrap() error {
	return e.Err
}

// NewPageError creates a new PageError.
func NewPageError(pageID PageID, op string, err error) *PageError {
	return &PageError{
		PageID: pageID,
		Op:     op,
		Err:    err,
	}
}

// ExecutionError wraps an error from query execution.
type ExecutionError struct {
	Executor string // Name of the executor
	Message  string
	Err      error
}

func (e *ExecutionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("execution error in %s: %s: %v", e.Executor, e.Message, e.Err)
	}
	return fmt.Sprintf("execution error in %s: %s", e.Executor, e.Message)
}

func (e *ExecutionError) Unwrap() error {
	return e.Err
}

// NewExecutionError creates a new ExecutionError.
func NewExecutionError(executor, message string, err error) *ExecutionError {
	return &ExecutionError{
		Executor: executor,
		Message:  message,
		Err:      err,
	}
}

// TransactionError wraps an error with transaction context.
type TransactionError struct {
	TxnID   TxnID
	Message string
	Err     error
}

func (e *TransactionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("transaction %d: %s: %v", e.TxnID, e.Message, e.Err)
	}
	return fmt.Sprintf("transaction %d: %s", e.TxnID, e.Message)
}

func (e *TransactionError) Unwrap() error {
	return e.Err
}

// NewTransactionError creates a new TransactionError.
func NewTransactionError(txnID TxnID, message string, err error) *TransactionError {
	return &TransactionError{
		TxnID:   txnID,
		Message: message,
		Err:     err,
	}
}

// =============================================================================
// ASSERTION HELPERS
// =============================================================================

// BustubAssert panics if the condition is false.
// Used for internal invariant checking during development.
func BustubAssert(condition bool, message string) {
	if !condition {
		panic(fmt.Sprintf("BUSTUB ASSERTION FAILED: %s", message))
	}
}

// BustubEnsure panics if the condition is false, with formatted message.
func BustubEnsure(condition bool, format string, args ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("BUSTUB ENSURE FAILED: "+format, args...))
	}
}

// Unreachable panics indicating code that should never be reached.
func Unreachable(message string) {
	panic(fmt.Sprintf("UNREACHABLE CODE: %s", message))
}
