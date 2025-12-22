// Package concurrency provides lock-based concurrency control.
//
// This file contains the LockManager for 2PL-based concurrency.
// BusTub primarily uses MVCC, but lock manager can be used for
// write-write conflict prevention.
//
// This mirrors: bustub/src/include/concurrency/lock_manager.h
package concurrency

import (
	"sync"

	"github.com/bustub-go/pkg/common"
)

// =============================================================================
// LOCK TYPES
// =============================================================================

// LockMode represents the type of lock.
type LockMode int

const (
	// LockModeShared (S) allows concurrent reads.
	LockModeShared LockMode = iota

	// LockModeExclusive (X) allows only one writer.
	LockModeExclusive

	// LockModeIntentionShared (IS) indicates intent to acquire S lock on children.
	LockModeIntentionShared

	// LockModeIntentionExclusive (IX) indicates intent to acquire X lock on children.
	LockModeIntentionExclusive

	// LockModeSharedIntentionExclusive (SIX) = S + IX
	LockModeSharedIntentionExclusive
)

// LockRequest represents a request for a lock.
type LockRequest struct {
	// TxnID is the requesting transaction.
	TxnID common.TxnID

	// Mode is the requested lock mode.
	Mode LockMode

	// Granted indicates if the lock has been granted.
	Granted bool
}

// LockRequestQueue is a queue of lock requests for a resource.
type LockRequestQueue struct {
	// requests is the queue of pending and granted requests.
	requests []*LockRequest

	// cv is used to wait for lock availability.
	cv *sync.Cond

	// upgrading is the txn_id of a transaction waiting for upgrade.
	// Only one transaction can upgrade at a time.
	upgrading common.TxnID

	mu sync.Mutex
}

// NewLockRequestQueue creates a new lock request queue.
func NewLockRequestQueue() *LockRequestQueue {
	q := &LockRequestQueue{
		requests:  make([]*LockRequest, 0),
		upgrading: common.InvalidTxnID,
	}
	q.cv = sync.NewCond(&q.mu)
	return q
}

// =============================================================================
// LOCK MANAGER
// =============================================================================

// LockManager implements lock-based concurrency control.
//
// HIERARCHICAL LOCKING:
// - Table-level locks (IS, IX, S, X, SIX)
// - Row-level locks (S, X)
//
// LOCK COMPATIBILITY:
//
//	     | IS | IX | S  | SIX | X
//	-----+----+----+----+-----+---
//	IS   | ✓  | ✓  | ✓  |  ✓  | ✗
//	IX   | ✓  | ✓  | ✗  |  ✗  | ✗
//	S    | ✓  | ✗  | ✓  |  ✗  | ✗
//	SIX  | ✓  | ✗  | ✗  |  ✗  | ✗
//	X    | ✗  | ✗  | ✗  |  ✗  | ✗
//
// DEADLOCK PREVENTION:
// BusTub uses wound-wait or wait-die to prevent deadlocks.
// (Implementation depends on assignment requirements)
type LockManager struct {
	// tableLockMap maps table_oid -> lock request queue
	tableLockMap sync.Map // map[uint32]*LockRequestQueue

	// rowLockMap maps (table_oid, rid) -> lock request queue
	rowLockMap sync.Map // map[string]*LockRequestQueue

	// For enabling/disabling the lock manager
	enabled bool
}

// NewLockManager creates a new lock manager.
func NewLockManager() *LockManager {
	return &LockManager{
		enabled: true,
	}
}

// =============================================================================
// TABLE LOCKS
// =============================================================================

// LockTable acquires a table-level lock.
//
// TODO: Implement this
//
// STEPS:
// 1. Get or create the lock request queue for this table
// 2. Check if transaction already holds a lock:
//   - If same mode, return (already have it)
//   - If upgrading, check if upgrade is valid
//
// 3. Create a lock request
// 4. Add to queue
// 5. Wait until lock can be granted:
//   - Check compatibility with existing locks
//   - Use condition variable to wait
//
// 6. Mark request as granted
// 7. Return
//
// UPGRADE RULES:
// - IS can upgrade to S, X, IX, SIX
// - IX can upgrade to X, SIX
// - S can upgrade to X, SIX
// - SIX can upgrade to X only
func (lm *LockManager) LockTable(txn *Transaction, mode LockMode, tableOID uint32) error {
	panic("LockTable not implemented")
}

// UnlockTable releases a table-level lock.
//
// TODO: Implement this
//
// STEPS:
// 1. Get the lock request queue
// 2. Find the transaction's lock request
// 3. Remove it from the queue
// 4. Signal waiting transactions (broadcast on CV)
//
// 2PL ENFORCEMENT:
// If using strict 2PL, only allow unlock at commit/abort.
// If using basic 2PL, track whether we're in shrinking phase.
func (lm *LockManager) UnlockTable(txn *Transaction, tableOID uint32) error {
	panic("UnlockTable not implemented")
}

// =============================================================================
// ROW LOCKS
// =============================================================================

// LockRow acquires a row-level lock.
//
// TODO: Implement this
//
// PRECONDITION:
// Must hold appropriate table-level lock:
// - For S lock: Hold IS, S, IX, or SIX on table
// - For X lock: Hold IX, SIX, or X on table
//
// Similar algorithm to LockTable but for row granularity.
func (lm *LockManager) LockRow(txn *Transaction, mode LockMode, tableOID uint32, rid common.RID) error {
	panic("LockRow not implemented")
}

// UnlockRow releases a row-level lock.
//
// TODO: Implement this
func (lm *LockManager) UnlockRow(txn *Transaction, tableOID uint32, rid common.RID) error {
	panic("UnlockRow not implemented")
}

// =============================================================================
// COMPATIBILITY CHECKING
// =============================================================================

// AreLocksCompatible checks if two lock modes are compatible.
//
// Use the compatibility matrix defined above.
func AreLocksCompatible(mode1, mode2 LockMode) bool {
	// Compatibility matrix
	// True means locks are compatible (can coexist)
	compatibilityMatrix := [][]bool{
		// IS     IX     S      SIX    X
		{true, true, true, true, false},     // IS
		{true, true, false, false, false},   // IX
		{true, false, true, false, false},   // S
		{true, false, false, false, false},  // SIX
		{false, false, false, false, false}, // X
	}

	return compatibilityMatrix[mode1][mode2]
}

// CanUpgrade checks if upgrading from one mode to another is valid.
func CanUpgrade(currentMode, requestedMode LockMode) bool {
	// Upgrade paths:
	// IS -> S, X, IX, SIX
	// S -> X, SIX
	// IX -> X, SIX
	// SIX -> X

	switch currentMode {
	case LockModeIntentionShared:
		return requestedMode != LockModeIntentionShared
	case LockModeShared:
		return requestedMode == LockModeExclusive || requestedMode == LockModeSharedIntentionExclusive
	case LockModeIntentionExclusive:
		return requestedMode == LockModeExclusive || requestedMode == LockModeSharedIntentionExclusive
	case LockModeSharedIntentionExclusive:
		return requestedMode == LockModeExclusive
	case LockModeExclusive:
		return false // Already have strongest lock
	}
	return false
}

// =============================================================================
// DEADLOCK DETECTION/PREVENTION
// =============================================================================

// DeadlockDetector detects deadlock cycles in the wait-for graph.
//
// WOUND-WAIT Algorithm:
// - If older transaction waits for younger: wait
// - If younger transaction waits for older: abort younger (wound)
//
// WAIT-DIE Algorithm:
// - If older transaction waits for younger: abort older (die)
// - If younger transaction waits for older: wait
type DeadlockDetector struct {
	// waitForGraph: txn_id -> set of txn_ids it's waiting for
	waitForGraph map[common.TxnID]map[common.TxnID]bool
	mu           sync.RWMutex
}

// NewDeadlockDetector creates a new deadlock detector.
func NewDeadlockDetector() *DeadlockDetector {
	return &DeadlockDetector{
		waitForGraph: make(map[common.TxnID]map[common.TxnID]bool),
	}
}

// AddEdge adds a wait-for edge: txn1 waits for txn2.
func (d *DeadlockDetector) AddEdge(txn1, txn2 common.TxnID) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.waitForGraph[txn1] == nil {
		d.waitForGraph[txn1] = make(map[common.TxnID]bool)
	}
	d.waitForGraph[txn1][txn2] = true
}

// RemoveEdge removes a wait-for edge.
func (d *DeadlockDetector) RemoveEdge(txn1, txn2 common.TxnID) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.waitForGraph[txn1] != nil {
		delete(d.waitForGraph[txn1], txn2)
	}
}

// HasCycle detects if there's a cycle in the wait-for graph.
//
// TODO: Implement this using DFS cycle detection.
func (d *DeadlockDetector) HasCycle() (bool, common.TxnID) {
	// Returns (has_cycle, victim_txn_id)
	// Victim should be chosen to minimize work lost (youngest txn)

	panic("HasCycle not implemented")
}

// rowLockKey creates a unique key for a (table, rid) pair.
func rowLockKey(tableOID uint32, rid common.RID) string {
	return string(rune(tableOID)) + "-" + string(rune(rid.GetPageID())) + "-" + string(rune(rid.GetSlotNum()))
}
