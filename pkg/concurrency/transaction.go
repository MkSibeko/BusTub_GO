// Package concurrency provides transaction and concurrency control.
//
// BusTub uses Multi-Version Concurrency Control (MVCC) with:
// - Snapshot isolation (or serializable)
// - Timestamp ordering for versioning
// - Undo logs for version chains
//
// ASSIGNMENT NOTES (Assignment #4):
// ==================================
// You will implement:
// 1. Transaction management (begin, commit, abort)
// 2. MVCC tuple visibility
// 3. Write-write conflict detection
// 4. (Optional) Serializable validation
//
// Key concepts:
// - Each transaction has a read_ts (snapshot timestamp)
// - Each transaction has a commit_ts (when it commits)
// - Tuple versions have timestamps showing when created
// - Undo logs store previous versions
//
// This mirrors: bustub/src/include/concurrency/
package concurrency

import (
	"sync"
	"sync/atomic"

	"github.com/bustub-go/pkg/common"
)

// =============================================================================
// TRANSACTION
// =============================================================================

// TransactionState represents the state of a transaction.
type TransactionState int

const (
	TransactionStateRunning TransactionState = iota
	TransactionStateCommitted
	TransactionStateAborted
	TransactionStateTainted // Has a conflict, will abort
)

// IsolationLevel defines the isolation level of a transaction.
type IsolationLevel int

const (
	// ReadUncommitted can see uncommitted changes.
	ReadUncommitted IsolationLevel = iota

	// SnapshotIsolation sees a consistent snapshot.
	SnapshotIsolation

	// Serializable provides full serializability.
	Serializable
)

// Transaction represents a database transaction.
//
// IMPORTANT FIELDS:
// - txn_id_: Unique transaction identifier
// - read_ts_: The timestamp of the snapshot this transaction reads
// - commit_ts_: Set when the transaction commits
// - state_: Current state (running, committed, aborted)
// - write_set_: Tracks what this transaction has modified
//
// MVCC BEHAVIOR:
//   - A transaction sees tuples where:
//     tuple.ts <= txn.read_ts (tuple was committed before we started)
//   - When committing, txn.commit_ts becomes the tuple's new ts
type Transaction struct {
	// txnID uniquely identifies this transaction.
	txnID common.TxnID

	// readTS is the snapshot timestamp.
	// The transaction sees data as of this point in time.
	readTS common.TxnTimestamp

	// commitTS is set when the transaction commits.
	commitTS common.TxnTimestamp

	// state is the current transaction state.
	state TransactionState

	// isolationLevel determines visibility rules.
	isolationLevel IsolationLevel

	// writeSet tracks modified (table_oid, rid) pairs.
	// Used for conflict detection and undo on abort.
	writeSet []WriteRecord

	// undoLogs are undo records created by this transaction.
	// Used to revert changes on abort.
	// undoLogs []UndoLog

	// readSet tracks what this transaction has read.
	// Used for serializable validation.
	readSet []ReadRecord

	mu sync.Mutex
}

// WriteRecord tracks a write operation.
type WriteRecord struct {
	TableOID uint32
	RID      common.RID
	// Type: INSERT, DELETE, UPDATE
	IsInsert bool
	IsDelete bool
}

// ReadRecord tracks a read operation (for serializable validation).
type ReadRecord struct {
	TableOID uint32
	RID      common.RID
}

// NewTransaction creates a new transaction.
func NewTransaction(txnID common.TxnID, isolationLevel IsolationLevel) *Transaction {
	return &Transaction{
		txnID:          txnID,
		state:          TransactionStateRunning,
		isolationLevel: isolationLevel,
		writeSet:       make([]WriteRecord, 0),
		readSet:        make([]ReadRecord, 0),
	}
}

// GetTxnID returns the transaction ID.
func (t *Transaction) GetTxnID() common.TxnID {
	return t.txnID
}

// GetReadTS returns the read timestamp.
func (t *Transaction) GetReadTS() common.TxnTimestamp {
	return t.readTS
}

// GetCommitTS returns the commit timestamp.
func (t *Transaction) GetCommitTS() common.TxnTimestamp {
	return t.commitTS
}

// GetState returns the transaction state.
func (t *Transaction) GetState() TransactionState {
	return t.state
}

// SetState sets the transaction state.
func (t *Transaction) SetState(state TransactionState) {
	t.state = state
}

// IsRunning returns true if the transaction is still running.
func (t *Transaction) IsRunning() bool {
	return t.state == TransactionStateRunning
}

// SetTainted marks the transaction as tainted (will abort).
func (t *Transaction) SetTainted() {
	t.state = TransactionStateTainted
}

// AppendWriteRecord adds a write to the write set.
func (t *Transaction) AppendWriteRecord(tableOID uint32, rid common.RID, isInsert, isDelete bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writeSet = append(t.writeSet, WriteRecord{
		TableOID: tableOID,
		RID:      rid,
		IsInsert: isInsert,
		IsDelete: isDelete,
	})
}

// AppendReadRecord adds a read to the read set.
func (t *Transaction) AppendReadRecord(tableOID uint32, rid common.RID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.readSet = append(t.readSet, ReadRecord{
		TableOID: tableOID,
		RID:      rid,
	})
}

// GetWriteSet returns the write set.
func (t *Transaction) GetWriteSet() []WriteRecord {
	return t.writeSet
}

// =============================================================================
// TRANSACTION MANAGER
// =============================================================================

// TransactionManager manages the lifecycle of transactions.
//
// RESPONSIBILITIES:
// - Begin new transactions
// - Assign timestamps (read_ts and commit_ts)
// - Commit transactions
// - Abort transactions
// - Track active transactions for garbage collection
//
// TIMESTAMP MANAGEMENT:
// - Uses a monotonically increasing counter
// - read_ts = current commit_ts when transaction begins
// - commit_ts = counter value when committing
//
// COMMIT PROTOCOL:
// 1. Validate (for serializable, check conflicts)
// 2. Assign commit_ts
// 3. Update all modified tuples' timestamps
// 4. Mark transaction as committed
//
// ABORT PROTOCOL:
// 1. Iterate through write set
// 2. Undo each modification using undo logs
// 3. Mark transaction as aborted
type TransactionManager struct {
	// nextTxnID is the next transaction ID to assign.
	nextTxnID atomic.Uint64

	// lastCommitTS is the last committed timestamp.
	// Used to assign read_ts to new transactions.
	lastCommitTS atomic.Uint64

	// activeTxns maps txn_id to Transaction for active transactions.
	activeTxns sync.Map // map[common.TxnID]*Transaction

	// Watermark tracks the oldest active transaction.
	// Used for garbage collection.
	watermark *Watermark
}

// NewTransactionManager creates a new transaction manager.
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		watermark: NewWatermark(),
	}
}

// Begin starts a new transaction.
//
// TODO: Implement this
//
// STEPS:
// 1. Assign a unique txn_id
// 2. Set read_ts to the current lastCommitTS
// 3. Create Transaction object
// 4. Add to activeTxns map
// 5. Update watermark
// 6. Return the transaction
func (tm *TransactionManager) Begin(isolationLevel IsolationLevel) *Transaction {
	panic("Begin not implemented")
}

// Commit commits a transaction.
//
// TODO: Implement this
//
// STEPS (for MVCC):
// 1. Check if transaction is tainted -> abort instead
// 2. For Serializable: validate (check for conflicts)
//   - If validation fails, abort
//
// 3. Assign commit_ts (increment counter)
// 4. Update all modified tuples:
//   - Set tuple's timestamp to commit_ts
//   - This makes changes visible to new transactions
//
// 5. Set transaction state to Committed
// 6. Remove from activeTxns
// 7. Update watermark
//
// VALIDATION (Serializable):
// Check that no tuple in read_set was modified by another
// transaction that committed after our read_ts.
func (tm *TransactionManager) Commit(txn *Transaction) error {
	panic("Commit not implemented")
}

// Abort aborts a transaction.
//
// TODO: Implement this
//
// STEPS:
// 1. Iterate through write set in reverse order
// 2. For each write:
//   - If INSERT: delete the tuple
//   - If DELETE: restore the tuple (undelete)
//   - If UPDATE: restore from undo log
//
// 3. Set transaction state to Aborted
// 4. Remove from activeTxns
// 5. Update watermark
func (tm *TransactionManager) Abort(txn *Transaction) {
	panic("Abort not implemented")
}

// GetWatermark returns the watermark (oldest active read_ts).
func (tm *TransactionManager) GetWatermark() common.TxnTimestamp {
	return tm.watermark.GetWatermark()
}

// =============================================================================
// WATERMARK
// =============================================================================

// Watermark tracks the oldest active read timestamp.
//
// This is used for garbage collection:
// - Tuple versions older than the watermark can be removed
// - No active transaction will ever need to read them
//
// IMPLEMENTATION:
// Use a min-heap or sorted map of active read_ts values.
// When a transaction commits/aborts, remove its read_ts.
// The minimum is the watermark.
type Watermark struct {
	// currentTS tracks active transactions' read timestamps.
	// Map: read_ts -> count of transactions with this read_ts
	currentTS map[common.TxnTimestamp]int

	// watermark is the current minimum active read_ts.
	watermark common.TxnTimestamp

	mu sync.RWMutex
}

// NewWatermark creates a new watermark tracker.
func NewWatermark() *Watermark {
	return &Watermark{
		currentTS: make(map[common.TxnTimestamp]int),
	}
}

// AddTxn adds a transaction's read_ts to tracking.
func (w *Watermark) AddTxn(readTS common.TxnTimestamp) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.currentTS[readTS]++
	w.updateWatermark()
}

// RemoveTxn removes a transaction's read_ts from tracking.
func (w *Watermark) RemoveTxn(readTS common.TxnTimestamp) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.currentTS[readTS]--
	if w.currentTS[readTS] <= 0 {
		delete(w.currentTS, readTS)
	}
	w.updateWatermark()
}

// GetWatermark returns the current watermark.
func (w *Watermark) GetWatermark() common.TxnTimestamp {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.watermark
}

// updateWatermark recalculates the watermark (minimum read_ts).
func (w *Watermark) updateWatermark() {
	if len(w.currentTS) == 0 {
		w.watermark = 0
		return
	}

	var min common.TxnTimestamp = ^common.TxnTimestamp(0) // Max uint64
	for ts := range w.currentTS {
		if ts < min {
			min = ts
		}
	}
	w.watermark = min
}

// =============================================================================
// UNDO LOG
// =============================================================================

// UndoLink points to an undo log entry.
type UndoLink struct {
	// PrevTxnID is the transaction that created the previous version.
	PrevTxnID common.TxnID

	// PrevLogIndex is the index in that transaction's undo log.
	PrevLogIndex int
}

// InvalidUndoLink represents no previous version.
var InvalidUndoLink = UndoLink{PrevTxnID: common.InvalidTxnID}

// UndoLog stores a previous version of a tuple.
//
// IMPLEMENTATION NOTES:
// =====================
// When a tuple is modified, we store the old version in an undo log.
// This allows:
// - Reading old versions for MVCC
// - Rolling back on abort
//
// The undo log forms a version chain:
// tuple -> undo_log_1 -> undo_log_2 -> ...
//
// Each entry has:
// - is_deleted: Whether this version was deleted
// - modified_fields: Bitmask of which fields changed
// - partial_tuple: Values for modified fields
// - timestamp: When this version was valid
// - prev_version: Link to even older version
type UndoLog struct {
	// IsDeleted indicates if this version was a delete.
	IsDeleted bool

	// ModifiedFields is a bitmask of which columns were modified.
	ModifiedFields []bool

	// PartialTuple contains values for modified columns.
	// Only includes columns where ModifiedFields[i] is true.
	PartialTuple []byte

	// Timestamp is when this version was created.
	Timestamp common.TxnTimestamp

	// PrevVersion points to the previous version.
	PrevVersion UndoLink
}

// =============================================================================
// VISIBILITY CHECKING
// =============================================================================

// TupleVisibility determines if a tuple is visible to a transaction.
//
// VISIBILITY RULES (Snapshot Isolation):
// A tuple is visible if:
// 1. It was committed (has a commit timestamp, not a txn_id)
// 2. Its timestamp <= our read_ts
// 3. It's not deleted (or was deleted after our read_ts)
//
// For uncommitted tuples (timestamp is a txn_id):
// - Only visible to the transaction that created it
// - Use IsTxnID() to check if timestamp is actually a txn_id
//
// TODO: Implement ReconstructTuple to rebuild a tuple from undo logs
// when the current version is not visible.

// IsTupleVisible checks if a tuple is visible to a transaction.
//
// Parameters:
// - tupleTS: The tuple's timestamp (or creating txn_id)
// - isDeleted: Whether the tuple is deleted
// - readTS: The transaction's read timestamp
// - txnID: The transaction's ID
//
// Returns true if the tuple is visible.
func IsTupleVisible(tupleTS common.TxnTimestamp, isDeleted bool, readTS common.TxnTimestamp, txnID common.TxnID) bool {
	// TODO: Implement this
	//
	// CASES:
	// 1. If tupleTS is a txn_id (not yet committed):
	//    - Visible only if txn_id == current transaction
	// 2. If tupleTS is a committed timestamp:
	//    - Visible if tupleTS <= readTS
	// 3. If tuple is deleted and we can see it, it's not visible
	//
	// NOTE: Use IsTxnID helper to distinguish timestamps from txn_ids

	panic("IsTupleVisible not implemented")
}

// ReconstructTuple rebuilds a tuple as of a certain timestamp.
//
// When the current version isn't visible, we follow the version chain
// (undo logs) to find the version that was valid at read_ts.
//
// TODO: Implement this
//
// STEPS:
//  1. Start with current tuple
//  2. If visible, return it
//  3. Follow undo log chain (tuple.prev_version)
//  4. For each undo log:
//     a. Check if this version is visible
//     b. If yes, reconstruct tuple from partial data
//     c. If no, continue to next version
//  5. If no visible version, tuple doesn't exist for this transaction
func ReconstructTuple(
// schema *catalog.Schema,
// tuple *table.Tuple,
// tupleMeta *table.TupleMeta,
// undoLogs map[common.TxnID][]UndoLog,
// readTS common.TxnTimestamp,
) {
	panic("ReconstructTuple not implemented")
}
