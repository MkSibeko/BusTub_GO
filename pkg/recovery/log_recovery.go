// Package recovery provides crash recovery functionality.
//
// BusTub uses Write-Ahead Logging (WAL) for durability:
// - All changes are logged BEFORE they're applied
// - On crash, logs are replayed to recover state
//
// ASSIGNMENT NOTES (Bonus/Optional):
// ===================================
// Recovery is typically covered in advanced assignments.
// Key concepts:
// - ARIES recovery algorithm
// - Checkpointing
// - Log record types
//
// This mirrors: bustub/src/include/recovery/
package recovery

import (
	"encoding/binary"
	"sync"

	"github.com/bustub-go/pkg/common"
	"github.com/bustub-go/pkg/storage/disk"
)

// =============================================================================
// LOG RECORD TYPES
// =============================================================================

// LogRecordType identifies the type of log record.
type LogRecordType int

const (
	LogRecordInvalid LogRecordType = iota

	// Transaction lifecycle
	LogRecordBegin
	LogRecordCommit
	LogRecordAbort

	// Data modifications
	LogRecordInsert
	LogRecordMarkDelete
	LogRecordApplyDelete
	LogRecordRollbackDelete
	LogRecordUpdate

	// For index operations
	LogRecordNewPage

	// Checkpointing
	LogRecordCheckpointBegin
	LogRecordCheckpointEnd

	// CLR (Compensation Log Record) for undo
	LogRecordCLR
)

// LSN (Log Sequence Number) uniquely identifies a log record.
type LSN = int64

// InvalidLSN indicates no LSN.
const InvalidLSN LSN = -1

// =============================================================================
// LOG RECORD
// =============================================================================

// LogRecord represents a single log entry.
//
// Log records are variable-length. The format is:
// - Header (fixed size): size, LSN, txn_id, prev_lsn, type
// - Payload (variable): depends on type
//
// LOG RECORD FORMATS:
// -------------------
// BEGIN: header only
// COMMIT: header only
// ABORT: header only
//
// INSERT:
//
//	header + table_oid + rid + tuple_size + tuple_data
//
// DELETE:
//
//	header + table_oid + rid + tuple_size + tuple_data
//
// UPDATE:
//
//	header + table_oid + rid
//	+ old_tuple_size + old_tuple_data
//	+ new_tuple_size + new_tuple_data
type LogRecord struct {
	// Size is the total size of this log record in bytes.
	Size int32

	// LSN is the log sequence number.
	LSN LSN

	// TxnID is the transaction that created this record.
	TxnID common.TxnID

	// PrevLSN is the previous LSN for this transaction.
	// Forms a chain for undo operations.
	PrevLSN LSN

	// Type identifies what kind of record this is.
	Type LogRecordType

	// === PAYLOAD FIELDS (depend on Type) ===

	// For INSERT, DELETE, UPDATE
	TableOID uint32
	RID      common.RID

	// For INSERT, DELETE
	TupleData []byte

	// For UPDATE
	OldTuple []byte
	NewTuple []byte

	// For NEW_PAGE
	PageID     common.PageID
	PrevPageID common.PageID

	// For CLR (Compensation Log Record)
	UndoNextLSN LSN
}

// NewLogRecord creates an empty log record.
func NewLogRecord() *LogRecord {
	return &LogRecord{
		LSN:     InvalidLSN,
		PrevLSN: InvalidLSN,
	}
}

// CreateBeginRecord creates a BEGIN log record.
func CreateBeginRecord(txnID common.TxnID) *LogRecord {
	return &LogRecord{
		TxnID: txnID,
		Type:  LogRecordBegin,
	}
}

// CreateCommitRecord creates a COMMIT log record.
func CreateCommitRecord(txnID common.TxnID, prevLSN LSN) *LogRecord {
	return &LogRecord{
		TxnID:   txnID,
		PrevLSN: prevLSN,
		Type:    LogRecordCommit,
	}
}

// CreateAbortRecord creates an ABORT log record.
func CreateAbortRecord(txnID common.TxnID, prevLSN LSN) *LogRecord {
	return &LogRecord{
		TxnID:   txnID,
		PrevLSN: prevLSN,
		Type:    LogRecordAbort,
	}
}

// CreateInsertRecord creates an INSERT log record.
func CreateInsertRecord(txnID common.TxnID, prevLSN LSN, tableOID uint32, rid common.RID, tuple []byte) *LogRecord {
	return &LogRecord{
		TxnID:     txnID,
		PrevLSN:   prevLSN,
		Type:      LogRecordInsert,
		TableOID:  tableOID,
		RID:       rid,
		TupleData: tuple,
	}
}

// CreateDeleteRecord creates a DELETE log record.
func CreateDeleteRecord(txnID common.TxnID, prevLSN LSN, tableOID uint32, rid common.RID, tuple []byte) *LogRecord {
	return &LogRecord{
		TxnID:     txnID,
		PrevLSN:   prevLSN,
		Type:      LogRecordMarkDelete,
		TableOID:  tableOID,
		RID:       rid,
		TupleData: tuple,
	}
}

// CreateUpdateRecord creates an UPDATE log record.
func CreateUpdateRecord(txnID common.TxnID, prevLSN LSN, tableOID uint32, rid common.RID, oldTuple, newTuple []byte) *LogRecord {
	return &LogRecord{
		TxnID:    txnID,
		PrevLSN:  prevLSN,
		Type:     LogRecordUpdate,
		TableOID: tableOID,
		RID:      rid,
		OldTuple: oldTuple,
		NewTuple: newTuple,
	}
}

// Serialize serializes the log record to bytes.
//
// TODO: Implement this
//
// STEPS:
// 1. Calculate total size
// 2. Write header fields
// 3. Write payload based on Type
// 4. Return byte slice
func (lr *LogRecord) Serialize() []byte {
	panic("LogRecord.Serialize not implemented")
}

// Deserialize deserializes a log record from bytes.
//
// TODO: Implement this
func DeserializeLogRecord(data []byte) *LogRecord {
	panic("DeserializeLogRecord not implemented")
}

// =============================================================================
// LOG MANAGER
// =============================================================================

// LogManager manages the write-ahead log.
//
// RESPONSIBILITIES:
// - Append log records to the log file
// - Assign LSNs
// - Flush logs to disk
// - Provide log records for recovery
//
// WAL PROTOCOL:
// 1. Before modifying a page, write log record
// 2. Before committing, flush all transaction's log records
// 3. Before flushing a dirty page, flush its page_lsn
//
// BUFFER MANAGEMENT:
// Log records are buffered in memory for efficiency.
// Flush happens:
// - Periodically (by background thread)
// - When buffer is full
// - On commit (force)
type LogManager struct {
	// diskManager writes to the log file.
	diskManager *disk.DiskManager

	// logBuffer holds log records before flushing.
	logBuffer []byte

	// flushBuffer is used during flush (double buffering).
	flushBuffer []byte

	// nextLSN is the next LSN to assign.
	nextLSN LSN

	// persistentLSN is the LSN up to which logs are persisted.
	persistentLSN LSN

	// mu protects the log buffer.
	mu sync.Mutex

	// flushMu serializes flush operations.
	flushMu sync.Mutex

	// flushCV signals when a flush completes.
	flushCV *sync.Cond

	// stopCh signals the background flush thread to stop.
	stopCh chan struct{}
}

// LogBufferSize is the size of the log buffer in bytes.
const LogBufferSize = 4096

// NewLogManager creates a new log manager.
func NewLogManager(diskManager *disk.DiskManager) *LogManager {
	lm := &LogManager{
		diskManager:   diskManager,
		logBuffer:     make([]byte, 0, LogBufferSize),
		flushBuffer:   make([]byte, 0, LogBufferSize),
		nextLSN:       0,
		persistentLSN: InvalidLSN,
		stopCh:        make(chan struct{}),
	}
	lm.flushCV = sync.NewCond(&lm.flushMu)
	return lm
}

// AppendLogRecord adds a log record to the buffer.
//
// TODO: Implement this
//
// STEPS:
// 1. Lock the buffer
// 2. Assign LSN to the record
// 3. Serialize the record
// 4. If buffer is full, flush first
// 5. Append to buffer
// 6. Update nextLSN
// 7. Return the assigned LSN
func (lm *LogManager) AppendLogRecord(record *LogRecord) LSN {
	panic("AppendLogRecord not implemented")
}

// Flush writes all buffered log records to disk.
//
// TODO: Implement this
//
// STEPS (with double buffering):
// 1. Lock buffer, swap logBuffer with flushBuffer
// 2. Unlock buffer (allows new appends)
// 3. Write flushBuffer to disk
// 4. Update persistentLSN
// 5. Signal waiting transactions
func (lm *LogManager) Flush() {
	panic("Flush not implemented")
}

// FlushLSN blocks until the given LSN is persisted.
//
// Used by commit to ensure durability before returning.
func (lm *LogManager) FlushLSN(lsn LSN) {
	lm.flushMu.Lock()
	defer lm.flushMu.Unlock()

	for lsn > lm.persistentLSN {
		lm.flushCV.Wait()
	}
}

// GetPersistentLSN returns the persistent LSN.
func (lm *LogManager) GetPersistentLSN() LSN {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.persistentLSN
}

// StartFlushThread starts the background flush thread.
//
// The thread periodically flushes the log buffer.
func (lm *LogManager) StartFlushThread() {
	go func() {
		// TODO: Implement periodic flushing
		// Flush every N milliseconds or when signaled
	}()
}

// StopFlushThread stops the background flush thread.
func (lm *LogManager) StopFlushThread() {
	close(lm.stopCh)
}

// =============================================================================
// LOG RECOVERY
// =============================================================================

// LogRecovery performs crash recovery using the log.
//
// ARIES RECOVERY (3 phases):
// ==========================
// 1. ANALYSIS: Scan log to determine:
//   - Active transactions at crash (ATT)
//   - Dirty pages (DPT)
//
// 2. REDO: Replay all logged changes:
//   - Start from earliest recLSN in DPT
//   - Apply changes that aren't already on disk
//   - "Repeat history"
//
// 3. UNDO: Rollback incomplete transactions:
//   - For each loser transaction
//   - Follow PrevLSN chain backwards
//   - Undo each change
//   - Write CLR records
type LogRecovery struct {
	diskManager *disk.DiskManager
	logManager  *LogManager
	// bpm         *buffer.BufferPoolManager

	// Active Transaction Table: txn_id -> last_lsn
	att map[common.TxnID]LSN

	// Dirty Page Table: page_id -> rec_lsn (first LSN to dirty this page)
	dpt map[common.PageID]LSN
}

// NewLogRecovery creates a new log recovery instance.
func NewLogRecovery(diskManager *disk.DiskManager, logManager *LogManager) *LogRecovery {
	return &LogRecovery{
		diskManager: diskManager,
		logManager:  logManager,
		att:         make(map[common.TxnID]LSN),
		dpt:         make(map[common.PageID]LSN),
	}
}

// Recover performs full crash recovery.
//
// TODO: Implement this
//
// STEPS:
// 1. Call Analysis()
// 2. Call Redo()
// 3. Call Undo()
func (lr *LogRecovery) Recover() {
	panic("Recover not implemented")
}

// Analysis scans the log to build ATT and DPT.
//
// TODO: Implement this
//
// STEPS:
// 1. Start from most recent checkpoint (or beginning)
// 2. Scan log forward
// 3. For each record:
//   - BEGIN: Add to ATT
//   - COMMIT/ABORT: Remove from ATT
//   - UPDATE/INSERT/DELETE: Update ATT, add to DPT if not present
//
// 4. ATT now contains loser transactions
// 5. DPT now contains pages that might need redo
func (lr *LogRecovery) Analysis() {
	panic("Analysis not implemented")
}

// Redo replays changes from the log.
//
// TODO: Implement this
//
// STEPS:
//  1. Find minimum recLSN in DPT
//  2. Scan log from that point forward
//  3. For each redo-able record:
//     a. Check if page is in DPT and recLSN <= record.LSN
//     b. Fetch page, check pageLSN < record.LSN
//     c. If both true, redo the operation
//     d. Update page's pageLSN
func (lr *LogRecovery) Redo() {
	panic("Redo not implemented")
}

// Undo rolls back loser transactions.
//
// TODO: Implement this
//
// STEPS:
//  1. Build todo list from ATT (last LSN of each loser)
//  2. While todo list not empty:
//     a. Pick record with largest LSN
//     b. Undo the operation
//     c. Write CLR record
//     d. Add PrevLSN to todo list (if not InvalidLSN)
//  3. Write ABORT record for each loser
func (lr *LogRecovery) Undo() {
	panic("Undo not implemented")
}

// =============================================================================
// CHECKPOINTING
// =============================================================================

// Checkpoint writes a checkpoint to the log.
//
// FUZZY CHECKPOINT:
// 1. Write CHECKPOINT_BEGIN
// 2. Record ATT and DPT
// 3. Write CHECKPOINT_END with ATT and DPT
//
// This doesn't require stopping transactions.
func (lm *LogManager) Checkpoint(att map[common.TxnID]LSN, dpt map[common.PageID]LSN) {
	// TODO: Implement this
	panic("Checkpoint not implemented")
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// GetIntFromBytes reads a 4-byte integer from a byte slice.
func GetIntFromBytes(data []byte, offset int) int32 {
	return int32(binary.LittleEndian.Uint32(data[offset:]))
}

// PutIntToBytes writes a 4-byte integer to a byte slice.
func PutIntToBytes(data []byte, offset int, value int32) {
	binary.LittleEndian.PutUint32(data[offset:], uint32(value))
}
