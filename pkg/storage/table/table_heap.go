// Package table provides table storage functionality.
//
// The table storage layer includes:
// - TableHeap: A collection of table pages storing tuples
// - Tuple: A record in the database
// - TableIterator: Sequential scan over a table
//
// ASSIGNMENT NOTES:
// ==================
// Understanding table storage is important for:
// - SeqScan executor (iterates over TableHeap)
// - Insert/Update/Delete executors
// - Recovery and concurrency control
//
// This mirrors: bustub/src/include/storage/table/
package table

import (
	"github.com/bustub-go/pkg/buffer"
	"github.com/bustub-go/pkg/catalog"
	"github.com/bustub-go/pkg/common"
)

// TableHeap is an unordered physical collection of tuples.
//
// The table heap is a doubly-linked list of pages. Each page
// contains tuples using the slotted page format.
//
// Operations:
// - InsertTuple: Add a new tuple
// - UpdateTuple: Modify an existing tuple in place
// - UpdateTupleInPlace: Modify specific columns
// - DeleteTuple: Mark a tuple as deleted
// - GetTuple: Retrieve a tuple by RID
//
// IMPLEMENTATION NOTES:
// =====================
//  1. The heap maintains a first_page_id_ that points to the
//     first page in the doubly-linked list.
//
//  2. When inserting, we scan pages for one with enough space.
//     A smarter implementation might use a free space map.
//
//  3. For MVCC, tuples have metadata (timestamp, prev version).
//     The actual tuple data may be stored elsewhere (undo log).
type TableHeap struct {
	// bpm is the buffer pool manager.
	bpm *buffer.BufferPoolManager

	// firstPageID is the page ID of the first page in the heap.
	firstPageID common.PageID

	// lastPageID is the page ID of the last page (for fast inserts).
	lastPageID common.PageID

	// numPages tracks the number of pages in the heap.
	numPages int
}

// NewTableHeap creates a new, empty table heap.
//
// This allocates a new page for the heap and initializes it.
func NewTableHeap(bpm *buffer.BufferPoolManager) *TableHeap {
	// TODO: Implement this
	//
	// ASSIGNMENT STEPS:
	// 1. Allocate a new page from the buffer pool
	// 2. Initialize the page as a table page
	// 3. Set the prev/next page links to INVALID_PAGE_ID
	// 4. Store the page ID as firstPageID and lastPageID
	// 5. Don't forget to unpin the page!

	panic("NewTableHeap not implemented")
}

// NewTableHeapFromExisting creates a TableHeap from an existing first page.
//
// Used when recovering or opening an existing table.
func NewTableHeapFromExisting(bpm *buffer.BufferPoolManager, firstPageID common.PageID) *TableHeap {
	return &TableHeap{
		bpm:         bpm,
		firstPageID: firstPageID,
	}
}

// InsertTuple inserts a tuple into the table.
//
// Returns the RID of the inserted tuple, or an error if the
// insertion fails (e.g., tuple too large).
//
// Parameters:
// - meta: Tuple metadata (for MVCC)
// - tuple: The tuple data to insert
//
// For MVCC, also specify the transaction context.
func (th *TableHeap) InsertTuple(meta TupleMeta, tuple *Tuple) (common.RID, error) {
	// TODO: Implement this
	//
	// ASSIGNMENT STEPS:
	// 1. Calculate the size needed: tuple.Size() + slot directory entry
	// 2. Scan pages starting from lastPageID looking for space
	// 3. If no page has space, allocate a new page
	// 4. Insert into the page's slot directory
	// 5. Copy the tuple data into the page
	// 6. Update the page's free space pointers
	// 7. Return the RID (page_id, slot_num)
	//
	// CONCURRENCY NOTES:
	// - Acquire page latch before modifying
	// - For MVCC, check tuple size against inline threshold
	//   and potentially store in undo log

	panic("InsertTuple not implemented")
}

// UpdateTupleMeta updates only the metadata of a tuple.
//
// This is used for MVCC operations where we need to update
// the timestamp or delete flag without changing the data.
func (th *TableHeap) UpdateTupleMeta(meta TupleMeta, rid common.RID) error {
	// TODO: Implement this
	//
	// ASSIGNMENT STEPS:
	// 1. Fetch the page containing the RID
	// 2. Get the slot from the slot directory
	// 3. Update the metadata portion of the tuple
	// 4. Mark page as dirty

	panic("UpdateTupleMeta not implemented")
}

// GetTuple retrieves a tuple by RID.
//
// Returns the tuple metadata and data, or error if not found.
func (th *TableHeap) GetTuple(rid common.RID) (TupleMeta, *Tuple, error) {
	// TODO: Implement this
	//
	// ASSIGNMENT STEPS:
	// 1. Fetch the page using the RID's page ID
	// 2. Look up the slot in the slot directory
	// 3. Read the tuple metadata
	// 4. Read and deserialize the tuple data
	// 5. Return both

	panic("GetTuple not implemented")
}

// GetTupleMeta returns only the metadata for a tuple.
//
// This is faster than GetTuple when we only need metadata.
func (th *TableHeap) GetTupleMeta(rid common.RID) (TupleMeta, error) {
	// TODO: Implement this

	panic("GetTupleMeta not implemented")
}

// MakeIterator returns an iterator at the beginning of the table.
func (th *TableHeap) MakeIterator() *TableIterator {
	return &TableIterator{
		heap:       th,
		curPageID:  th.firstPageID,
		curSlotNum: 0,
	}
}

// MakeEagerIterator returns an iterator that pre-fetches tuples.
func (th *TableHeap) MakeEagerIterator() *TableIterator {
	// For now, same as regular iterator
	return th.MakeIterator()
}

// GetFirstPageID returns the first page ID in the heap.
func (th *TableHeap) GetFirstPageID() common.PageID {
	return th.firstPageID
}

// =============================================================================
// TUPLE
// =============================================================================

// Tuple represents a record in the database.
//
// A tuple contains:
// - A RID (Record ID) identifying its location
// - Serialized data based on a schema
type Tuple struct {
	// rid is the record identifier (page + slot).
	rid common.RID

	// data is the serialized tuple data.
	data []byte
}

// NewTuple creates a new tuple with the given data.
func NewTuple(data []byte) *Tuple {
	t := &Tuple{
		data: make([]byte, len(data)),
	}
	copy(t.data, data)
	return t
}

// NewTupleFromRID creates a tuple with a specific RID.
func NewTupleFromRID(rid common.RID, data []byte) *Tuple {
	t := NewTuple(data)
	t.rid = rid
	return t
}

// GetRID returns the tuple's record ID.
func (t *Tuple) GetRID() common.RID {
	return t.rid
}

// SetRID sets the tuple's record ID.
func (t *Tuple) SetRID(rid common.RID) {
	t.rid = rid
}

// GetData returns the raw tuple data.
func (t *Tuple) GetData() []byte {
	return t.data
}

// Size returns the size of the tuple in bytes.
func (t *Tuple) Size() int {
	return len(t.data)
}

// GetValue extracts a value from the tuple using the schema.
//
// Parameters:
// - schema: The schema describing the tuple layout
// - colIdx: The column index to extract
//
// TODO: Implement this to deserialize values from the byte array
func (t *Tuple) GetValue(schema *catalog.Schema, colIdx int) interface{} {
	// TODO: Implement this
	//
	// STEPS:
	// 1. Get the column from the schema
	// 2. Get the column's offset
	// 3. Based on the column type, deserialize the bytes
	// 4. Return the value

	panic("GetValue not implemented")
}

// SerializeValue serializes a value into the tuple at the given offset.
// This is used when constructing tuples for insertion.
func (t *Tuple) SerializeValue(schema *catalog.Schema, colIdx int, value interface{}) {
	// TODO: Implement this

	panic("SerializeValue not implemented")
}

// =============================================================================
// TUPLE METADATA (for MVCC)
// =============================================================================

// TupleMeta contains metadata about a tuple version.
//
// This is used for MVCC to track:
// - When the tuple was created/deleted
// - Whether it's been deleted
// - Links to previous versions
type TupleMeta struct {
	// Timestamp is the transaction timestamp that created this version.
	Timestamp common.TxnTimestamp

	// IsDeleted indicates if this tuple has been deleted.
	IsDeleted bool

	// PrevVersion points to the previous version in the undo log.
	// This is INVALID_TXN_ID if there's no previous version.
	// PrevVersion UndoLink // Will be defined in concurrency package
}

// TupleMetaSize is the size of tuple metadata in bytes.
const TupleMetaSize = 8 + 1 // timestamp + deleted flag

// =============================================================================
// TABLE ITERATOR
// =============================================================================

// TableIterator provides sequential access to tuples in a table.
//
// Usage:
//
//	iter := heap.MakeIterator()
//	for !iter.IsEnd() {
//	    meta, tuple := iter.GetTuple()
//	    // Process tuple
//	    iter.Next()
//	}
type TableIterator struct {
	// heap is the table being iterated.
	heap *TableHeap

	// curPageID is the current page.
	curPageID common.PageID

	// curSlotNum is the current slot within the page.
	curSlotNum int

	// pageGuard holds the current page (for locking).
	// pageGuard *buffer.ReadPageGuard
}

// IsEnd returns true if the iterator is past the last tuple.
func (ti *TableIterator) IsEnd() bool {
	return ti.curPageID == common.InvalidPageID
}

// Next advances to the next tuple.
//
// IMPLEMENTATION NOTES:
// 1. Increment slot number
// 2. If past the last slot on this page, move to next page
// 3. If no more pages, set curPageID to INVALID
func (ti *TableIterator) Next() {
	// TODO: Implement this
	//
	// STEPS:
	// 1. Get the current page and find how many tuples it has
	// 2. Increment slot number
	// 3. If slot >= num_tuples, get the next page ID from page header
	// 4. If next page is INVALID, we're at the end
	// 5. Otherwise, move to next page and reset slot to 0

	panic("TableIterator.Next not implemented")
}

// GetTuple returns the current tuple.
func (ti *TableIterator) GetTuple() (TupleMeta, *Tuple) {
	// TODO: Implement this
	//
	// STEPS:
	// 1. Create RID from current page and slot
	// 2. Call heap.GetTuple with that RID
	// 3. Return the result

	panic("TableIterator.GetTuple not implemented")
}

// GetRID returns the RID of the current tuple.
func (ti *TableIterator) GetRID() common.RID {
	return common.NewRID(ti.curPageID, uint32(ti.curSlotNum))
}
