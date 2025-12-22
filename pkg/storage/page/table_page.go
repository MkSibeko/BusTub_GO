// Package page - Table Page implementation.
//
// TablePage stores tuples in a slotted page format.
// This is the storage format used by the table heap.
//
// ASSIGNMENT NOTES:
// ==================
// Understanding the table page layout is important for:
// - Assignment 3 (Query Execution) - reading and writing tuples
// - Assignment 4 (Concurrency) - understanding MVCC implementation
//
// Page Layout:
// +----------------+
// | Page Header    | <- Common header (8 bytes)
// +----------------+
// | Table Header   | <- Table-specific header
// +----------------+
// | Slot Array     | <- Grows downward from header
// | ...            |
// +----------------+
// | Free Space     |
// +----------------+
// | ...            |
// | Tuple Data     | <- Grows upward from bottom
// +----------------+
//
// This mirrors: bustub/src/include/storage/page/table_page.h
package page

import (
	"github.com/bustub-go/pkg/common"
)

// =============================================================================
// TABLE PAGE CONSTANTS
// =============================================================================

// Table page header layout (after common page header)
const (
	// TablePageHeaderSize is the size of the table-specific header.
	TablePageHeaderSize = 12

	// OffsetNextPageID is the offset of the next page pointer.
	OffsetNextPageID = PageHeaderSize + 0

	// OffsetNumTuples is the offset of the tuple count.
	OffsetNumTuples = PageHeaderSize + 4

	// OffsetNumDeletedTuples is the offset of deleted tuple count.
	OffsetNumDeletedTuples = PageHeaderSize + 6

	// OffsetFreeSpacePointer is the offset of the free space pointer.
	OffsetFreeSpacePointer = PageHeaderSize + 8

	// SlotArrayStart is where the slot array begins.
	SlotArrayStart = PageHeaderSize + TablePageHeaderSize
)

// Slot entry size (each slot is 4 bytes: 2 for offset, 2 for size)
const SlotSize = 4

// TupleMeta contains metadata for a tuple.
//
// This is stored alongside each tuple in the table heap.
// It's used for MVCC and deletion tracking.
type TupleMeta struct {
	// Timestamp is the transaction timestamp for MVCC.
	Timestamp common.Timestamp

	// IsDeleted indicates if the tuple has been deleted.
	IsDeleted bool
}

// TablePage provides methods to manipulate a table page.
//
// Table pages use a slotted page format:
// - Header at the top contains metadata
// - Slot array grows down from the header
// - Tuple data grows up from the bottom
// - Free space is in the middle
//
// Each slot contains:
// - Offset: Where the tuple data starts (from page bottom)
// - Size: Size of the tuple data in bytes
//
// TODO (ASSIGNMENT):
// ==================
// This structure is provided. You need to understand:
// 1. How to read tuples using the slot array
// 2. How to insert tuples and update the slot array
// 3. How to handle tuple deletion (mark deleted, don't remove)
// 4. How the free space pointer works
type TablePage struct {
	// data is the raw page data.
	data []byte
}

// NewTablePage creates a table page view over raw data.
func NewTablePage(data []byte) *TablePage {
	return &TablePage{data: data}
}

// Init initializes a new table page.
//
// Sets up the header with:
// - Next page ID = InvalidPageID
// - Num tuples = 0
// - Free space pointer = end of page
func (p *TablePage) Init() {
	SetPageType(p.data, PageTypeTable)
	p.SetNextPageID(common.InvalidPageID)
	p.SetNumTuples(0)
	p.SetNumDeletedTuples(0)
	p.SetFreeSpacePointer(uint16(common.PageSize))
}

// GetNextPageID returns the ID of the next page in the table heap.
func (p *TablePage) GetNextPageID() common.PageID {
	return common.PageID(ReadInt32(p.data, OffsetNextPageID))
}

// SetNextPageID sets the ID of the next page.
func (p *TablePage) SetNextPageID(pageID common.PageID) {
	WriteInt32(p.data, OffsetNextPageID, int32(pageID))
}

// GetNumTuples returns the number of tuples in the page.
func (p *TablePage) GetNumTuples() uint16 {
	return ReadUint16(p.data, OffsetNumTuples)
}

// SetNumTuples sets the number of tuples.
func (p *TablePage) SetNumTuples(numTuples uint16) {
	WriteUint16(p.data, OffsetNumTuples, numTuples)
}

// GetNumDeletedTuples returns the number of deleted tuples.
func (p *TablePage) GetNumDeletedTuples() uint16 {
	return ReadUint16(p.data, OffsetNumDeletedTuples)
}

// SetNumDeletedTuples sets the number of deleted tuples.
func (p *TablePage) SetNumDeletedTuples(numDeleted uint16) {
	WriteUint16(p.data, OffsetNumDeletedTuples, numDeleted)
}

// GetFreeSpacePointer returns the pointer to free space.
func (p *TablePage) GetFreeSpacePointer() uint16 {
	return ReadUint16(p.data, OffsetFreeSpacePointer)
}

// SetFreeSpacePointer sets the free space pointer.
func (p *TablePage) SetFreeSpacePointer(fsp uint16) {
	WriteUint16(p.data, OffsetFreeSpacePointer, fsp)
}

// GetFreeSpaceRemaining returns the amount of free space available.
func (p *TablePage) GetFreeSpaceRemaining() uint16 {
	slotArrayEnd := SlotArrayStart + int(p.GetNumTuples())*SlotSize
	return p.GetFreeSpacePointer() - uint16(slotArrayEnd)
}

// GetSlotOffset returns the slot array entry for a given slot.
func (p *TablePage) GetSlotOffset(slotNum uint16) int {
	return SlotArrayStart + int(slotNum)*SlotSize
}

// GetTupleOffset returns the offset of a tuple's data.
func (p *TablePage) GetTupleOffset(slotNum uint16) uint16 {
	slotOffset := p.GetSlotOffset(slotNum)
	return ReadUint16(p.data, slotOffset)
}

// GetTupleSize returns the size of a tuple.
func (p *TablePage) GetTupleSize(slotNum uint16) uint16 {
	slotOffset := p.GetSlotOffset(slotNum)
	return ReadUint16(p.data, slotOffset+2)
}

// SetSlot sets the offset and size for a slot.
func (p *TablePage) SetSlot(slotNum uint16, offset uint16, size uint16) {
	slotOffset := p.GetSlotOffset(slotNum)
	WriteUint16(p.data, slotOffset, offset)
	WriteUint16(p.data, slotOffset+2, size)
}

// InsertTuple inserts a tuple into the page.
//
// TODO (ASSIGNMENT):
// ==================
// Insert a tuple and return its slot number.
//
// Steps:
// 1. Check if there's enough space for the tuple + slot entry
// 2. Calculate the new tuple offset (free space pointer - tuple size)
// 3. Copy tuple data to that offset
// 4. Add slot entry pointing to the tuple
// 5. Update free space pointer
// 6. Increment tuple count
//
// Returns:
// - The slot number where the tuple was inserted
// - false if there's not enough space
func (p *TablePage) InsertTuple(meta TupleMeta, tupleData []byte) (uint16, bool) {
	// TODO (ASSIGNMENT): Implement InsertTuple
	//
	// tupleSize := uint16(len(tupleData))
	// spaceNeeded := tupleSize + SlotSize // tuple data + slot entry
	//
	// if p.GetFreeSpaceRemaining() < spaceNeeded {
	//     return 0, false
	// }
	//
	// slotNum := p.GetNumTuples()
	// newOffset := p.GetFreeSpacePointer() - tupleSize
	//
	// // Copy tuple data
	// copy(p.data[newOffset:], tupleData)
	//
	// // Set slot entry
	// p.SetSlot(slotNum, newOffset, tupleSize)
	//
	// // Update header
	// p.SetFreeSpacePointer(newOffset)
	// p.SetNumTuples(slotNum + 1)
	//
	// return slotNum, true

	panic("TODO: Implement TablePage.InsertTuple()")
}

// GetTuple retrieves a tuple by slot number.
//
// TODO (ASSIGNMENT):
// ==================
// Get the tuple data and metadata for a given slot.
//
// Steps:
// 1. Validate slot number
// 2. Get offset and size from slot array
// 3. Copy tuple data from that offset
// 4. Return tuple data and metadata
func (p *TablePage) GetTuple(slotNum uint16) (TupleMeta, []byte, bool) {
	// TODO (ASSIGNMENT): Implement GetTuple
	//
	// if slotNum >= p.GetNumTuples() {
	//     return TupleMeta{}, nil, false
	// }
	//
	// offset := p.GetTupleOffset(slotNum)
	// size := p.GetTupleSize(slotNum)
	//
	// tupleData := make([]byte, size)
	// copy(tupleData, p.data[offset:offset+size])
	//
	// // TODO: Extract metadata from tuple data or separate storage
	// meta := TupleMeta{}
	//
	// return meta, tupleData, true

	panic("TODO: Implement TablePage.GetTuple()")
}

// UpdateTupleMeta updates the metadata for a tuple.
//
// Used for marking tuples as deleted or updating timestamps.
func (p *TablePage) UpdateTupleMeta(slotNum uint16, meta TupleMeta) bool {
	// TODO (ASSIGNMENT): Implement UpdateTupleMeta
	// This depends on where metadata is stored

	panic("TODO: Implement TablePage.UpdateTupleMeta()")
}
