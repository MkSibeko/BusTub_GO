// Package common - RID (Record Identifier) implementation.
//
// RID uniquely identifies a tuple (row) within the database.
// It is composed of:
// - PageID: Which page contains the tuple
// - SlotOffset: Position within that page's slot array
//
// ASSIGNMENT NOTES:
// RID is used throughout the system to reference specific tuples:
// - Table heap stores tuples and returns RIDs
// - Indexes map keys to RIDs
// - Executors use RIDs to fetch/update/delete tuples
package common

import "fmt"

// RID (Record Identifier) uniquely identifies a tuple in the database.
//
// A tuple's location is determined by:
// 1. PageID - which page in the table heap contains the tuple
// 2. SlotOffset - which slot within that page's slot array
//
// This is a common pattern in database systems for tuple addressing.
type RID struct {
	// PageID identifies which page contains this tuple.
	PageID PageID

	// SlotOffset is the index into the page's slot array.
	// The slot array maps slot offsets to actual byte offsets within the page.
	SlotOffset SlotOffset
}

// NewRID creates a new RID with the given page ID and slot offset.
func NewRID(pageID PageID, slotOffset SlotOffset) RID {
	return RID{
		PageID:     pageID,
		SlotOffset: slotOffset,
	}
}

// InvalidRID returns an RID representing an invalid/uninitialized record.
func InvalidRID() RID {
	return RID{
		PageID:     InvalidPageID,
		SlotOffset: 0,
	}
}

// IsValid returns true if this RID represents a valid record location.
func (r RID) IsValid() bool {
	return r.PageID != InvalidPageID
}

// Equals checks if two RIDs refer to the same tuple.
func (r RID) Equals(other RID) bool {
	return r.PageID == other.PageID && r.SlotOffset == other.SlotOffset
}

// String returns a human-readable representation of the RID.
func (r RID) String() string {
	return fmt.Sprintf("RID(page=%d, slot=%d)", r.PageID, r.SlotOffset)
}

// Less returns true if this RID is "less than" another RID.
// Comparison is done first by PageID, then by SlotOffset.
// This is useful for ordering RIDs in data structures.
func (r RID) Less(other RID) bool {
	if r.PageID != other.PageID {
		return r.PageID < other.PageID
	}
	return r.SlotOffset < other.SlotOffset
}

// GetPageID returns the page ID component of this RID.
func (r RID) GetPageID() PageID {
	return r.PageID
}

// GetSlotOffset returns the slot offset component of this RID.
func (r RID) GetSlotOffset() SlotOffset {
	return r.SlotOffset
}
