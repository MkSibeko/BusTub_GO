// Package page - Extendible Hash Table Page implementations.
//
// Extendible Hash Table uses three types of pages:
// - Header Page: Top-level directory of directories
// - Directory Page: Maps hash buckets to bucket pages
// - Bucket Page: Stores actual key-value pairs
//
// ASSIGNMENT 2 - EXTENDIBLE HASH TABLE:
// ======================================
// You need to implement these page layouts and the hash table operations.
//
// Structure Overview:
//
// Header Page
//
//	|
//	+-- Directory Page 0
//	|      +-- Bucket Page A
//	|      +-- Bucket Page B
//	|
//	+-- Directory Page 1
//	       +-- Bucket Page C
//	       +-- Bucket Page D
//
// Key Concepts:
// - Global Depth: Number of bits used to index into the directory
// - Local Depth: Number of bits used by each bucket
// - Bucket Splitting: When a bucket overflows, split it and increase local depth
// - Directory Doubling: When global depth increases, double the directory
//
// This mirrors:
// - bustub/src/include/storage/page/extendible_htable_header_page.h
// - bustub/src/include/storage/page/extendible_htable_directory_page.h
// - bustub/src/include/storage/page/extendible_htable_bucket_page.h
package page

import "github.com/bustub-go/pkg/common"

// =============================================================================
// EXTENDIBLE HASH TABLE CONSTANTS
// =============================================================================

const (
	// HTableHeaderMaxDepth is the maximum depth of the header.
	HTableHeaderMaxDepth = 9

	// HTableDirectoryMaxDepth is the maximum depth of a directory.
	HTableDirectoryMaxDepth = 9

	// HTableHeaderArraySize is the size of the directory page ID array in header.
	// 2^9 = 512 directory pages max
	HTableHeaderArraySize = 1 << HTableHeaderMaxDepth

	// HTableDirectoryArraySize is the size of the bucket page ID array in directory.
	// 2^9 = 512 buckets per directory
	HTableDirectoryArraySize = 1 << HTableDirectoryMaxDepth
)

// HTableBucketArraySize calculates the maximum entries in a bucket.
// Based on page size and entry size.
func HTableBucketArraySize(entrySize int) int {
	// Leave room for header (8 bytes: size + max_size + ...)
	usableSpace := common.PageSize - PageHeaderSize - 8
	return usableSpace / entrySize
}

// =============================================================================
// EXTENDIBLE HASH TABLE HEADER PAGE
// =============================================================================

// Header page layout
const (
	// OffsetHeaderMaxDepth is where max depth is stored.
	OffsetHeaderMaxDepth = PageHeaderSize + 0

	// OffsetHeaderDirectoryPageIDs is where the directory page ID array starts.
	OffsetHeaderDirectoryPageIDs = PageHeaderSize + 4
)

// ExtendibleHTableHeaderPage is the top-level page of the hash table.
//
// The header page maps high-order hash bits to directory pages.
// This allows the hash table to scale beyond a single directory page.
//
// Layout:
// +------------------+
// | Page Header      |
// +------------------+
// | Max Depth        | 4 bytes
// +------------------+
// | Directory IDs[]  | 4 bytes each
// +------------------+
//
// TODO (ASSIGNMENT 2):
// Implement the following methods:
// - Init() - Initialize the header page
// - HashToDirectoryIndex() - Map hash to directory page
// - GetDirectoryPageID() - Get directory page ID for an index
// - SetDirectoryPageID() - Set directory page ID
// - MaxSize() - Return maximum number of directory pages
type ExtendibleHTableHeaderPage struct {
	data []byte
}

// NewExtendibleHTableHeaderPage creates a header page view.
func NewExtendibleHTableHeaderPage(data []byte) *ExtendibleHTableHeaderPage {
	return &ExtendibleHTableHeaderPage{data: data}
}

// Init initializes the header page.
//
// TODO (ASSIGNMENT): Implement this method.
// - Set max depth
// - Initialize all directory page IDs to InvalidPageID
func (p *ExtendibleHTableHeaderPage) Init(maxDepth uint32) {
	SetPageType(p.data, PageTypeHashTableHeader)
	WriteUint32(p.data, OffsetHeaderMaxDepth, maxDepth)

	// Initialize all directory entries to invalid
	for i := 0; i < int(1<<maxDepth); i++ {
		offset := OffsetHeaderDirectoryPageIDs + i*4
		WriteInt32(p.data, offset, int32(common.InvalidPageID))
	}
}

// GetMaxDepth returns the maximum depth of the header.
func (p *ExtendibleHTableHeaderPage) GetMaxDepth() uint32 {
	return ReadUint32(p.data, OffsetHeaderMaxDepth)
}

// HashToDirectoryIndex maps a hash value to a directory page index.
//
// TODO (ASSIGNMENT): Implement this method.
// Use the high-order bits of the hash to index into the directory array.
// The number of bits used is determined by max_depth.
func (p *ExtendibleHTableHeaderPage) HashToDirectoryIndex(hash uint32) uint32 {
	// TODO: Extract high-order bits from hash
	// Hint: hash >> (32 - maxDepth)
	panic("TODO: Implement ExtendibleHTableHeaderPage.HashToDirectoryIndex()")
}

// GetDirectoryPageID returns the directory page ID for an index.
func (p *ExtendibleHTableHeaderPage) GetDirectoryPageID(directoryIdx uint32) common.PageID {
	offset := OffsetHeaderDirectoryPageIDs + int(directoryIdx)*4
	return common.PageID(ReadInt32(p.data, offset))
}

// SetDirectoryPageID sets the directory page ID for an index.
func (p *ExtendibleHTableHeaderPage) SetDirectoryPageID(directoryIdx uint32, pageID common.PageID) {
	offset := OffsetHeaderDirectoryPageIDs + int(directoryIdx)*4
	WriteInt32(p.data, offset, int32(pageID))
}

// MaxSize returns the maximum number of directory pages.
func (p *ExtendibleHTableHeaderPage) MaxSize() uint32 {
	return 1 << p.GetMaxDepth()
}

// =============================================================================
// EXTENDIBLE HASH TABLE DIRECTORY PAGE
// =============================================================================

// Directory page layout
const (
	// OffsetDirectoryMaxDepth is where max depth is stored.
	OffsetDirectoryMaxDepth = PageHeaderSize + 0

	// OffsetDirectoryGlobalDepth is where global depth is stored.
	OffsetDirectoryGlobalDepth = PageHeaderSize + 4

	// OffsetDirectoryLocalDepths is where local depths array starts.
	OffsetDirectoryLocalDepths = PageHeaderSize + 8

	// OffsetDirectoryBucketPageIDs is where bucket page IDs start.
	// After local depths: 8 + 512 bytes = 520
	OffsetDirectoryBucketPageIDs = PageHeaderSize + 8 + HTableDirectoryArraySize
)

// ExtendibleHTableDirectoryPage maps hash values to bucket pages.
//
// The directory page contains:
// - Global depth: Number of bits used to index into this directory
// - Local depths: Array of local depths for each bucket
// - Bucket page IDs: Array of page IDs for each bucket
//
// Layout:
// +------------------+
// | Page Header      |
// +------------------+
// | Max Depth        | 4 bytes
// | Global Depth     | 4 bytes
// +------------------+
// | Local Depths[]   | 1 byte each (512 bytes total)
// +------------------+
// | Bucket IDs[]     | 4 bytes each
// +------------------+
//
// TODO (ASSIGNMENT 2):
// Implement the following methods:
// - Init() - Initialize the directory page
// - HashToBucketIndex() - Map hash to bucket index
// - GetBucketPageID() - Get bucket page ID
// - SetBucketPageID() - Set bucket page ID
// - GetLocalDepth() - Get local depth for a bucket
// - SetLocalDepth() - Set local depth
// - IncrGlobalDepth() - Increase global depth (directory doubling)
// - DecrGlobalDepth() - Decrease global depth
// - CanShrink() - Check if directory can shrink
// - GetSplitImageIndex() - Get sibling bucket index after split
type ExtendibleHTableDirectoryPage struct {
	data []byte
}

// NewExtendibleHTableDirectoryPage creates a directory page view.
func NewExtendibleHTableDirectoryPage(data []byte) *ExtendibleHTableDirectoryPage {
	return &ExtendibleHTableDirectoryPage{data: data}
}

// Init initializes the directory page.
//
// TODO (ASSIGNMENT): Implement this method.
func (p *ExtendibleHTableDirectoryPage) Init(maxDepth uint32) {
	SetPageType(p.data, PageTypeHashTableDirectory)
	WriteUint32(p.data, OffsetDirectoryMaxDepth, maxDepth)
	WriteUint32(p.data, OffsetDirectoryGlobalDepth, 0)

	// Initialize local depths and bucket IDs
	for i := 0; i < HTableDirectoryArraySize; i++ {
		p.data[OffsetDirectoryLocalDepths+i] = 0
		WriteInt32(p.data, OffsetDirectoryBucketPageIDs+i*4, int32(common.InvalidPageID))
	}
}

// GetMaxDepth returns the maximum depth.
func (p *ExtendibleHTableDirectoryPage) GetMaxDepth() uint32 {
	return ReadUint32(p.data, OffsetDirectoryMaxDepth)
}

// GetGlobalDepth returns the global depth.
func (p *ExtendibleHTableDirectoryPage) GetGlobalDepth() uint32 {
	return ReadUint32(p.data, OffsetDirectoryGlobalDepth)
}

// SetGlobalDepth sets the global depth.
func (p *ExtendibleHTableDirectoryPage) SetGlobalDepth(globalDepth uint32) {
	WriteUint32(p.data, OffsetDirectoryGlobalDepth, globalDepth)
}

// GetLocalDepth returns the local depth for a bucket.
func (p *ExtendibleHTableDirectoryPage) GetLocalDepth(bucketIdx uint32) uint32 {
	return uint32(p.data[OffsetDirectoryLocalDepths+int(bucketIdx)])
}

// SetLocalDepth sets the local depth for a bucket.
func (p *ExtendibleHTableDirectoryPage) SetLocalDepth(bucketIdx uint32, localDepth uint32) {
	p.data[OffsetDirectoryLocalDepths+int(bucketIdx)] = byte(localDepth)
}

// GetBucketPageID returns the bucket page ID for an index.
func (p *ExtendibleHTableDirectoryPage) GetBucketPageID(bucketIdx uint32) common.PageID {
	offset := OffsetDirectoryBucketPageIDs + int(bucketIdx)*4
	return common.PageID(ReadInt32(p.data, offset))
}

// SetBucketPageID sets the bucket page ID for an index.
func (p *ExtendibleHTableDirectoryPage) SetBucketPageID(bucketIdx uint32, pageID common.PageID) {
	offset := OffsetDirectoryBucketPageIDs + int(bucketIdx)*4
	WriteInt32(p.data, offset, int32(pageID))
}

// Size returns the current size of the directory (2^global_depth).
func (p *ExtendibleHTableDirectoryPage) Size() uint32 {
	return 1 << p.GetGlobalDepth()
}

// HashToBucketIndex maps a hash value to a bucket index.
//
// TODO (ASSIGNMENT): Implement this method.
// Use the low-order bits of the hash (global_depth bits).
func (p *ExtendibleHTableDirectoryPage) HashToBucketIndex(hash uint32) uint32 {
	// TODO: hash & ((1 << globalDepth) - 1)
	panic("TODO: Implement ExtendibleHTableDirectoryPage.HashToBucketIndex()")
}

// IncrGlobalDepth increases the global depth by 1.
//
// TODO (ASSIGNMENT): Implement this method.
// This doubles the directory size. Copy entries appropriately.
func (p *ExtendibleHTableDirectoryPage) IncrGlobalDepth() {
	// TODO: Implement directory doubling
	panic("TODO: Implement ExtendibleHTableDirectoryPage.IncrGlobalDepth()")
}

// DecrGlobalDepth decreases the global depth by 1.
//
// TODO (ASSIGNMENT): Implement this method.
func (p *ExtendibleHTableDirectoryPage) DecrGlobalDepth() {
	// TODO: Implement directory shrinking
	panic("TODO: Implement ExtendibleHTableDirectoryPage.DecrGlobalDepth()")
}

// CanShrink returns true if the directory can shrink.
//
// TODO (ASSIGNMENT): Implement this method.
// The directory can shrink if all local depths are less than global depth.
func (p *ExtendibleHTableDirectoryPage) CanShrink() bool {
	panic("TODO: Implement ExtendibleHTableDirectoryPage.CanShrink()")
}

// GetSplitImageIndex returns the split image index for a bucket.
//
// TODO (ASSIGNMENT): Implement this method.
// The split image is the sibling bucket after a split.
// It differs from the original bucket in the bit at position local_depth.
func (p *ExtendibleHTableDirectoryPage) GetSplitImageIndex(bucketIdx uint32) uint32 {
	panic("TODO: Implement ExtendibleHTableDirectoryPage.GetSplitImageIndex()")
}

// =============================================================================
// EXTENDIBLE HASH TABLE BUCKET PAGE
// =============================================================================

// Bucket page layout
const (
	// OffsetBucketSize is where the current size is stored.
	OffsetBucketSize = PageHeaderSize + 0

	// OffsetBucketMaxSize is where the maximum size is stored.
	OffsetBucketMaxSize = PageHeaderSize + 4

	// OffsetBucketEntries is where key-value entries start.
	OffsetBucketEntries = PageHeaderSize + 8
)

// ExtendibleHTableBucketPage stores actual key-value pairs.
//
// Bucket pages are the leaf nodes of the hash table.
// They store entries as (key, value) pairs.
//
// Layout:
// +------------------+
// | Page Header      |
// +------------------+
// | Size             | 4 bytes
// | Max Size         | 4 bytes
// +------------------+
// | Entry[0]         | key + value
// | Entry[1]         |
// | ...              |
// +------------------+
//
// TODO (ASSIGNMENT 2):
// Implement the following methods:
// - Init() - Initialize the bucket page
// - Lookup() - Find value for a key
// - Insert() - Insert key-value pair
// - Remove() - Remove key-value pair
// - IsFull() - Check if bucket is full
// - IsEmpty() - Check if bucket is empty
// - EntryAt() - Get entry at index
type ExtendibleHTableBucketPage[K any, V any] struct {
	data      []byte
	keySize   int
	valueSize int
}

// NewExtendibleHTableBucketPage creates a bucket page view.
func NewExtendibleHTableBucketPage[K any, V any](data []byte, keySize int, valueSize int) *ExtendibleHTableBucketPage[K, V] {
	return &ExtendibleHTableBucketPage[K, V]{
		data:      data,
		keySize:   keySize,
		valueSize: valueSize,
	}
}

// Init initializes the bucket page.
func (p *ExtendibleHTableBucketPage[K, V]) Init(maxSize uint32) {
	SetPageType(p.data, PageTypeHashTableBucket)
	WriteUint32(p.data, OffsetBucketSize, 0)
	WriteUint32(p.data, OffsetBucketMaxSize, maxSize)
}

// GetSize returns the current number of entries.
func (p *ExtendibleHTableBucketPage[K, V]) GetSize() uint32 {
	return ReadUint32(p.data, OffsetBucketSize)
}

// SetSize sets the current number of entries.
func (p *ExtendibleHTableBucketPage[K, V]) SetSize(size uint32) {
	WriteUint32(p.data, OffsetBucketSize, size)
}

// GetMaxSize returns the maximum number of entries.
func (p *ExtendibleHTableBucketPage[K, V]) GetMaxSize() uint32 {
	return ReadUint32(p.data, OffsetBucketMaxSize)
}

// IsFull returns true if the bucket is full.
func (p *ExtendibleHTableBucketPage[K, V]) IsFull() bool {
	return p.GetSize() >= p.GetMaxSize()
}

// IsEmpty returns true if the bucket is empty.
func (p *ExtendibleHTableBucketPage[K, V]) IsEmpty() bool {
	return p.GetSize() == 0
}

// GetEntrySize returns the size of one entry.
func (p *ExtendibleHTableBucketPage[K, V]) GetEntrySize() int {
	return p.keySize + p.valueSize
}

// GetEntryOffset returns the byte offset for an entry at index.
func (p *ExtendibleHTableBucketPage[K, V]) GetEntryOffset(index uint32) int {
	return OffsetBucketEntries + int(index)*p.GetEntrySize()
}

// TODO (ASSIGNMENT): Implement the following methods:
// - Lookup(key K, comparator) (V, bool)
// - Insert(key K, value V, comparator) bool
// - Remove(key K, comparator) bool
// - KeyAt(index uint32) K
// - ValueAt(index uint32) V
