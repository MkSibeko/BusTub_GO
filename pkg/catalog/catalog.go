// Package catalog provides database catalog functionality.
//
// The catalog stores metadata about:
// - Tables: Name, schema, heap location
// - Indexes: Name, type, key columns
//
// ASSIGNMENT NOTES:
// ==================
// The catalog is provided but understanding it is important for:
// - Creating and looking up tables in executors
// - Creating and using indexes for query optimization
//
// This mirrors: bustub/src/include/catalog/catalog.h
package catalog

import (
	"sync"
	"sync/atomic"

	"github.com/bustub-go/pkg/buffer"
	"github.com/bustub-go/pkg/common"
	"github.com/bustub-go/pkg/storage/index"
)

// TableOID is the object identifier for a table.
type TableOID = uint32

// IndexOID is the object identifier for an index.
type IndexOID = uint32

// IndexType represents the type of index.
type IndexType int

const (
	// IndexTypeBPlusTree is a B+ tree index.
	IndexTypeBPlusTree IndexType = iota

	// IndexTypeHashTable is a hash table index.
	IndexTypeHashTable

	// IndexTypeSTLOrdered is an ordered STL container (for testing).
	IndexTypeSTLOrdered

	// IndexTypeSTLUnordered is an unordered STL container (for testing).
	IndexTypeSTLUnordered
)

// TableInfo contains metadata about a table.
type TableInfo struct {
	// Schema describes the columns in the table.
	Schema *Schema

	// Name is the name of the table.
	Name string

	// Table is the table heap storing the actual data.
	// (Will be implemented in storage/table package)
	// Table *TableHeap

	// OID is the unique object identifier for this table.
	OID TableOID
}

// IndexInfo contains metadata about an index.
type IndexInfo struct {
	// KeySchema describes the columns that form the index key.
	KeySchema *Schema

	// Name is the name of the index.
	Name string

	// Index is the actual index implementation.
	Index index.Index

	// IndexOID is the unique object identifier for this index.
	IndexOID IndexOID

	// TableName is the name of the table this index is on.
	TableName string

	// KeySize is the size of the index key in bytes.
	KeySize int

	// IsPrimaryKey indicates if this is a primary key index.
	IsPrimaryKey bool

	// Type is the type of index.
	Type IndexType
}

// Catalog manages database metadata.
//
// The catalog provides:
// - Table creation and lookup
// - Index creation and lookup
// - Thread-safe access to metadata
//
// This is a non-persistent catalog designed for the executor.
// A real database would persist catalog information.
type Catalog struct {
	// bpm is the buffer pool manager.
	bpm *buffer.BufferPoolManager

	// mu protects the catalog data structures.
	mu sync.RWMutex

	// tables maps table OIDs to TableInfo.
	tables map[TableOID]*TableInfo

	// tableNames maps table names to OIDs.
	tableNames map[string]TableOID

	// indexes maps index OIDs to IndexInfo.
	indexes map[IndexOID]*IndexInfo

	// indexNames maps (table_name, index_name) to index OIDs.
	// Outer map is table name, inner map is index name.
	indexNames map[string]map[string]IndexOID

	// nextTableOID is the next table OID to assign.
	nextTableOID atomic.Uint32

	// nextIndexOID is the next index OID to assign.
	nextIndexOID atomic.Uint32
}

// NewCatalog creates a new catalog.
func NewCatalog(bpm *buffer.BufferPoolManager) *Catalog {
	return &Catalog{
		bpm:        bpm,
		tables:     make(map[TableOID]*TableInfo),
		tableNames: make(map[string]TableOID),
		indexes:    make(map[IndexOID]*IndexInfo),
		indexNames: make(map[string]map[string]IndexOID),
	}
}

// =============================================================================
// TABLE OPERATIONS
// =============================================================================

// CreateTable creates a new table and returns its info.
//
// Parameters:
// - tableName: Name of the new table (must be unique)
// - schema: The schema for the table
//
// Returns nil if a table with the same name already exists.
//
// Note: Tables beginning with "__" are reserved for system use.
func (c *Catalog) CreateTable(tableName string, schema *Schema) *TableInfo {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for duplicate
	if _, exists := c.tableNames[tableName]; exists {
		return nil
	}

	// Assign OID
	oid := TableOID(c.nextTableOID.Add(1) - 1)

	// Create table info
	info := &TableInfo{
		Schema: schema,
		Name:   tableName,
		OID:    oid,
	}

	// TODO: Create the actual TableHeap
	// info.Table = NewTableHeap(c.bpm)

	// Register in catalog
	c.tables[oid] = info
	c.tableNames[tableName] = oid
	c.indexNames[tableName] = make(map[string]IndexOID)

	return info
}

// GetTable returns table info by name.
//
// Returns nil if the table doesn't exist.
func (c *Catalog) GetTable(tableName string) *TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	oid, exists := c.tableNames[tableName]
	if !exists {
		return nil
	}
	return c.tables[oid]
}

// GetTableByOID returns table info by OID.
//
// Returns nil if the table doesn't exist.
func (c *Catalog) GetTableByOID(oid TableOID) *TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.tables[oid]
}

// GetAllTables returns all tables in the catalog.
func (c *Catalog) GetAllTables() []*TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*TableInfo, 0, len(c.tables))
	for _, info := range c.tables {
		result = append(result, info)
	}
	return result
}

// =============================================================================
// INDEX OPERATIONS
// =============================================================================

// CreateIndex creates a new index on a table.
//
// Parameters:
// - indexName: Name of the new index
// - tableName: Table to create the index on
// - keySchema: Schema for the index key
// - keyAttrs: Column indices that form the key
// - keySize: Size of the key in bytes
// - indexType: Type of index to create
//
// Returns nil if:
// - The table doesn't exist
// - An index with the same name already exists on the table
func (c *Catalog) CreateIndex(
	indexName string,
	tableName string,
	keySchema *Schema,
	keyAttrs []int,
	keySize int,
	indexType IndexType,
) *IndexInfo {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check table exists
	tableOID, exists := c.tableNames[tableName]
	if !exists {
		return nil
	}
	_ = tableOID // Will use to get table info

	// Check for duplicate index name
	if indexMap, ok := c.indexNames[tableName]; ok {
		if _, exists := indexMap[indexName]; exists {
			return nil
		}
	}

	// Assign OID
	oid := IndexOID(c.nextIndexOID.Add(1) - 1)

	// TODO: Create the actual index
	// var idx index.Index
	// switch indexType {
	// case IndexTypeBPlusTree:
	//     headerPageID := c.bpm.NewPage()
	//     idx = NewBPlusTreeIndex(...)
	// case IndexTypeHashTable:
	//     idx = NewExtendibleHashTableIndex(...)
	// }

	// Create index info
	info := &IndexInfo{
		KeySchema:    keySchema,
		Name:         indexName,
		Index:        nil, // TODO: Set actual index
		IndexOID:     oid,
		TableName:    tableName,
		KeySize:      keySize,
		IsPrimaryKey: false,
		Type:         indexType,
	}

	// Register in catalog
	c.indexes[oid] = info
	c.indexNames[tableName][indexName] = oid

	return info
}

// GetIndex returns index info by name.
//
// Returns nil if the index doesn't exist.
func (c *Catalog) GetIndex(tableName, indexName string) *IndexInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	indexMap, exists := c.indexNames[tableName]
	if !exists {
		return nil
	}
	oid, exists := indexMap[indexName]
	if !exists {
		return nil
	}
	return c.indexes[oid]
}

// GetIndexByOID returns index info by OID.
func (c *Catalog) GetIndexByOID(oid IndexOID) *IndexInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.indexes[oid]
}

// GetTableIndexes returns all indexes on a table.
func (c *Catalog) GetTableIndexes(tableName string) []*IndexInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*IndexInfo, 0)
	indexMap, exists := c.indexNames[tableName]
	if !exists {
		return result
	}

	for _, oid := range indexMap {
		if info, ok := c.indexes[oid]; ok {
			result = append(result, info)
		}
	}
	return result
}

// =============================================================================
// SCHEMA
// =============================================================================

// Schema describes the structure of a table or index key.
type Schema struct {
	// Columns are the columns in the schema.
	Columns []Column

	// columnOffsets caches the byte offset of each column.
	columnOffsets []int

	// tupleLength is the fixed length of a tuple (for fixed-size schemas).
	tupleLength int
}

// NewSchema creates a new schema from columns.
func NewSchema(columns []Column) *Schema {
	s := &Schema{
		Columns:       columns,
		columnOffsets: make([]int, len(columns)),
	}

	// Calculate offsets
	offset := 0
	for i, col := range columns {
		s.columnOffsets[i] = offset
		offset += col.GetStorageSize()
	}
	s.tupleLength = offset

	return s
}

// GetColumnCount returns the number of columns.
func (s *Schema) GetColumnCount() int {
	return len(s.Columns)
}

// GetColumn returns the column at the given index.
func (s *Schema) GetColumn(idx int) *Column {
	if idx < 0 || idx >= len(s.Columns) {
		return nil
	}
	return &s.Columns[idx]
}

// GetColumnIdx returns the index of a column by name.
// Returns -1 if not found.
func (s *Schema) GetColumnIdx(name string) int {
	for i, col := range s.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// GetTupleLength returns the length of a tuple in bytes.
func (s *Schema) GetTupleLength() int {
	return s.tupleLength
}

// GetColumnOffset returns the byte offset of a column.
func (s *Schema) GetColumnOffset(idx int) int {
	if idx < 0 || idx >= len(s.columnOffsets) {
		return -1
	}
	return s.columnOffsets[idx]
}

// =============================================================================
// COLUMN
// =============================================================================

// Column describes a single column in a schema.
type Column struct {
	// Name is the column name.
	Name string

	// Type is the column's data type.
	Type common.TypeID

	// Length is the maximum length for variable-length types.
	// For fixed-length types, this is the type's fixed size.
	Length int

	// Offset is the byte offset within a tuple.
	Offset int
}

// NewColumn creates a new column.
func NewColumn(name string, typeID common.TypeID) Column {
	return Column{
		Name:   name,
		Type:   typeID,
		Length: getDefaultLength(typeID),
	}
}

// NewVarcharColumn creates a new varchar column with specified length.
func NewVarcharColumn(name string, length int) Column {
	return Column{
		Name:   name,
		Type:   common.TypeVarchar,
		Length: length,
	}
}

// GetStorageSize returns the storage size for this column.
func (c *Column) GetStorageSize() int {
	return c.Length
}

// getDefaultLength returns the default storage size for a type.
func getDefaultLength(typeID common.TypeID) int {
	switch typeID {
	case common.TypeBoolean:
		return 1
	case common.TypeTinyInt:
		return 1
	case common.TypeSmallInt:
		return 2
	case common.TypeInteger:
		return 4
	case common.TypeBigInt:
		return 8
	case common.TypeDecimal:
		return 8
	case common.TypeTimestamp:
		return 8
	case common.TypeVarchar:
		return 128 // Default varchar length
	default:
		return 0
	}
}

// TypeID aliases for common package types
type TypeID = int

const (
	TypeInvalid   TypeID = 0
	TypeBoolean   TypeID = 1
	TypeTinyInt   TypeID = 2
	TypeSmallInt  TypeID = 3
	TypeInteger   TypeID = 4
	TypeBigInt    TypeID = 5
	TypeDecimal   TypeID = 6
	TypeVarchar   TypeID = 7
	TypeTimestamp TypeID = 8
)
