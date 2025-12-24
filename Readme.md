# BusTub-Go: A Go Implementation of BusTub

A Go implementation of [BusTub](https://github.com/cmu-db/bustub), a relational database management system developed at Carnegie Mellon University for the [Introduction to Database Systems](https://15445.courses.cs.cmu.edu) (15-445/645) course.

This project mirrors the C++ BusTub architecture but implements it idiomatically in Go, providing an educational tool for understanding database internals with Go's concurrency model.

> **⚠️ IMPORTANT**: This is a **scaffolding-only** project. The core assignment components are **NOT IMPLEMENTED** - they contain detailed TODO comments explaining what needs to be implemented. The goal is to provide a learning framework matching the CMU course structure.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Quick Start](#quick-start)
3. [Project Structure](#project-structure)
4. [Architecture Overview](#architecture-overview)
5. [Development Plan (Spec-Driven)](#development-plan-spec-driven)
6. [Component Specifications](#component-specifications)
7. [Implementation Phases](#implementation-phases)
8. [Go-Specific Design Decisions](#go-specific-design-decisions)
9. [Testing Strategy](#testing-strategy)
10. [Getting Started](#getting-started)

---

## Project Overview

BusTub-Go aims to implement a fully functional relational database management system in Go, following the same educational progression as the CMU 15-445 course. The implementation covers:

- **Buffer Pool Management** - Managing pages in memory with replacement policies
- **Storage Engine** - Disk-based storage with page layouts
- **Index Structures** - B+ Trees and Extendible Hash Tables
- **Query Execution** - Volcano-style tuple-at-a-time iterator model
- **Concurrency Control** - Lock-based and MVCC approaches
- **Recovery** - Write-Ahead Logging (WAL) and crash recovery

---

## Quick Start

```bash
# Navigate to the project
cd bustub_go

# Build
go build ./...

# Run the main binary (shows implementation checklist)
go run ./cmd/bustub

# Run all tests (tests are skipped until you implement the components)
go test ./... -v
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         SQL Layer                                │
│  ┌─────────┐  ┌──────────┐  ┌───────────┐  ┌─────────────────┐  │
│  │ Parser  │→ │  Binder  │→ │  Planner  │→ │    Optimizer    │  │
│  └─────────┘  └──────────┘  └───────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Execution Engine                            │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Executors (SeqScan, IndexScan, Join, Aggregation, etc.)   │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                       Access Methods                             │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────────┐ │
│  │  Table Heap │  │   B+ Tree    │  │  Extendible Hash Table  │ │
│  └─────────────┘  └──────────────┘  └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   Buffer Pool Manager                            │
│  ┌──────────────┐  ┌─────────────────┐  ┌────────────────────┐  │
│  │ Page Guards  │  │  LRU-K/ARC      │  │  Frame Headers     │  │
│  │              │  │  Replacer       │  │                    │  │
│  └──────────────┘  └─────────────────┘  └────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Disk Manager                                │
│  ┌─────────────────────┐  ┌────────────────────────────────────┐│
│  │  Disk Scheduler     │  │  Read/Write Pages to/from Disk     ││
│  └─────────────────────┘  └────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                              ↓
                    ┌─────────────────┐
                    │   Disk/Files    │
                    └─────────────────┘
```

---

## Development Plan (Spec-Driven)

This project follows a **spec-driven development** approach. Each component is specified before implementation, ensuring clear contracts and testable interfaces.

### Phase 1: Foundation (Primer & Buffer Pool)

#### 1.1 Common Types & Utilities
- **Config**: Page size (8KB), frame/page/transaction IDs, constants
- **RID**: Record Identifier (page_id + slot_offset)
- **Errors**: Custom error types for database operations
- **Channel**: Thread-safe channel for Go concurrency

#### 1.2 Disk Manager
- **Spec**: Read/write pages to/from disk, lazy allocation
- **Interface**:
  ```go
  type DiskManager interface {
      ReadPage(pageID PageID, data []byte) error
      WritePage(pageID PageID, data []byte) error
      DeletePage(pageID PageID) error
      Shutdown() error
  }
  ```

#### 1.3 Disk Scheduler
- **Spec**: Background goroutine processes disk I/O requests
- **Interface**:
  ```go
  type DiskScheduler interface {
      Schedule(requests []DiskRequest)
      CreatePromise() chan bool
  }
  ```

#### 1.4 LRU-K Replacer
- **Spec**: Eviction policy based on k-distance backward access time
- **Interface**:
  ```go
  type LRUKReplacer interface {
      Evict() (FrameID, bool)
      RecordAccess(frameID FrameID, accessType AccessType)
      SetEvictable(frameID FrameID, evictable bool)
      Remove(frameID FrameID)
      Size() int
  }
  ```

#### 1.5 Buffer Pool Manager
- **Spec**: Manages frames in memory, page table, eviction
- **Interface**:
  ```go
  type BufferPoolManager interface {
      NewPage() PageID
      DeletePage(pageID PageID) bool
      ReadPage(pageID PageID, accessType AccessType) ReadPageGuard
      WritePage(pageID PageID, accessType AccessType) WritePageGuard
      FlushPage(pageID PageID) bool
      FlushAllPages()
      GetPinCount(pageID PageID) (int, bool)
  }
  ```

#### 1.6 Page Guards (RAII Pattern in Go)
- **Spec**: Automatic pin/unpin and latch management
- **Types**: `ReadPageGuard`, `WritePageGuard`

---

### Phase 2: Storage (Hash Index)

#### 2.1 Page Layouts
- **Extendible Hash Table Header Page**
- **Extendible Hash Table Directory Page**
- **Extendible Hash Table Bucket Page**

#### 2.2 Extendible Hash Table
- **Spec**: Disk-backed, dynamically growing hash table
- **Interface**:
  ```go
  type ExtendibleHashTable[K, V any] interface {
      Insert(key K, value V, txn *Transaction) bool
      Remove(key K, txn *Transaction) bool
      GetValue(key K, result *[]V, txn *Transaction) bool
  }
  ```

---

### Phase 3: Storage (B+ Tree Index)

#### 3.1 B+ Tree Page Layouts
- **B+ Tree Page** (base)
- **B+ Tree Internal Page**
- **B+ Tree Leaf Page**
- **B+ Tree Header Page**

#### 3.2 B+ Tree Index
- **Spec**: Ordered index supporting insert, delete, point/range queries
- **Interface**:
  ```go
  type BPlusTree[K, V any] interface {
      Insert(key K, value V) bool
      Remove(key K)
      GetValue(key K) ([]V, bool)
      Begin() Iterator
      End() Iterator
  }
  ```

#### 3.3 Index Iterator
- **Spec**: Range scan over leaf pages
- **Interface**:
  ```go
  type IndexIterator[K, V any] interface {
      IsEnd() bool
      Key() K
      Value() V
      Next() IndexIterator[K, V]
  }
  ```

---

### Phase 4: Query Execution

#### 4.1 Catalog
- **Spec**: Metadata about tables and indexes
- **Components**: TableInfo, IndexInfo, Schema, Column

#### 4.2 Table Heap
- **Spec**: Unordered collection of tuples stored in pages
- **Interface**:
  ```go
  type TableHeap interface {
      InsertTuple(meta TupleMeta, tuple Tuple, txn *Transaction) (RID, bool)
      UpdateTupleMeta(meta TupleMeta, rid RID)
      GetTuple(rid RID) (TupleMeta, Tuple)
      MakeIterator() TableIterator
  }
  ```

#### 4.3 Tuple & Schema
- **Tuple**: Row of data with values
- **Schema**: Column definitions (name, type, offset)
- **Value**: Typed database value (Integer, Varchar, Boolean, etc.)

#### 4.4 Executor Framework (Volcano Model)
- **Abstract Executor Interface**:
  ```go
  type Executor interface {
      Init()
      Next(batchSize int) ([]Tuple, []RID, bool)
      GetOutputSchema() *Schema
  }
  ```

#### 4.5 Executors to Implement
| Executor | Description |
|----------|-------------|
| SeqScanExecutor | Full table scan |
| IndexScanExecutor | Index-based lookup |
| FilterExecutor | Predicate evaluation |
| ProjectionExecutor | Column projection |
| InsertExecutor | Insert tuples |
| DeleteExecutor | Delete tuples |
| UpdateExecutor | Update tuples |
| NestedLoopJoinExecutor | Nested loop join |
| HashJoinExecutor | Hash-based join |
| AggregationExecutor | GROUP BY + aggregates |
| SortExecutor | In-memory sorting |
| LimitExecutor | LIMIT clause |
| TopNExecutor | ORDER BY + LIMIT optimization |

#### 4.6 Expressions
- **ColumnValueExpression**: Reference to a column
- **ConstantValueExpression**: Constant value
- **ComparisonExpression**: =, <, >, <=, >=, <>
- **ArithmeticExpression**: +, -, *, /
- **LogicExpression**: AND, OR, NOT
- **AggregateExpression**: COUNT, SUM, MIN, MAX, AVG

---

### Phase 5: Concurrency Control

#### 5.1 Lock Manager
- **Spec**: Row-level locking with deadlock detection
- **Lock Modes**: Shared, Exclusive, Intention-Shared, Intention-Exclusive, Shared-Intention-Exclusive
- **Interface**:
  ```go
  type LockManager interface {
      LockTable(txn *Transaction, lockMode LockMode, tableOID TableOID) bool
      UnlockTable(txn *Transaction, tableOID TableOID) bool
      LockRow(txn *Transaction, lockMode LockMode, tableOID TableOID, rid RID) bool
      UnlockRow(txn *Transaction, tableOID TableOID, rid RID) bool
  }
  ```

#### 5.2 Transaction Manager
- **Spec**: Begin, commit, abort transactions
- **MVCC Support**: Undo logs, version chains
- **Interface**:
  ```go
  type TransactionManager interface {
      Begin(isolationLevel IsolationLevel) *Transaction
      Commit(txn *Transaction) bool
      Abort(txn *Transaction)
  }
  ```

#### 5.3 Watermark
- **Spec**: Track minimum read timestamp for garbage collection

---

### Phase 6: Recovery (Optional/Advanced)

#### 6.1 Log Manager
- **Spec**: Write-ahead logging
- **Log Records**: Begin, Commit, Abort, Insert, Delete, Update, NewPage

#### 6.2 Log Recovery
- **ARIES Algorithm**: Analysis, Redo, Undo phases
- **Checkpointing**: Periodic snapshots

---

## Project Structure

```
bustub_go/
├── cmd/
│   └── bustub/              # Main entry point & shell
├── pkg/
│   ├── common/              # Common types, config, utilities
│   │   ├── config.go        # Constants (PAGE_SIZE, etc.)
│   │   ├── rid.go           # Record ID type
│   │   ├── channel.go       # Thread-safe channel
│   │   └── errors.go        # Custom error types
│   │
│   ├── buffer/              # Buffer Pool Management
│   │   ├── replacer.go      # Replacer interface
│   │   ├── lru_k_replacer.go
│   │   ├── arc_replacer.go
│   │   ├── buffer_pool_manager.go
│   │   ├── frame_header.go
│   │   └── page_guard.go    # ReadPageGuard, WritePageGuard
│   │
│   ├── storage/
│   │   ├── disk/            # Disk I/O
│   │   │   ├── disk_manager.go
│   │   │   └── disk_scheduler.go
│   │   │
│   │   ├── page/            # Page layouts
│   │   │   ├── page.go
│   │   │   ├── table_page.go
│   │   │   ├── b_plus_tree_page.go
│   │   │   ├── b_plus_tree_internal_page.go
│   │   │   ├── b_plus_tree_leaf_page.go
│   │   │   ├── b_plus_tree_header_page.go
│   │   │   ├── extendible_htable_header_page.go
│   │   │   ├── extendible_htable_directory_page.go
│   │   │   └── extendible_htable_bucket_page.go
│   │   │
│   │   ├── index/           # Index structures
│   │   │   ├── index.go     # Index interface
│   │   │   ├── b_plus_tree.go
│   │   │   ├── b_plus_tree_index.go
│   │   │   ├── index_iterator.go
│   │   │   └── hash_table_index.go
│   │   │
│   │   └── table/           # Table storage
│   │       ├── table_heap.go
│   │       ├── tuple.go
│   │       └── table_iterator.go
│   │
│   ├── container/           # Data structures
│   │   └── hash/
│   │       ├── extendible_hash_table.go
│   │       └── hash_function.go
│   │
│   ├── catalog/             # Database catalog
│   │   ├── catalog.go
│   │   ├── schema.go
│   │   └── column.go
│   │
│   ├── type/                # Type system
│   │   ├── type_id.go
│   │   ├── value.go
│   │   └── types.go         # Integer, Varchar, Boolean, etc.
│   │
│   ├── execution/           # Query execution
│   │   ├── executor_context.go
│   │   ├── executor_factory.go
│   │   ├── executors/
│   │   │   ├── abstract_executor.go
│   │   │   ├── seq_scan_executor.go
│   │   │   ├── index_scan_executor.go
│   │   │   ├── filter_executor.go
│   │   │   ├── projection_executor.go
│   │   │   ├── insert_executor.go
│   │   │   ├── delete_executor.go
│   │   │   ├── update_executor.go
│   │   │   ├── nested_loop_join_executor.go
│   │   │   ├── hash_join_executor.go
│   │   │   ├── aggregation_executor.go
│   │   │   ├── sort_executor.go
│   │   │   ├── limit_executor.go
│   │   │   └── topn_executor.go
│   │   │
│   │   ├── expressions/
│   │   │   ├── abstract_expression.go
│   │   │   ├── column_value_expression.go
│   │   │   ├── constant_value_expression.go
│   │   │   ├── comparison_expression.go
│   │   │   ├── arithmetic_expression.go
│   │   │   ├── logic_expression.go
│   │   │   └── aggregate_expression.go
│   │   │
│   │   └── plans/
│   │       ├── abstract_plan.go
│   │       ├── seq_scan_plan.go
│   │       ├── index_scan_plan.go
│   │       └── ... (one per executor)
│   │
│   ├── concurrency/         # Concurrency control
│   │   ├── lock_manager.go
│   │   ├── transaction.go
│   │   ├── transaction_manager.go
│   │   └── watermark.go
│   │
│   ├── recovery/            # Crash recovery
│   │   ├── log_manager.go
│   │   ├── log_record.go
│   │   └── log_recovery.go
│   │
│   ├── binder/              # SQL binding
│   │   └── binder.go
│   │
│   ├── planner/             # Query planning
│   │   └── planner.go
│   │
│   └── optimizer/           # Query optimization
│       └── optimizer.go
│
├── test/                    # Unit and integration tests
│   ├── buffer/
│   ├── storage/
│   ├── container/
│   ├── execution/
│   └── concurrency/
│
├── tools/                   # Utility tools
│   ├── shell/               # Interactive SQL shell
│   └── btree_printer/       # B+ tree visualization
│
├── go.mod
├── go.sum
├── Makefile
└── Readme.md
```

---

## Component Specifications

### Common Types (pkg/common/config.go)

```go
// PageSize is the size of a database page in bytes (8KB)
const PageSize = 8192

// BufferPoolSize is the default number of frames in the buffer pool
const BufferPoolSize = 128

// InvalidPageID represents an invalid page identifier
const InvalidPageID PageID = -1

// InvalidFrameID represents an invalid frame identifier  
const InvalidFrameID FrameID = -1

// InvalidTxnID represents an invalid transaction identifier
const InvalidTxnID TxnID = -1

// Type aliases for clarity
type PageID = int32
type FrameID = int32
type TxnID = int64
type LSN = int32
type SlotOffset = uint16
type TableOID = uint32
type IndexOID = uint32
```

### RID - Record Identifier (pkg/common/rid.go)

```go
// RID uniquely identifies a tuple within the database
// Composed of: PageID (which page) + SlotOffset (position within page)
type RID struct {
    PageID     PageID
    SlotOffset SlotOffset
}
```

---

## Implementation Phases

### Phase 1: Buffer Pool Manager (Assignment 1 Equivalent)

**Goal**: Implement memory management layer between disk and higher layers.

**Components to implement**:
1. `LRUKReplacer` - Track page access history, select eviction victims
2. `DiskScheduler` - Background disk I/O processing
3. `BufferPoolManager` - Central memory management
4. `PageGuard` - Safe page access with automatic cleanup

**Key Concepts**:
- Page pinning/unpinning
- Dirty page tracking
- Eviction policies (LRU-K backward k-distance)
- Thread-safe operations using Go's sync primitives

---

### Phase 2: Extendible Hash Table (Assignment 2 Equivalent)

**Goal**: Implement disk-based hash index that grows dynamically.

**Components to implement**:
1. `ExtendibleHTableHeaderPage` - Maps hash prefixes to directories
2. `ExtendibleHTableDirectoryPage` - Maps hash suffixes to buckets
3. `ExtendibleHTableBucketPage` - Stores actual key-value pairs
4. `DiskExtendibleHashTable` - Main hash table logic

**Key Concepts**:
- Global and local depth
- Bucket splitting
- Directory doubling
- Hash function application

---

### Phase 3: B+ Tree Index (Assignment 2 Equivalent)

**Goal**: Implement ordered index with efficient point and range queries.

**Components to implement**:
1. `BPlusTreePage` - Common page header
2. `BPlusTreeInternalPage` - Internal node with keys and child pointers
3. `BPlusTreeLeafPage` - Leaf node with keys and values
4. `BPlusTree` - Main B+ tree operations
5. `IndexIterator` - Range scan support

**Key Concepts**:
- Page splits and merges
- Redistribution
- Tree balancing
- Concurrent access (optional crabbing protocol)

---

### Phase 4: Query Execution (Assignment 3 Equivalent)

**Goal**: Implement Volcano-style query execution engine.

**Components to implement**:
1. Access executors (SeqScan, IndexScan)
2. Modification executors (Insert, Delete, Update)
3. Join executors (NestedLoopJoin, HashJoin)
4. Aggregation executor
5. Sort and TopN executors

**Key Concepts**:
- Iterator model (Init → Next → Close)
- Expression evaluation
- Schema propagation
- Tuple batching for efficiency

---

### Phase 5: Concurrency Control (Assignment 4 Equivalent)

**Goal**: Implement transaction management with MVCC or locking.

**Components to implement**:
1. `Transaction` - Transaction state and undo logs
2. `LockManager` - Lock acquisition and deadlock detection
3. `TransactionManager` - Begin, commit, abort
4. `Watermark` - Garbage collection support

**Key Concepts**:
- Isolation levels (Read Committed, Repeatable Read, Snapshot Isolation)
- Two-Phase Locking (2PL)
- MVCC with undo logs
- Deadlock detection (wait-for graph)

---

## Go-Specific Design Decisions

### 1. Interfaces over Inheritance
Go uses composition and interfaces instead of C++ inheritance. Each component defines a clear interface.

### 2. Error Handling
Use Go's explicit error returns instead of C++ exceptions:
```go
func (bpm *BufferPoolManager) NewPage() (PageID, error)
```

### 3. Concurrency with Goroutines
- Use `sync.Mutex` and `sync.RWMutex` for locks
- Use channels for inter-goroutine communication
- Use `context.Context` for cancellation

### 4. No RAII - Use defer
Page guards use `defer` for cleanup:
```go
guard := bpm.ReadPage(pageID)
defer guard.Drop()
```

### 5. Generics (Go 1.18+)
Use generics for B+ tree and hash table implementations:
```go
type BPlusTree[K Comparable, V any] struct { ... }
```

### 6. No Templates - Use Code Generation or Generics
Replace C++ templates with Go generics or interface{} with type assertions.

---

## Testing Strategy

### Unit Tests
Each package has a corresponding `_test.go` file:
```
pkg/buffer/buffer_pool_manager_test.go
pkg/buffer/lru_k_replacer_test.go
pkg/storage/index/b_plus_tree_test.go
```

### Integration Tests
```
test/integration/sql_test.go
test/integration/concurrent_test.go
```

### Benchmarks
```go
func BenchmarkBufferPoolManager(b *testing.B) { ... }
func BenchmarkBPlusTreeInsert(b *testing.B) { ... }
```

### Test Commands
```bash
# Run all tests
go test ./...

# Run tests with race detector
go test -race ./...

# Run specific package tests
go test ./pkg/buffer/...

# Run benchmarks
go test -bench=. ./pkg/buffer/...
```

---

## Getting Started

### Prerequisites
- Go 1.21 or later
- Make (optional, for Makefile targets)

### Building
```bash
cd bustub_go
go mod tidy
go build ./...
```

### Running Tests
```bash
go test ./...
```

### Running the Shell (once implemented)
```bash
go run cmd/bustub/main.go
```

---

## License

This project is for educational purposes, inspired by CMU's BusTub project.

---

## References

- [CMU 15-445/645 Database Systems](https://15445.courses.cs.cmu.edu)
- [Original BusTub Repository](https://github.com/cmu-db/bustub)
- [Database System Concepts (Silberschatz)](https://www.db-book.com/)
- [Architecture of a Database System (Hellerstein)](https://dsf.berkeley.edu/papers/fntdb07-architecture.pdf)
