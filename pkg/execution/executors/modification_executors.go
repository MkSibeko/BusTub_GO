// Package executors provides executor implementations.
//
// This file contains data modification executors (INSERT, UPDATE, DELETE).
package executors

import (
	"github.com/bustub-go/pkg/catalog"
	"github.com/bustub-go/pkg/common"
	"github.com/bustub-go/pkg/execution"
	"github.com/bustub-go/pkg/storage/table"
)

// =============================================================================
// INSERT EXECUTOR
// =============================================================================

// InsertPlan describes an insert operation.
type InsertPlan struct {
	OutputSchema *catalog.Schema
	Child        execution.Plan
	TableOID     catalog.TableOID
}

func (p *InsertPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *InsertPlan) GetChildren() []execution.Plan    { return []execution.Plan{p.Child} }
func (p *InsertPlan) GetType() execution.PlanType      { return execution.PlanTypeInsert }

// InsertExecutor inserts tuples into a table.
//
// IMPLEMENTATION NOTES:
// =====================
// The insert executor:
// 1. Pulls tuples from its child executor (VALUES or SELECT)
// 2. Inserts each tuple into the table heap
// 3. Updates ALL indexes on the table
// 4. Returns the number of rows inserted (as a single tuple)
//
// CONCURRENCY NOTES:
// - For MVCC, set the tuple's timestamp to the transaction's read_ts
// - Acquire exclusive locks if using locking-based concurrency
type InsertExecutor struct {
	execution.AbstractExecutor
	plan      *InsertPlan
	childExec execution.Executor
	tableInfo *catalog.TableInfo
	indexes   []*catalog.IndexInfo

	// hasInserted tracks if we've done the insert.
	// Insert returns only one result tuple (the count).
	hasInserted bool
}

func NewInsertExecutor(ctx *execution.ExecutorContext, plan *InsertPlan, child execution.Executor) *InsertExecutor {
	return &InsertExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		childExec:        child,
	}
}

// Init initializes the insert executor.
//
// TODO: Implement this
//
// STEPS:
// 1. Look up table info from catalog
// 2. Get all indexes on the table
// 3. Initialize child executor
func (e *InsertExecutor) Init() {
	panic("InsertExecutor.Init not implemented")
}

// Next performs the insert and returns the count.
//
// TODO: Implement this
//
// STEPS:
//  1. If already inserted, return (nil, false)
//  2. Set hasInserted = true
//  3. Loop: get tuples from child executor
//  4. For each tuple:
//     a. Insert into table heap (get RID)
//     b. For each index, insert the key->RID mapping
//  5. Create a result tuple containing the count
//  6. Return the result tuple
//
// IMPORTANT: You MUST update indexes or queries won't find data!
func (e *InsertExecutor) Next() (*table.Tuple, bool) {
	panic("InsertExecutor.Next not implemented")
}

// =============================================================================
// UPDATE EXECUTOR
// =============================================================================

// UpdatePlan describes an update operation.
type UpdatePlan struct {
	OutputSchema *catalog.Schema
	Child        execution.Plan
	TableOID     catalog.TableOID
	TargetExprs  []execution.Expression // New values for each column
}

func (p *UpdatePlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *UpdatePlan) GetChildren() []execution.Plan    { return []execution.Plan{p.Child} }
func (p *UpdatePlan) GetType() execution.PlanType      { return execution.PlanTypeUpdate }

// UpdateExecutor updates tuples in a table.
//
// IMPLEMENTATION NOTES:
// =====================
// BusTub uses a "delete-and-insert" approach for updates:
// 1. Mark the old tuple as deleted
// 2. Insert the new tuple
// 3. Update indexes (remove old, add new)
//
// This is simpler than in-place updates and works well with MVCC.
//
// MVCC NOTES:
// - The old tuple version remains visible to older transactions
// - The new tuple has the current transaction's timestamp
type UpdateExecutor struct {
	execution.AbstractExecutor
	plan       *UpdatePlan
	childExec  execution.Executor
	tableInfo  *catalog.TableInfo
	indexes    []*catalog.IndexInfo
	hasUpdated bool
}

func NewUpdateExecutor(ctx *execution.ExecutorContext, plan *UpdatePlan, child execution.Executor) *UpdateExecutor {
	return &UpdateExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		childExec:        child,
	}
}

// Init initializes the update executor.
//
// TODO: Implement this
func (e *UpdateExecutor) Init() {
	panic("UpdateExecutor.Init not implemented")
}

// Next performs the update and returns the count.
//
// TODO: Implement this
//
// STEPS:
//  1. Get tuples from child (these are tuples to update)
//  2. For each tuple:
//     a. Extract the RID
//     b. Evaluate TargetExprs to get new values
//     c. Build new tuple
//     d. Delete old tuple (mark as deleted)
//     e. Insert new tuple
//     f. Update indexes (delete old key, insert new key)
//  3. Return count of updated rows
func (e *UpdateExecutor) Next() (*table.Tuple, bool) {
	panic("UpdateExecutor.Next not implemented")
}

// =============================================================================
// DELETE EXECUTOR
// =============================================================================

// DeletePlan describes a delete operation.
type DeletePlan struct {
	OutputSchema *catalog.Schema
	Child        execution.Plan
	TableOID     catalog.TableOID
}

func (p *DeletePlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *DeletePlan) GetChildren() []execution.Plan    { return []execution.Plan{p.Child} }
func (p *DeletePlan) GetType() execution.PlanType      { return execution.PlanTypeDelete }

// DeleteExecutor deletes tuples from a table.
//
// IMPLEMENTATION NOTES:
// =====================
// For MVCC, "delete" means marking the tuple as deleted, not
// actually removing it. The tuple remains visible to older
// transactions that started before the delete.
//
// STEPS:
// 1. Get tuples to delete from child
// 2. Mark each tuple as deleted (update metadata)
// 3. Delete from all indexes
// 4. Return count of deleted rows
type DeleteExecutor struct {
	execution.AbstractExecutor
	plan       *DeletePlan
	childExec  execution.Executor
	tableInfo  *catalog.TableInfo
	indexes    []*catalog.IndexInfo
	hasDeleted bool
}

func NewDeleteExecutor(ctx *execution.ExecutorContext, plan *DeletePlan, child execution.Executor) *DeleteExecutor {
	return &DeleteExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		childExec:        child,
	}
}

// Init initializes the delete executor.
//
// TODO: Implement this
func (e *DeleteExecutor) Init() {
	panic("DeleteExecutor.Init not implemented")
}

// Next performs the delete and returns the count.
//
// TODO: Implement this
//
// STEPS:
//  1. Get tuples from child (with RIDs)
//  2. For each tuple:
//     a. Get the RID
//     b. Mark tuple as deleted (update TupleMeta)
//     c. Delete from all indexes
//  3. Return count of deleted rows
func (e *DeleteExecutor) Next() (*table.Tuple, bool) {
	panic("DeleteExecutor.Next not implemented")
}

// =============================================================================
// VALUES EXECUTOR
// =============================================================================

// ValuesPlan describes a VALUES clause.
type ValuesPlan struct {
	OutputSchema *catalog.Schema
	Values       [][]execution.Expression // Rows of values
}

func (p *ValuesPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *ValuesPlan) GetChildren() []execution.Plan    { return nil }
func (p *ValuesPlan) GetType() execution.PlanType      { return execution.PlanTypeValues }

// ValuesExecutor produces tuples from constant values.
//
// This is used for INSERT ... VALUES statements.
type ValuesExecutor struct {
	execution.AbstractExecutor
	plan   *ValuesPlan
	cursor int
}

func NewValuesExecutor(ctx *execution.ExecutorContext, plan *ValuesPlan) *ValuesExecutor {
	return &ValuesExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		cursor:           0,
	}
}

// Init initializes the values executor.
func (e *ValuesExecutor) Init() {
	e.cursor = 0
}

// Next returns the next row of values.
//
// TODO: Implement this
//
// STEPS:
// 1. If cursor >= len(Values), return (nil, false)
// 2. Get Values[cursor]
// 3. Evaluate each expression (they're usually constants)
// 4. Build a tuple from the values
// 5. Increment cursor
// 6. Return the tuple
func (e *ValuesExecutor) Next() (*table.Tuple, bool) {
	panic("ValuesExecutor.Next not implemented")
}

// =============================================================================
// HELPER TYPES
// =============================================================================

// RID is imported from common for use in executors.
type RID = common.RID
