// Package executors provides executor implementations.
//
// ASSIGNMENT #3 - Query Execution
// ================================
// This package contains all the executor implementations for the
// Volcano-style query execution engine.
//
// Executor Categories:
// --------------------
// 1. Access Methods (read from storage)
//   - SeqScanExecutor: Full table scan
//   - IndexScanExecutor: Use an index for lookup
//
// 2. Modification (change data)
//   - InsertExecutor: Insert new tuples
//   - UpdateExecutor: Update existing tuples
//   - DeleteExecutor: Delete tuples
//
// 3. Operators (transform data)
//   - FilterExecutor: Apply predicates (WHERE)
//   - ProjectionExecutor: Select columns (SELECT list)
//
// 4. Join (combine tables)
//   - NestedLoopJoinExecutor: Simple O(n*m) join
//   - HashJoinExecutor: Build hash table, probe
//
// 5. Aggregation (GROUP BY, aggregate functions)
//   - AggregationExecutor: SUM, COUNT, AVG, MIN, MAX
//
// 6. Sorting (ORDER BY)
//   - SortExecutor: Sort all tuples
//   - LimitExecutor: Return first N tuples
//   - TopNExecutor: Optimized ORDER BY ... LIMIT N
//
// This mirrors: bustub/src/include/execution/executors/
package executors

import (
	"github.com/bustub-go/pkg/catalog"
	"github.com/bustub-go/pkg/execution"
	"github.com/bustub-go/pkg/storage/table"
)

// =============================================================================
// SEQ SCAN EXECUTOR
// =============================================================================

// SeqScanPlan describes a sequential scan over a table.
type SeqScanPlan struct {
	// OutputSchema is the schema of output tuples.
	OutputSchema *catalog.Schema

	// TableOID identifies the table to scan.
	TableOID catalog.TableOID

	// FilterPredicate is an optional filter (WHERE clause).
	// If nil, all tuples are returned.
	FilterPredicate execution.Expression
}

// GetOutputSchema returns the output schema.
func (p *SeqScanPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }

// GetChildren returns child plans (none for SeqScan).
func (p *SeqScanPlan) GetChildren() []execution.Plan { return nil }

// GetType returns the plan type.
func (p *SeqScanPlan) GetType() execution.PlanType { return execution.PlanTypeSeqScan }

// SeqScanExecutor performs a sequential scan over a table.
//
// IMPLEMENTATION NOTES:
// =====================
// The sequential scan reads ALL tuples from a table in order.
// This is the simplest access method.
//
// For each tuple:
// 1. Check if it's visible (MVCC - not deleted, in timestamp range)
// 2. Apply the filter predicate if present
// 3. Return visible and matching tuples
type SeqScanExecutor struct {
	execution.AbstractExecutor

	// plan is the seq scan plan.
	plan *SeqScanPlan

	// tableInfo has table metadata.
	tableInfo *catalog.TableInfo

	// iterator iterates over the table heap.
	iterator *table.TableIterator
}

// NewSeqScanExecutor creates a new sequential scan executor.
func NewSeqScanExecutor(ctx *execution.ExecutorContext, plan *SeqScanPlan) *SeqScanExecutor {
	return &SeqScanExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
	}
}

// Init initializes the executor.
//
// TODO: Implement this
//
// STEPS:
// 1. Look up the table in the catalog using TableOID
// 2. Get the TableHeap from TableInfo
// 3. Create an iterator over the table heap
func (e *SeqScanExecutor) Init() {
	panic("SeqScanExecutor.Init not implemented")
}

// Next returns the next tuple.
//
// TODO: Implement this
//
// STEPS:
// 1. Loop through the iterator
// 2. For each tuple, check visibility (MVCC):
//   - Is the tuple deleted? Skip if so.
//   - Is it visible to our transaction? (check timestamps)
//
// 3. Apply filter predicate if present
// 4. If tuple passes, return it
// 5. When iterator ends, return (nil, false)
func (e *SeqScanExecutor) Next() (tuple *table.Tuple, ok bool) {
	panic("SeqScanExecutor.Next not implemented")
}

// =============================================================================
// INDEX SCAN EXECUTOR
// =============================================================================

// IndexScanPlan describes an index-based scan.
type IndexScanPlan struct {
	OutputSchema *catalog.Schema
	TableOID     catalog.TableOID
	IndexOID     catalog.IndexOID

	// FilterPredicate is an optional additional filter.
	FilterPredicate execution.Expression
}

func (p *IndexScanPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *IndexScanPlan) GetChildren() []execution.Plan    { return nil }
func (p *IndexScanPlan) GetType() execution.PlanType      { return execution.PlanTypeIndexScan }

// IndexScanExecutor uses an index to find tuples.
//
// IMPLEMENTATION NOTES:
// =====================
// Index scan uses an index iterator to find matching RIDs,
// then fetches the actual tuples from the table heap.
//
// This is more efficient than seq scan when the index is selective.
type IndexScanExecutor struct {
	execution.AbstractExecutor
	plan      *IndexScanPlan
	tableInfo *catalog.TableInfo
	indexInfo *catalog.IndexInfo
	// indexIterator *index.IndexIterator
}

func NewIndexScanExecutor(ctx *execution.ExecutorContext, plan *IndexScanPlan) *IndexScanExecutor {
	return &IndexScanExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
	}
}

// Init initializes the index scan.
//
// TODO: Implement this
//
// STEPS:
// 1. Look up table and index in catalog
// 2. Get an iterator from the index
func (e *IndexScanExecutor) Init() {
	panic("IndexScanExecutor.Init not implemented")
}

// Next returns the next matching tuple.
//
// TODO: Implement this
//
// STEPS:
// 1. Get next RID from index iterator
// 2. Fetch tuple from table heap using RID
// 3. Check visibility (MVCC)
// 4. Apply additional filter if present
// 5. Return matching tuples
func (e *IndexScanExecutor) Next() (*table.Tuple, bool) {
	panic("IndexScanExecutor.Next not implemented")
}

// =============================================================================
// FILTER EXECUTOR
// =============================================================================

// FilterPlan describes a filter operation.
type FilterPlan struct {
	OutputSchema *catalog.Schema
	Predicate    execution.Expression
	Child        execution.Plan
}

func (p *FilterPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *FilterPlan) GetChildren() []execution.Plan    { return []execution.Plan{p.Child} }
func (p *FilterPlan) GetType() execution.PlanType      { return execution.PlanTypeFilter }

// FilterExecutor filters tuples based on a predicate.
//
// IMPLEMENTATION NOTES:
// =====================
// This is a simple pipeline operator that passes through
// tuples that satisfy the predicate.
type FilterExecutor struct {
	execution.AbstractExecutor
	plan        *FilterPlan
	childExec   execution.Executor
	childSchema *catalog.Schema
}

func NewFilterExecutor(ctx *execution.ExecutorContext, plan *FilterPlan, child execution.Executor) *FilterExecutor {
	return &FilterExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		childExec:        child,
	}
}

// Init initializes the filter.
//
// TODO: Implement this - call Init on child executor
func (e *FilterExecutor) Init() {
	panic("FilterExecutor.Init not implemented")
}

// Next returns the next tuple that passes the filter.
//
// TODO: Implement this
//
// STEPS:
// 1. Get tuple from child
// 2. Evaluate predicate on tuple
// 3. If true, return tuple
// 4. If false, get next tuple (loop)
// 5. When child returns false, return (nil, false)
func (e *FilterExecutor) Next() (*table.Tuple, bool) {
	panic("FilterExecutor.Next not implemented")
}

// =============================================================================
// PROJECTION EXECUTOR
// =============================================================================

// ProjectionPlan describes a projection operation.
type ProjectionPlan struct {
	OutputSchema *catalog.Schema
	Expressions  []execution.Expression
	Child        execution.Plan
}

func (p *ProjectionPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *ProjectionPlan) GetChildren() []execution.Plan    { return []execution.Plan{p.Child} }
func (p *ProjectionPlan) GetType() execution.PlanType      { return execution.PlanTypeProjection }

// ProjectionExecutor evaluates expressions and produces new tuples.
//
// IMPLEMENTATION NOTES:
// =====================
// For each input tuple, evaluate all expressions and create
// a new tuple with the results.
type ProjectionExecutor struct {
	execution.AbstractExecutor
	plan      *ProjectionPlan
	childExec execution.Executor
}

func NewProjectionExecutor(ctx *execution.ExecutorContext, plan *ProjectionPlan, child execution.Executor) *ProjectionExecutor {
	return &ProjectionExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		childExec:        child,
	}
}

// Init initializes the projection.
func (e *ProjectionExecutor) Init() {
	panic("ProjectionExecutor.Init not implemented")
}

// Next projects the next tuple.
//
// TODO: Implement this
//
// STEPS:
// 1. Get tuple from child
// 2. Evaluate each expression in plan.Expressions
// 3. Build a new tuple with the evaluated values
// 4. Return the new tuple
func (e *ProjectionExecutor) Next() (*table.Tuple, bool) {
	panic("ProjectionExecutor.Next not implemented")
}
