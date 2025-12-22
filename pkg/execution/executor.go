// Package execution provides the query execution engine.
//
// BusTub uses a Volcano-style (iterator) execution model:
// - Each operator is an "executor" that produces tuples one at a time
// - Executors form a tree (execution plan)
// - The root executor pulls tuples from children recursively
//
// ASSIGNMENT NOTES (Assignment #3):
// ==================================
// You will implement various executors:
// 1. Access Methods: SeqScan, IndexScan
// 2. Modification: Insert, Update, Delete
// 3. Join: NestedLoopJoin, HashJoin
// 4. Aggregation: Aggregation (with GROUP BY)
// 5. Sorting: Sort, TopN, Limit
//
// Key concepts:
// - Init(): Called once before execution starts
// - Next(): Called repeatedly to get each tuple
// - Tuples flow UP the tree from leaves to root
//
// This mirrors: bustub/src/include/execution/
package execution

import (
	"github.com/bustub-go/pkg/buffer"
	"github.com/bustub-go/pkg/catalog"
	"github.com/bustub-go/pkg/common"
	"github.com/bustub-go/pkg/storage/table"
)

// =============================================================================
// EXECUTOR CONTEXT
// =============================================================================

// ExecutorContext stores context information for query execution.
//
// This is passed to all executors and provides access to:
// - Buffer pool for page access
// - Catalog for table/index metadata
// - Transaction for MVCC operations
// - Lock manager for concurrency control
type ExecutorContext struct {
	// Catalog provides access to table and index metadata.
	catalog *catalog.Catalog

	// BufferPoolManager for page access.
	bpm *buffer.BufferPoolManager

	// Transaction context (for MVCC).
	// txn *Transaction // Will be defined in concurrency package

	// LockManager for concurrency control.
	// lockManager *LockManager

	// IsDelete indicates if this is part of a delete operation.
	// Used by IndexScan to return RIDs instead of tuples.
	IsDelete bool
}

// NewExecutorContext creates a new executor context.
func NewExecutorContext(
	catalog *catalog.Catalog,
	bpm *buffer.BufferPoolManager,
) *ExecutorContext {
	return &ExecutorContext{
		catalog: catalog,
		bpm:     bpm,
	}
}

// GetCatalog returns the catalog.
func (ec *ExecutorContext) GetCatalog() *catalog.Catalog {
	return ec.catalog
}

// GetBufferPoolManager returns the buffer pool manager.
func (ec *ExecutorContext) GetBufferPoolManager() *buffer.BufferPoolManager {
	return ec.bpm
}

// GetTransaction returns the current transaction.
// func (ec *ExecutorContext) GetTransaction() *Transaction {
// 	return ec.txn
// }

// =============================================================================
// ABSTRACT EXECUTOR
// =============================================================================

// Executor is the interface all executors must implement.
//
// The Volcano model uses a pull-based approach:
// 1. Init() is called once to prepare the executor
// 2. Next() is called repeatedly to get tuples one at a time
// 3. Next() returns (nil, false) when there are no more tuples
type Executor interface {
	// Init initializes the executor.
	//
	// This is called once before any calls to Next().
	// Use this to:
	// - Initialize any state needed for execution
	// - Call Init() on child executors
	// - Open iterators on tables/indexes
	Init()

	// Next produces the next tuple.
	//
	// Returns:
	// - tuple: The next output tuple (nil if no more tuples)
	// - ok: true if a tuple was produced, false if done
	//
	// Each call advances the executor's state.
	Next() (tuple *table.Tuple, ok bool)

	// GetOutputSchema returns the schema of output tuples.
	GetOutputSchema() *catalog.Schema
}

// AbstractExecutor provides common functionality for executors.
//
// All executor implementations should embed this struct.
type AbstractExecutor struct {
	// execCtx is the execution context.
	execCtx *ExecutorContext

	// plan is the plan node for this executor.
	plan Plan
}

// NewAbstractExecutor creates a new abstract executor.
func NewAbstractExecutor(execCtx *ExecutorContext, plan Plan) AbstractExecutor {
	return AbstractExecutor{
		execCtx: execCtx,
		plan:    plan,
	}
}

// GetExecutorContext returns the executor context.
func (e *AbstractExecutor) GetExecutorContext() *ExecutorContext {
	return e.execCtx
}

// GetOutputSchema returns the output schema from the plan.
func (e *AbstractExecutor) GetOutputSchema() *catalog.Schema {
	return e.plan.GetOutputSchema()
}

// =============================================================================
// PLAN INTERFACE
// =============================================================================

// Plan is the interface for all plan nodes.
//
// Plan nodes describe WHAT to execute.
// Executors describe HOW to execute it.
type Plan interface {
	// GetOutputSchema returns the schema of output tuples.
	GetOutputSchema() *catalog.Schema

	// GetChildren returns child plan nodes.
	GetChildren() []Plan

	// GetType returns the type of plan node.
	GetType() PlanType
}

// PlanType identifies the type of plan node.
type PlanType int

const (
	PlanTypeSeqScan PlanType = iota
	PlanTypeIndexScan
	PlanTypeInsert
	PlanTypeUpdate
	PlanTypeDelete
	PlanTypeFilter
	PlanTypeProjection
	PlanTypeNestedLoopJoin
	PlanTypeHashJoin
	PlanTypeAggregation
	PlanTypeSort
	PlanTypeLimit
	PlanTypeTopN
	PlanTypeValues
	PlanTypeInitCheck
)

// =============================================================================
// EXPRESSION INTERFACE
// =============================================================================

// Expression represents an expression that can be evaluated.
//
// Expressions are used for:
// - Filter predicates (WHERE clause)
// - Projections (SELECT list)
// - Join conditions
// - Aggregation functions
type Expression interface {
	// Evaluate evaluates the expression on the given tuple.
	//
	// Parameters:
	// - tuple: The tuple to evaluate against
	// - schema: The schema of the tuple
	//
	// Returns the result of the expression.
	Evaluate(tuple *table.Tuple, schema *catalog.Schema) Value

	// EvaluateJoin evaluates for a join (two input tuples).
	EvaluateJoin(
		leftTuple *table.Tuple, leftSchema *catalog.Schema,
		rightTuple *table.Tuple, rightSchema *catalog.Schema,
	) Value

	// GetReturnType returns the type of the expression result.
	GetReturnType() common.TypeID

	// GetChildren returns child expressions.
	GetChildren() []Expression
}

// Value represents an expression result.
// This is a placeholder - use types.Value in actual implementation.
type Value interface{}

// =============================================================================
// EXECUTOR FACTORY
// =============================================================================

// ExecutorFactory creates executor instances from plan nodes.
//
// This is used by the execution engine to build the executor tree.
type ExecutorFactory struct {
	execCtx *ExecutorContext
}

// NewExecutorFactory creates a new executor factory.
func NewExecutorFactory(execCtx *ExecutorContext) *ExecutorFactory {
	return &ExecutorFactory{execCtx: execCtx}
}

// CreateExecutor creates an executor for a plan node.
//
// This recursively creates executors for child plans.
func (f *ExecutorFactory) CreateExecutor(plan Plan) Executor {
	// TODO: Implement this
	//
	// STEPS:
	// 1. Switch on plan.GetType()
	// 2. For each type, create the appropriate executor
	// 3. For plans with children, recursively create child executors
	// 4. Return the executor
	//
	// Example:
	// switch plan.GetType() {
	// case PlanTypeSeqScan:
	//     return NewSeqScanExecutor(f.execCtx, plan.(*SeqScanPlan))
	// case PlanTypeFilter:
	//     child := f.CreateExecutor(plan.GetChildren()[0])
	//     return NewFilterExecutor(f.execCtx, plan.(*FilterPlan), child)
	// ...
	// }

	panic("CreateExecutor not implemented")
}

// =============================================================================
// EXECUTION ENGINE
// =============================================================================

// ExecutionEngine executes query plans.
type ExecutionEngine struct {
	catalog *catalog.Catalog
	bpm     *buffer.BufferPoolManager
}

// NewExecutionEngine creates a new execution engine.
func NewExecutionEngine(catalog *catalog.Catalog, bpm *buffer.BufferPoolManager) *ExecutionEngine {
	return &ExecutionEngine{
		catalog: catalog,
		bpm:     bpm,
	}
}

// Execute runs a query plan and returns all result tuples.
//
// This is the main entry point for query execution.
func (e *ExecutionEngine) Execute(plan Plan) ([]*table.Tuple, error) {
	// TODO: Implement this
	//
	// STEPS:
	// 1. Create executor context
	// 2. Create executor factory
	// 3. Create the root executor from the plan
	// 4. Call Init() on the root executor
	// 5. Repeatedly call Next() and collect tuples
	// 6. Return all tuples

	panic("Execute not implemented")
}
