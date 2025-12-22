// Package executors provides executor implementations.
//
// This file contains aggregation, sorting, and limit executors.
package executors

import (
	"github.com/bustub-go/pkg/catalog"
	"github.com/bustub-go/pkg/execution"
	"github.com/bustub-go/pkg/storage/table"
)

// =============================================================================
// AGGREGATION EXECUTOR
// =============================================================================

// AggregationType identifies the type of aggregate function.
type AggregationType int

const (
	AggregateCountStar AggregationType = iota
	AggregateCount
	AggregateSum
	AggregateMin
	AggregateMax
)

// AggregationPlan describes an aggregation operation.
type AggregationPlan struct {
	OutputSchema *catalog.Schema
	Child        execution.Plan

	// GroupByExprs are the expressions to group by.
	// Empty for aggregation without GROUP BY.
	GroupByExprs []execution.Expression

	// AggregateExprs are the expressions to aggregate.
	AggregateExprs []execution.Expression

	// AggregateTypes specifies the type of each aggregate.
	AggregateTypes []AggregationType
}

func (p *AggregationPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *AggregationPlan) GetChildren() []execution.Plan    { return []execution.Plan{p.Child} }
func (p *AggregationPlan) GetType() execution.PlanType      { return execution.PlanTypeAggregation }

// AggregateKey represents a group-by key.
type AggregateKey struct {
	Values []execution.Value
}

// AggregateValue represents accumulated aggregate values.
type AggregateValue struct {
	Values []execution.Value
}

// AggregationExecutor performs aggregation (GROUP BY).
//
// IMPLEMENTATION NOTES:
// =====================
// Aggregation is a BLOCKING operator - it must read ALL input
// tuples before producing any output.
//
// ALGORITHM:
//  1. Read all tuples from child
//  2. For each tuple:
//     a. Compute GROUP BY key
//     b. Compute aggregate input values
//     c. Update running aggregates for this group
//  3. After all input consumed, iterate over groups
//  4. For each group, emit one output tuple
//
// DATA STRUCTURE:
// Use a hash table: GROUP_BY_KEY -> AGGREGATE_VALUES
//
// AGGREGATE OPERATIONS:
// - COUNT(*): Increment counter
// - COUNT(x): Increment if x is not NULL
// - SUM(x): Add x to running sum
// - MIN(x): Keep minimum value
// - MAX(x): Keep maximum value
// - AVG(x): Track sum and count, divide at the end
type AggregationExecutor struct {
	execution.AbstractExecutor
	plan      *AggregationPlan
	childExec execution.Executor

	// hashTable maps group key -> aggregate values
	hashTable map[string]*AggregateValue

	// For iterating over results
	results     []*table.Tuple
	resultIdx   int
	initialized bool
}

func NewAggregationExecutor(ctx *execution.ExecutorContext, plan *AggregationPlan, child execution.Executor) *AggregationExecutor {
	return &AggregationExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		childExec:        child,
		hashTable:        make(map[string]*AggregateValue),
	}
}

// Init initializes the aggregation executor.
//
// TODO: Implement this
//
// STEPS:
//  1. Initialize child executor
//  2. Read ALL tuples from child
//  3. For each tuple:
//     a. Compute group key
//     b. If group not in hash table, initialize aggregate values
//     c. Update aggregate values with this tuple's contribution
//  4. Build result tuples from hash table
//  5. Set initialized = true
func (e *AggregationExecutor) Init() {
	panic("AggregationExecutor.Init not implemented")
}

// Next returns the next aggregation result.
//
// TODO: Implement this
//
// STEPS:
// 1. If resultIdx >= len(results), return (nil, false)
// 2. Return results[resultIdx] and increment
func (e *AggregationExecutor) Next() (*table.Tuple, bool) {
	panic("AggregationExecutor.Next not implemented")
}

// computeGroupKey computes the group-by key for a tuple.
func (e *AggregationExecutor) computeGroupKey(tuple *table.Tuple, schema *catalog.Schema) string {
	// TODO: Implement this
	//
	// STEPS:
	// 1. Evaluate each GroupByExpr on the tuple
	// 2. Serialize the values
	// 3. Return as string key

	panic("computeGroupKey not implemented")
}

// combineAggregateValues updates aggregate values with a new tuple.
func (e *AggregationExecutor) combineAggregateValues(agg *AggregateValue, tuple *table.Tuple, schema *catalog.Schema) {
	// TODO: Implement this
	//
	// For each aggregate in plan.AggregateTypes:
	// 1. Evaluate the corresponding AggregateExpr
	// 2. Update the aggregate value based on type:
	//    - COUNT(*): agg.Values[i]++
	//    - COUNT(x): if x != NULL, agg.Values[i]++
	//    - SUM(x): agg.Values[i] += x
	//    - MIN(x): agg.Values[i] = min(agg.Values[i], x)
	//    - MAX(x): agg.Values[i] = max(agg.Values[i], x)

	panic("combineAggregateValues not implemented")
}

// =============================================================================
// SORT EXECUTOR
// =============================================================================

// OrderByType specifies sort order.
type OrderByType int

const (
	OrderByAsc OrderByType = iota
	OrderByDesc
)

// SortPlan describes a sort operation.
type SortPlan struct {
	OutputSchema *catalog.Schema
	Child        execution.Plan
	OrderByExprs []execution.Expression
	OrderByTypes []OrderByType
}

func (p *SortPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *SortPlan) GetChildren() []execution.Plan    { return []execution.Plan{p.Child} }
func (p *SortPlan) GetType() execution.PlanType      { return execution.PlanTypeSort }

// SortExecutor sorts tuples.
//
// IMPLEMENTATION NOTES:
// =====================
// Sort is a BLOCKING operator - must read all input before output.
//
// ALGORITHM:
// 1. Read all tuples from child into memory
// 2. Sort using the ORDER BY expressions
// 3. Emit tuples one at a time
//
// SORTING:
// Use Go's sort.Slice with a custom comparator that:
// 1. Evaluates ORDER BY expressions on both tuples
// 2. Compares values, respecting ASC/DESC
// 3. For ties, moves to next ORDER BY column
//
// OPTIMIZATION:
// For large datasets, consider external merge sort.
// BusTub sticks to in-memory sort for simplicity.
type SortExecutor struct {
	execution.AbstractExecutor
	plan      *SortPlan
	childExec execution.Executor

	sortedTuples []*table.Tuple
	cursor       int
	initialized  bool
}

func NewSortExecutor(ctx *execution.ExecutorContext, plan *SortPlan, child execution.Executor) *SortExecutor {
	return &SortExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		childExec:        child,
	}
}

// Init initializes the sort executor.
//
// TODO: Implement this
//
// STEPS:
// 1. Initialize child executor
// 2. Read ALL tuples from child into sortedTuples
// 3. Sort sortedTuples using ORDER BY expressions
// 4. Reset cursor to 0
func (e *SortExecutor) Init() {
	panic("SortExecutor.Init not implemented")
}

// Next returns the next sorted tuple.
//
// TODO: Implement this
func (e *SortExecutor) Next() (*table.Tuple, bool) {
	panic("SortExecutor.Next not implemented")
}

// =============================================================================
// LIMIT EXECUTOR
// =============================================================================

// LimitPlan describes a LIMIT operation.
type LimitPlan struct {
	OutputSchema *catalog.Schema
	Child        execution.Plan
	Limit        int
	Offset       int // OFFSET clause
}

func (p *LimitPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *LimitPlan) GetChildren() []execution.Plan    { return []execution.Plan{p.Child} }
func (p *LimitPlan) GetType() execution.PlanType      { return execution.PlanTypeLimit }

// LimitExecutor limits the number of output tuples.
//
// IMPLEMENTATION NOTES:
// =====================
// Limit is a PIPELINE operator - doesn't need to buffer.
//
// Simply count tuples and stop when limit is reached.
// For OFFSET, skip that many tuples first.
type LimitExecutor struct {
	execution.AbstractExecutor
	plan      *LimitPlan
	childExec execution.Executor
	count     int // Tuples emitted so far
	offset    int // Tuples skipped so far
}

func NewLimitExecutor(ctx *execution.ExecutorContext, plan *LimitPlan, child execution.Executor) *LimitExecutor {
	return &LimitExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		childExec:        child,
	}
}

// Init initializes the limit executor.
func (e *LimitExecutor) Init() {
	e.childExec.Init()
	e.count = 0
	e.offset = 0
}

// Next returns the next tuple within the limit.
//
// TODO: Implement this
//
// STEPS:
// 1. Skip OFFSET tuples if not already done
// 2. If count >= limit, return (nil, false)
// 3. Get next tuple from child
// 4. If child done, return (nil, false)
// 5. Increment count and return tuple
func (e *LimitExecutor) Next() (*table.Tuple, bool) {
	panic("LimitExecutor.Next not implemented")
}

// =============================================================================
// TOP-N EXECUTOR
// =============================================================================

// TopNPlan describes a Top-N operation (ORDER BY ... LIMIT N optimization).
type TopNPlan struct {
	OutputSchema *catalog.Schema
	Child        execution.Plan
	OrderByExprs []execution.Expression
	OrderByTypes []OrderByType
	N            int
}

func (p *TopNPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *TopNPlan) GetChildren() []execution.Plan    { return []execution.Plan{p.Child} }
func (p *TopNPlan) GetType() execution.PlanType      { return execution.PlanTypeTopN }

// TopNExecutor returns the top N tuples.
//
// IMPLEMENTATION NOTES:
// =====================
// TopN is an OPTIMIZATION for ORDER BY ... LIMIT N.
// Instead of sorting ALL tuples, we use a priority queue
// (heap) to keep only the top N.
//
// ALGORITHM:
//  1. Use a max-heap (for ASC) or min-heap (for DESC) of size N
//  2. For each input tuple:
//     a. If heap has < N elements, add tuple
//     b. If tuple is "better" than worst in heap, replace
//  3. At the end, extract tuples from heap in order
//
// Time: O(m * log N) where m is total input tuples
// Space: O(N) - only keep N tuples in memory
//
// This is much better than full sort when N << m.
type TopNExecutor struct {
	execution.AbstractExecutor
	plan      *TopNPlan
	childExec execution.Executor

	// Use a slice as a heap
	heap        []*table.Tuple
	results     []*table.Tuple
	resultIdx   int
	initialized bool
}

func NewTopNExecutor(ctx *execution.ExecutorContext, plan *TopNPlan, child execution.Executor) *TopNExecutor {
	return &TopNExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		childExec:        child,
	}
}

// Init initializes the top-n executor.
//
// TODO: Implement this
//
// STEPS:
// 1. Initialize child executor
// 2. Read tuples from child
// 3. Maintain a heap of size N:
//   - If heap has < N elements, push
//   - If new tuple beats worst, pop worst and push new
//
// 4. Extract heap contents in sorted order into results
func (e *TopNExecutor) Init() {
	panic("TopNExecutor.Init not implemented")
}

// Next returns the next top-N tuple.
//
// TODO: Implement this
func (e *TopNExecutor) Next() (*table.Tuple, bool) {
	panic("TopNExecutor.Next not implemented")
}
