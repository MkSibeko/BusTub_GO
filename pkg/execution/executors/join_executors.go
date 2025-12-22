// Package executors provides executor implementations.
//
// This file contains join executors (NestedLoopJoin, HashJoin).
package executors

import (
	"github.com/bustub-go/pkg/catalog"
	"github.com/bustub-go/pkg/execution"
	"github.com/bustub-go/pkg/storage/table"
)

// =============================================================================
// NESTED LOOP JOIN EXECUTOR
// =============================================================================

// JoinType represents the type of join.
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull // Full outer join
)

// NestedLoopJoinPlan describes a nested loop join.
type NestedLoopJoinPlan struct {
	OutputSchema *catalog.Schema
	LeftChild    execution.Plan
	RightChild   execution.Plan
	Predicate    execution.Expression
	JoinType     JoinType
}

func (p *NestedLoopJoinPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *NestedLoopJoinPlan) GetChildren() []execution.Plan {
	return []execution.Plan{p.LeftChild, p.RightChild}
}
func (p *NestedLoopJoinPlan) GetType() execution.PlanType { return execution.PlanTypeNestedLoopJoin }

// NestedLoopJoinExecutor performs a nested loop join.
//
// IMPLEMENTATION NOTES:
// =====================
// The nested loop join is the simplest join algorithm:
//
//	for each tuple r in R:
//	    for each tuple s in S:
//	        if predicate(r, s):
//	            emit (r, s)
//
// Time complexity: O(|R| * |S|)
// Space complexity: O(1) - doesn't need to buffer either side
//
// HANDLING JOIN TYPES:
// - INNER: Only emit when predicate is true
// - LEFT: Emit left tuple with NULL right if no match found
// - RIGHT: Emit right tuple with NULL left if no match found
//
// STATEFUL ITERATION:
// The executor needs to remember:
// - Current left tuple
// - Position in right child
// - Whether current left tuple found any matches (for LEFT JOIN)
type NestedLoopJoinExecutor struct {
	execution.AbstractExecutor
	plan *NestedLoopJoinPlan

	leftExec  execution.Executor
	rightExec execution.Executor

	// State for iteration
	leftTuple   *table.Tuple
	leftDone    bool
	leftSchema  *catalog.Schema
	rightSchema *catalog.Schema

	// For LEFT JOIN: track if current left tuple had any matches
	leftMatched bool
}

func NewNestedLoopJoinExecutor(
	ctx *execution.ExecutorContext,
	plan *NestedLoopJoinPlan,
	leftChild, rightChild execution.Executor,
) *NestedLoopJoinExecutor {
	return &NestedLoopJoinExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		leftExec:         leftChild,
		rightExec:        rightChild,
	}
}

// Init initializes the nested loop join.
//
// TODO: Implement this
//
// STEPS:
// 1. Initialize both child executors
// 2. Get first tuple from left child
// 3. Store schemas for both sides
func (e *NestedLoopJoinExecutor) Init() {
	panic("NestedLoopJoinExecutor.Init not implemented")
}

// Next returns the next joined tuple.
//
// TODO: Implement this
//
// ALGORITHM (INNER JOIN):
//  1. If leftDone, return (nil, false)
//  2. Get next tuple from right child
//  3. If right child has tuple:
//     a. Evaluate predicate on (leftTuple, rightTuple)
//     b. If true, combine tuples and return
//     c. If false, continue (call Next again recursively)
//  4. If right child is done:
//     a. Re-initialize right child
//     b. Get next left tuple
//     c. If left is done, return (nil, false)
//     d. Continue with step 2
//
// ALGORITHM (LEFT JOIN):
// Same as above, but:
// - Track if current left tuple matched any right tuple
// - When moving to next left, if !leftMatched, emit (left, NULL)
//
// COMBINING TUPLES:
// The output tuple has columns from both sides concatenated.
// Use the output schema to serialize the combined values.
func (e *NestedLoopJoinExecutor) Next() (*table.Tuple, bool) {
	panic("NestedLoopJoinExecutor.Next not implemented")
}

// combinetuples combines a left and right tuple into one.
func (e *NestedLoopJoinExecutor) combineTuples(left, right *table.Tuple) *table.Tuple {
	// TODO: Implement this
	//
	// STEPS:
	// 1. Get values from left tuple
	// 2. Get values from right tuple (or NULLs if right is nil)
	// 3. Serialize all values according to output schema
	// 4. Return new tuple

	panic("combineTuples not implemented")
}

// =============================================================================
// HASH JOIN EXECUTOR
// =============================================================================

// HashJoinPlan describes a hash join.
type HashJoinPlan struct {
	OutputSchema  *catalog.Schema
	LeftChild     execution.Plan
	RightChild    execution.Plan
	LeftKeyExprs  []execution.Expression // Expressions to compute left join key
	RightKeyExprs []execution.Expression // Expressions to compute right join key
	JoinType      JoinType
}

func (p *HashJoinPlan) GetOutputSchema() *catalog.Schema { return p.OutputSchema }
func (p *HashJoinPlan) GetChildren() []execution.Plan {
	return []execution.Plan{p.LeftChild, p.RightChild}
}
func (p *HashJoinPlan) GetType() execution.PlanType { return execution.PlanTypeHashJoin }

// HashJoinExecutor performs a hash join.
//
// IMPLEMENTATION NOTES:
// =====================
// Hash join is efficient for equi-joins (WHERE a.id = b.id).
//
// ALGORITHM:
//  1. BUILD PHASE: Read all tuples from one side (usually smaller),
//     hash them by join key, store in hash table.
//  2. PROBE PHASE: Read tuples from other side, hash by join key,
//     look up matching tuples in hash table.
//
// Time complexity: O(|R| + |S|) expected
// Space complexity: O(|smaller|)
//
// IMPORTANT DETAILS:
// - The hash key is computed from LeftKeyExprs/RightKeyExprs
// - Multiple tuples can have the same hash key (collision + duplicates)
// - Store tuples (not RIDs) in the hash table
//
// For LEFT JOIN, track which left tuples matched.
type HashJoinExecutor struct {
	execution.AbstractExecutor
	plan *HashJoinPlan

	leftExec  execution.Executor
	rightExec execution.Executor

	// hashTable maps hash key -> list of tuples from build side
	// Using []byte as map key requires conversion to string
	hashTable map[string][]*table.Tuple

	// State for probe phase
	rightTuple    *table.Tuple
	matchingIndex int // Index into matching tuples list
	matchingList  []*table.Tuple
	buildDone     bool
	probeDone     bool

	leftSchema  *catalog.Schema
	rightSchema *catalog.Schema
}

func NewHashJoinExecutor(
	ctx *execution.ExecutorContext,
	plan *HashJoinPlan,
	leftChild, rightChild execution.Executor,
) *HashJoinExecutor {
	return &HashJoinExecutor{
		AbstractExecutor: execution.NewAbstractExecutor(ctx, plan),
		plan:             plan,
		leftExec:         leftChild,
		rightExec:        rightChild,
		hashTable:        make(map[string][]*table.Tuple),
	}
}

// Init initializes the hash join.
//
// TODO: Implement this
//
// STEPS:
//  1. Initialize both child executors
//  2. BUILD PHASE: Read all tuples from LEFT child
//  3. For each left tuple:
//     a. Compute hash key using LeftKeyExprs
//     b. Add tuple to hash table under that key
//  4. Set buildDone = true
func (e *HashJoinExecutor) Init() {
	panic("HashJoinExecutor.Init not implemented")
}

// Next returns the next joined tuple.
//
// TODO: Implement this
//
// ALGORITHM (PROBE PHASE):
//  1. If we have matching tuples from previous probe:
//     a. Return next match from matchingList
//     b. Increment matchingIndex
//  2. Get next tuple from right child
//  3. If right child is done, return (nil, false)
//  4. Compute hash key from right tuple using RightKeyExprs
//  5. Look up in hash table
//  6. If matches found, set matchingList and return first match
//  7. If no matches, continue to step 2
func (e *HashJoinExecutor) Next() (*table.Tuple, bool) {
	panic("HashJoinExecutor.Next not implemented")
}

// computeHashKey computes the hash key for a tuple.
func (e *HashJoinExecutor) computeHashKey(tuple *table.Tuple, schema *catalog.Schema, keyExprs []execution.Expression) string {
	// TODO: Implement this
	//
	// STEPS:
	// 1. Evaluate each key expression on the tuple
	// 2. Serialize the values into a byte array
	// 3. Return as string (for use as map key)

	panic("computeHashKey not implemented")
}

// combineTuples combines a left and right tuple.
func (e *HashJoinExecutor) combineTuples(left, right *table.Tuple) *table.Tuple {
	// Same as NestedLoopJoin
	panic("combineTuples not implemented")
}
