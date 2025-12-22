// Package buffer - LRU-K Replacer implementation.
//
// The LRU-K algorithm evicts the frame whose backward k-distance is maximum.
// Backward k-distance is the difference between current timestamp and the
// timestamp of the k-th previous access.
//
// ASSIGNMENT 1 - LRU-K REPLACER:
// ===============================
// This is a main component you need to implement for Assignment 1.
//
// Algorithm Overview:
// 1. Each frame tracks its access history (up to K accesses)
// 2. Backward k-distance = current_timestamp - timestamp of k-th previous access
// 3. Frames with < K accesses have +infinity backward k-distance
// 4. Evict the frame with maximum backward k-distance
// 5. Ties are broken using LRU among frames with +infinity distance
//
// Why LRU-K?
// - Better than simple LRU for scan-resistant caching
// - Avoids evicting frequently accessed pages due to one-time scans
// - K=2 is common in practice, BusTub uses K=10
//
// This mirrors: bustub/src/include/buffer/lru_k_replacer.h
package buffer

import (
	"container/list"
	"math"
	"sync"

	"github.com/bustub-go/pkg/common"
)

// LRUKNode stores the access history for a single frame.
//
// TODO (ASSIGNMENT): You may modify this structure as needed.
type LRUKNode struct {
	// history stores timestamps of last K accesses.
	// Oldest access is at the front, newest at the back.
	history *list.List

	// k is the K value (from the replacer).
	k int

	// frameID is the frame this node represents.
	frameID common.FrameID

	// isEvictable indicates if this frame can be evicted.
	isEvictable bool
}

// NewLRUKNode creates a new node for the given frame.
func NewLRUKNode(frameID common.FrameID, k int) *LRUKNode {
	return &LRUKNode{
		history:     list.New(),
		k:           k,
		frameID:     frameID,
		isEvictable: false,
	}
}

// RecordAccess adds a timestamp to the access history.
//
// TODO (ASSIGNMENT): Implement this method.
// Keep only the last K accesses in the history.
func (n *LRUKNode) RecordAccess(timestamp int) {
	// TODO: Add timestamp to history
	// If history has more than K entries, remove the oldest
	panic("TODO: Implement LRUKNode.RecordAccess()")
}

// GetBackwardKDistance computes the backward k-distance.
//
// TODO (ASSIGNMENT): Implement this method.
//
// If the frame has been accessed fewer than K times, return +infinity.
// Otherwise, return currentTimestamp - timestamp of k-th previous access.
func (n *LRUKNode) GetBackwardKDistance(currentTimestamp int) float64 {
	// TODO: Implement backward k-distance calculation
	// Hint: If history.Len() < k, return math.MaxFloat64 (+infinity)
	// Otherwise, return currentTimestamp - history.Front().Value.(int)
	panic("TODO: Implement LRUKNode.GetBackwardKDistance()")
}

// GetEarliestAccessTime returns the earliest access timestamp.
//
// Used to break ties among frames with +infinity k-distance.
// Returns the timestamp of the very first access.
func (n *LRUKNode) GetEarliestAccessTime() int {
	if n.history.Len() == 0 {
		return math.MaxInt
	}
	return n.history.Front().Value.(int)
}

// HasKAccesses returns true if this frame has been accessed at least K times.
func (n *LRUKNode) HasKAccesses() bool {
	return n.history.Len() >= n.k
}

// LRUKReplacer implements the LRU-K replacement policy.
//
// TODO (ASSIGNMENT 1 - LRU-K REPLACER):
// ======================================
// You need to implement all methods of this struct:
//
// 1. Evict() - Find and return the frame with maximum backward k-distance
// 2. RecordAccess() - Record that a frame was accessed
// 3. SetEvictable() - Mark a frame as evictable or not
// 4. Remove() - Remove a frame from the replacer
// 5. Size() - Return the number of evictable frames
//
// Implementation Hints:
// - Use a map to store LRUKNode for each frame
// - Use a mutex for thread safety
// - Track current timestamp and increment on each access
// - When evicting, iterate over all evictable frames to find the victim
//
// Data Structures:
// - nodeStore: map[FrameID]*LRUKNode - stores all tracked frames
// - currentTimestamp: int - logical clock for access ordering
// - currSize: int - number of currently evictable frames
type LRUKReplacer struct {
	// nodeStore maps frame IDs to their LRUKNode.
	// All frames that have been accessed are stored here.
	nodeStore map[common.FrameID]*LRUKNode

	// currentTimestamp is the logical clock for ordering accesses.
	// Incremented on each RecordAccess call.
	currentTimestamp int

	// currSize is the number of evictable frames.
	// Updated when SetEvictable is called.
	currSize int

	// replacerSize is the maximum number of frames to track.
	replacerSize int

	// k is the K value for LRU-K.
	k int

	// mu protects all fields.
	mu sync.Mutex
}

// NewLRUKReplacer creates a new LRU-K replacer.
//
// Parameters:
// - numFrames: Maximum number of frames the replacer will track
// - k: The K value for the LRU-K algorithm
func NewLRUKReplacer(numFrames int, k int) *LRUKReplacer {
	return &LRUKReplacer{
		nodeStore:        make(map[common.FrameID]*LRUKNode),
		currentTimestamp: 0,
		currSize:         0,
		replacerSize:     numFrames,
		k:                k,
	}
}

// Evict finds a victim frame for eviction using the LRU-K policy.
//
// TODO (ASSIGNMENT 1):
// =====================
// Find the frame with the maximum backward k-distance among all
// evictable frames.
//
// Algorithm:
// 1. Iterate over all frames in nodeStore
// 2. Skip frames that are not evictable
// 3. Calculate backward k-distance for each evictable frame
// 4. Track the frame with maximum k-distance
// 5. For frames with +infinity k-distance, use LRU (earliest access wins)
// 6. Remove the victim from nodeStore and return its ID
//
// Returns:
// - The FrameID of the evicted frame
// - true if eviction was successful, false if no evictable frames
//
// Thread Safety: This method must be thread-safe.
func (r *LRUKReplacer) Evict() (common.FrameID, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO (ASSIGNMENT): Implement LRU-K eviction
	//
	// Pseudocode:
	// victimFrame := InvalidFrameID
	// maxKDistance := -1.0
	// earliestAccess := MaxInt
	//
	// for frameID, node := range r.nodeStore {
	//     if !node.isEvictable {
	//         continue
	//     }
	//
	//     kDistance := node.GetBackwardKDistance(r.currentTimestamp)
	//
	//     // Update victim if this frame has larger k-distance
	//     // or same +inf k-distance but earlier first access
	//     ...
	// }
	//
	// if victimFrame != InvalidFrameID {
	//     delete(r.nodeStore, victimFrame)
	//     r.currSize--
	//     return victimFrame, true
	// }
	// return InvalidFrameID, false

	panic("TODO: Implement LRUKReplacer.Evict()")
}

// RecordAccess records that a frame has been accessed.
//
// TODO (ASSIGNMENT 1):
// =====================
// Record the access in the frame's history.
//
// Steps:
// 1. If frame doesn't exist in nodeStore, create a new LRUKNode
// 2. Validate the frame ID is within bounds
// 3. Record the access timestamp in the node's history
// 4. Increment the current timestamp
//
// Parameters:
// - frameID: The frame that was accessed
// - accessType: Type of access (may affect some policies, optional)
//
// Thread Safety: This method must be thread-safe.
func (r *LRUKReplacer) RecordAccess(frameID common.FrameID, accessType common.AccessType) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO (ASSIGNMENT): Implement access recording
	//
	// if frameID is invalid (< 0 or >= replacerSize), panic or return error
	//
	// node, exists := r.nodeStore[frameID]
	// if !exists {
	//     node = NewLRUKNode(frameID, r.k)
	//     r.nodeStore[frameID] = node
	// }
	//
	// node.RecordAccess(r.currentTimestamp)
	// r.currentTimestamp++

	panic("TODO: Implement LRUKReplacer.RecordAccess()")
}

// SetEvictable sets whether a frame can be evicted.
//
// TODO (ASSIGNMENT 1):
// =====================
// Update the evictable status of a frame and adjust currSize accordingly.
//
// Steps:
// 1. Find the frame in nodeStore (do nothing if not found)
// 2. If the evictable status is changing:
//   - If becoming evictable: increment currSize
//   - If becoming non-evictable: decrement currSize
//
// 3. Update the node's isEvictable flag
//
// Parameters:
// - frameID: The frame to update
// - evictable: true if the frame can be evicted
//
// Thread Safety: This method must be thread-safe.
func (r *LRUKReplacer) SetEvictable(frameID common.FrameID, evictable bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO (ASSIGNMENT): Implement SetEvictable
	//
	// node, exists := r.nodeStore[frameID]
	// if !exists {
	//     return // Frame not tracked yet
	// }
	//
	// if node.isEvictable != evictable {
	//     if evictable {
	//         r.currSize++
	//     } else {
	//         r.currSize--
	//     }
	//     node.isEvictable = evictable
	// }

	panic("TODO: Implement LRUKReplacer.SetEvictable()")
}

// Remove removes a frame from the replacer entirely.
//
// TODO (ASSIGNMENT 1):
// =====================
// Remove the frame from tracking completely.
//
// Steps:
// 1. Find the frame in nodeStore
// 2. If it exists and is evictable, decrement currSize
// 3. Delete the frame from nodeStore
//
// This is called when a page is deleted from the buffer pool.
//
// Thread Safety: This method must be thread-safe.
func (r *LRUKReplacer) Remove(frameID common.FrameID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO (ASSIGNMENT): Implement Remove
	//
	// node, exists := r.nodeStore[frameID]
	// if !exists {
	//     return
	// }
	//
	// if node.isEvictable {
	//     r.currSize--
	// }
	// delete(r.nodeStore, frameID)

	panic("TODO: Implement LRUKReplacer.Remove()")
}

// Size returns the number of evictable frames.
//
// This is the number of frames that can currently be evicted,
// i.e., frames with isEvictable = true.
func (r *LRUKReplacer) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currSize
}
