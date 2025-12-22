// Package main provides the entry point for BusTub-Go.
//
// This is a minimal shell that demonstrates how the components
// connect together. It's not intended for production use.
//
// For actual usage, see the test files which demonstrate
// each component in isolation.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("==============================================")
	fmt.Println("  BusTub-Go: A Go Implementation of BusTub")
	fmt.Println("==============================================")
	fmt.Println()
	fmt.Println("This is an educational database system based on CMU's BusTub.")
	fmt.Println()
	fmt.Println("Components to implement (in order):")
	fmt.Println()
	fmt.Println("Assignment #1 - Buffer Pool Manager")
	fmt.Println("  [ ] LRU-K Replacer (pkg/buffer/lru_k_replacer.go)")
	fmt.Println("  [ ] Buffer Pool Manager (pkg/buffer/buffer_pool_manager.go)")
	fmt.Println()
	fmt.Println("Assignment #2 - Storage & Indexing")
	fmt.Println("  [ ] Extendible Hash Table (pkg/container/hash/extendible_hash_table.go)")
	fmt.Println("  [ ] B+ Tree Index (pkg/storage/index/b_plus_tree.go)")
	fmt.Println()
	fmt.Println("Assignment #3 - Query Execution")
	fmt.Println("  [ ] SeqScan Executor")
	fmt.Println("  [ ] Insert/Update/Delete Executors")
	fmt.Println("  [ ] NestedLoopJoin & HashJoin Executors")
	fmt.Println("  [ ] Aggregation Executor")
	fmt.Println("  [ ] Sort, Limit, TopN Executors")
	fmt.Println()
	fmt.Println("Assignment #4 - Concurrency Control")
	fmt.Println("  [ ] MVCC Transaction Manager")
	fmt.Println("  [ ] Tuple Visibility")
	fmt.Println("  [ ] Lock Manager (optional)")
	fmt.Println()
	fmt.Println("Run 'go test ./...' to execute tests.")
	fmt.Println()

	if len(os.Args) > 1 && os.Args[1] == "--demo" {
		runDemo()
	}
}

// runDemo shows a simple demonstration of components.
func runDemo() {
	fmt.Println("=== Demo Mode ===")
	fmt.Println()
	fmt.Println("To run the demo, implement the core components first!")
	fmt.Println()
	fmt.Println("Example workflow:")
	fmt.Println("1. Create a DiskManager for a database file")
	fmt.Println("2. Create a BufferPoolManager with the DiskManager")
	fmt.Println("3. Create a Catalog with the BufferPoolManager")
	fmt.Println("4. Create tables and insert data")
	fmt.Println("5. Run queries through the ExecutionEngine")
	fmt.Println()

	// TODO: Uncomment this when components are implemented
	/*
		// Create disk manager
		dm, err := disk.NewDiskManager("test.db")
		if err != nil {
			fmt.Println("Error creating disk manager:", err)
			return
		}
		defer dm.Close()

		// Create buffer pool manager
		bpm := buffer.NewBufferPoolManager(common.BufferPoolSize, dm)

		// Create catalog
		catalog := catalog.NewCatalog(bpm)

		// Create a table
		schema := catalog.NewSchema([]catalog.Column{
			catalog.NewColumn("id", common.TypeInteger),
			catalog.NewColumn("name", common.TypeVarchar),
		})
		tableInfo := catalog.CreateTable("users", schema)
		fmt.Println("Created table:", tableInfo.Name)

		// Insert data (would use executors)
		fmt.Println("To insert data, implement the Insert executor!")

		// Query data
		fmt.Println("To query data, implement the SeqScan executor!")
	*/
}
