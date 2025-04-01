package main

import (
	"database/sql"
	"fmt"
	"log"
	"os/exec"
	"runtime"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func getMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}

func runTempTableApproach(db *sql.DB) (time.Duration, uint64) {
	startMem := getMemoryUsage()
	startTime := time.Now()

	// Drop temp table if exists
	_, err := db.Exec(`DROP TABLE IF EXISTS temp_data`)
	if err != nil {
		log.Fatal(err)
	}

	// Create temp table
	_, err = db.Exec(`
		CREATE TEMP TABLE temp_data AS 
		SELECT
			timestamp as tp_timestamp,
			account_id as tp_index
		FROM read_ndjson('test_data/logs_large.jsonl')
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Get count
	var count int64
	err = db.QueryRow("SELECT COUNT(*) FROM temp_data").Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	// Copy to output
	_, err = db.Exec(`
		COPY temp_data 
		TO 'test_data/output_temp.jsonl' (FORMAT JSON)
	`)
	if err != nil {
		log.Fatal(err)
	}

	duration := time.Since(startTime)
	endMem := getMemoryUsage()
	memoryUsed := endMem - startMem

	return duration, memoryUsed
}

func runSeparateQueryApproach(db *sql.DB) (time.Duration, uint64) {
	startMem := getMemoryUsage()
	startTime := time.Now()

	// Get count using wc -l
	cmd := exec.Command("wc", "-l", "test_data/logs_large.jsonl")
	_, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}

	// Copy to output
	_, err = db.Exec(`
		COPY (
			SELECT
				timestamp as tp_timestamp,
				account_id as tp_index
			FROM read_ndjson('test_data/logs_large.jsonl')
		) TO 'test_data/output_separate.jsonl' (FORMAT JSON)
	`)
	if err != nil {
		log.Fatal(err)
	}

	duration := time.Since(startTime)
	endMem := getMemoryUsage()
	memoryUsed := endMem - startMem

	return duration, memoryUsed
}

func runBenchmarks() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Run each approach multiple times
	iterations := 3
	fmt.Println("Running benchmarks...")
	fmt.Println("----------------------------------------")

	// Temp Table Approach
	fmt.Println("Temp Table Approach:")
	var tempTableTimes []time.Duration
	var tempTableMemory []uint64
	for i := 0; i < iterations; i++ {
		duration, memory := runTempTableApproach(db)
		tempTableTimes = append(tempTableTimes, duration)
		tempTableMemory = append(tempTableMemory, memory)
		fmt.Printf("Run %d: Time: %v, Memory: %d MB\n", i+1, duration, memory/1024/1024)
	}

	// Separate Query Approach
	fmt.Println("\nSeparate Query Approach:")
	var separateTimes []time.Duration
	var separateMemory []uint64
	for i := 0; i < iterations; i++ {
		duration, memory := runSeparateQueryApproach(db)
		separateTimes = append(separateTimes, duration)
		separateMemory = append(separateMemory, memory)
		fmt.Printf("Run %d: Time: %v, Memory: %d MB\n", i+1, duration, memory/1024/1024)
	}

	// Calculate averages
	var avgTempTime, avgSeparateTime time.Duration
	var avgTempMem, avgSeparateMem uint64

	for _, t := range tempTableTimes {
		avgTempTime += t
	}
	avgTempTime /= time.Duration(iterations)

	for _, t := range separateTimes {
		avgSeparateTime += t
	}
	avgSeparateTime /= time.Duration(iterations)

	for _, m := range tempTableMemory {
		avgTempMem += m
	}
	avgTempMem /= uint64(iterations)

	for _, m := range separateMemory {
		avgSeparateMem += m
	}
	avgSeparateMem /= uint64(iterations)

	fmt.Println("\nResults Summary:")
	fmt.Println("----------------------------------------")
	fmt.Printf("Temp Table Approach:\n")
	fmt.Printf("  Average Time: %v\n", avgTempTime)
	fmt.Printf("  Average Memory: %d MB\n", avgTempMem/1024/1024)
	fmt.Printf("\nSeparate Query Approach:\n")
	fmt.Printf("  Average Time: %v\n", avgSeparateTime)
	fmt.Printf("  Average Memory: %d MB\n", avgSeparateMem/1024/1024)
}

func main() {
	runBenchmarks()
}
