package resmon

// #include <mach/mach.h>
import "C"
import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"
)

// GetFilesDiskUsage calculates the total disk usage of files matching the given patterns.
// It processes each pattern concurrently and accumulates the total size of all matching files.
// patterns: slice of file path patterns to match
// Returns total size in bytes and error if any occurred during processing
func GetFilesDiskUsage(patterns []string) (uint64, error) {
	var totalSize uint64
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, pattern := range patterns {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			matches, err := filepath.Glob(p)
			if err != nil {
				log.Printf("Error matching pattern %s: %v", p, err)
				return
			}

			for _, match := range matches {
				info, err := os.Stat(match)
				if err != nil {
					log.Printf("Error getting file info for %s: %v", match, err)
					continue
				}
				if !info.IsDir() {
					mu.Lock()
					totalSize += uint64(info.Size())
					mu.Unlock()
				}
			}
		}(pattern)
	}

	wg.Wait()
	return totalSize, nil
}

// MonitorResourceUsage starts a goroutine that periodically logs memory statistics
// until the done channel is closed. The interval parameter specifies how ofte	n
// to log the statistics.

type ResourceUsageStats struct {
	MaxMemoryUsageMb uint64 // Memory usage in megabytes
	MaxDiskUsageMb   uint64
}

// MonitorResourceUsage starts a monitoring goroutine that tracks memory and disk usage statistics
// at specified intervals until the context is canceled.
// ctx: context for cancellation
// interval: duration between measurements
// filePatterns: patterns of files to monitor for disk usage
// Returns a channel that will receive the final ResourceUsageStats when monitoring stops
func MonitorResourceUsage(ctx context.Context, interval time.Duration, filePatterns []string) chan ResourceUsageStats {
	var resultChan = make(chan ResourceUsageStats, 1)
	var maxMem uint64 = 0
	var maxDisk uint64 = 0
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Get current process
		//p, err := process.NewProcess(int32(os.Getpid()))
		//if err != nil {
		//	fmt.Printf("Failed to get process: %v", err)
		//	return
		//}

		for {
			select {
			case <-ticker.C:

				totalMemory, err := GetActivityMonMemory()
				if err != nil {
					continue
				}

				if totalMemory > maxMem {
					maxMem = totalMemory
				}

				diskUsage, err := GetFilesDiskUsage(filePatterns)
				if err != nil {
					log.Printf("Failed to get disk usage: %v", err)
					continue
				}
				if diskUsage > maxDisk {
					maxDisk = diskUsage
				}

				//LogProcessMemStats(label, memInfo, m)
			case <-ctx.Done():
				resultChan <- ResourceUsageStats{
					MaxMemoryUsageMb: maxMem / (1024 * 1024),
					MaxDiskUsageMb:   maxDisk / (1024 * 1024),
				}
				return
			}
		}
	}()
	return resultChan
}

// GetActivityMonMemory retrieves the current memory usage of the process
// - it returns a figure which matches Mac activity monitor
// by combining resident memory size and compressed memory.
// Returns total memory usage in bytes and error if retrieval fails
func GetActivityMonMemory() (uint64, error) {
	stats, err := GetProcessMemoryInfo(os.Getpid())
	if err != nil {
		log.Printf("Failed to get process memory: %v", err)
		return 0, err
	}
	totalMemory := stats["resident_size"] + stats["compressed"]
	return totalMemory / (1024 * 10243), nil

}

// GetProcessMemoryInfo retrieves detailed memory statistics for a specific process
// using the Mach task API on Darwin systems.
// pid: process ID to get memory information for
// Returns a map containing memory statistics (resident_size, virtual_size, compressed) in bytes
// and error if retrieval fails
func GetProcessMemoryInfo(pid int) (map[string]uint64, error) {
	var task C.task_t

	// Get the task port for the specified PID
	kernReturn := C.task_for_pid(C.mach_task_self_, C.int(pid), &task)
	if kernReturn != C.KERN_SUCCESS {
		return nil, fmt.Errorf("failed to get task for pid %d: %d", pid, kernReturn)
	}
	defer C.mach_port_deallocate(C.mach_task_self_, task)

	// Get task memory info
	var taskInfo C.task_vm_info_data_t
	var count = C.mach_msg_type_number_t(C.TASK_VM_INFO_COUNT)

	kernReturn = C.task_info(
		task,
		C.TASK_VM_INFO,
		(*C.integer_t)(unsafe.Pointer(&taskInfo)),
		&count,
	)

	if kernReturn != C.KERN_SUCCESS {
		return nil, fmt.Errorf("failed to get task info: %d", kernReturn)
	}

	stats := map[string]uint64{
		"resident_size": uint64(taskInfo.resident_size),
		"virtual_size":  uint64(taskInfo.virtual_size),
		"compressed":    uint64(taskInfo.compressed),
	}

	return stats, nil
}
