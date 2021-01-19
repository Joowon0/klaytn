package prque

import (
	"math/rand"
	"testing"

	"github.com/klaytn/klaytn/common"
)

func BenchmarkHeapByteSlicePush(b *testing.B) {
	// Create some initial data
	data := make([][]byte, b.N)
	for i := 0; i < len(data); i++ {
		data[i] = common.MakeRandomBytes(256)
	}
	// Execute the benchmark
	b.ResetTimer()
	var queue ByteHeap
	for i := 0; i < len(data); i++ {
		queue.Push(data[i])
	}
}

func BenchmarkHeapByteSlicePop(b *testing.B) {
	// Create some initial data
	data := make([][]byte, b.N)
	for i := 0; i < len(data); i++ {
		data[i] = common.MakeRandomBytes(256)
	}
	var queue ByteHeap
	for i := 0; i < len(data); i++ {
		queue.Push(data[i])
	}
	// Execute the benchmark
	b.ResetTimer()
	for queue.Len() > 0 {
		queue.Pop()
	}
}

func BenchmarkIntHeapPush(b *testing.B) {
	// Create some initial data
	data := make([]int64, b.N)
	for i := 0; i < len(data); i++ {
		data[i] = rand.Int63()
	}
	// Execute the benchmark
	b.ResetTimer()
	var queue IntHeap
	for i := 0; i < len(data); i++ {
		queue.Push(data[i])
	}
}

func BenchmarkIntHeapPop(b *testing.B) {
	// Create some initial data
	data := make([]int64, b.N)
	for i := 0; i < len(data); i++ {
		data[i] = rand.Int63()
	}
	var queue IntHeap
	for i := 0; i < len(data); i++ {
		queue.Push(data[i])
	}
	// Execute the benchmark
	b.ResetTimer()
	for queue.Len() > 0 {
		queue.Pop()
	}
}
