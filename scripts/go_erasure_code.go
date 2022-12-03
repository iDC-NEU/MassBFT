package main

/*
#include <stdio.h>
#include <stdlib.h>

*/
import "C"
import (
	"bytes"
	"github.com/klauspost/reedsolomon"
	"github.com/orcaman/concurrent-map/v2"
	"log"
	"strconv"
	"sync/atomic"
	"unsafe"
)

type Integer int64

func (i Integer) String() string {
	return strconv.Itoa(int(i))
}

var engines cmap.ConcurrentMap[Integer, reedsolomon.Encoder]
var maxIndex int64
var encodeBuffer cmap.ConcurrentMap[Integer, [][]byte]
var decodeBuffer cmap.ConcurrentMap[Integer, []byte]

func init() {
	engines = cmap.NewStringer[Integer, reedsolomon.Encoder]()
	encodeBuffer = cmap.NewStringer[Integer, [][]byte]()
	decodeBuffer = cmap.NewStringer[Integer, []byte]()
	maxIndex = 0
	//runtime.GOMAXPROCS(runtime.NumCPU()/3 + 1)
}

//export instanceCreate
func instanceCreate(dataNum, parityNum int) int {
	instance, err := reedsolomon.New(dataNum, parityNum)
	if err != nil {
		log.Println("Error creating erasureCode instance")
		return -1
	}
	old := maxIndex // grab a token
	for !atomic.CompareAndSwapInt64(&maxIndex, old, maxIndex+1) {
		old = maxIndex
	}
	engines.Set(Integer(old), instance)
	return int(old)
}

//export instanceDestroy
func instanceDestroy(id int) int {
	engines.Remove(Integer(id))
	encodeBuffer.Remove(Integer(id))
	decodeBuffer.Remove(Integer(id))
	return 0
}

//export encodeFirst
func encodeFirst(id int, data unsafe.Pointer, dataSize int, fragmentLen *int, dataLen *int) int {
	dataRaw := unsafe.Slice((*byte)(data), dataSize)
	enc, suc := engines.Get(Integer(id))
	if !suc {
		return -1
	}
	shardsReal, err := enc.Split(dataRaw)
	if err != nil {
		log.Println("Error split data")
		return -1
	}
	*fragmentLen = len(shardsReal[0])
	err = enc.Encode(shardsReal)
	if err != nil {
		log.Println("Error encode data")
		return -1
	}
	*dataLen = len(shardsReal)
	encodeBuffer.Set(Integer(id), shardsReal)
	return 0
}

//export encodeNext
func encodeNext(id int, shards unsafe.Pointer, size int) int {
	goShard, _ := encodeBuffer.Get(Integer(id))
	sharRef := unsafe.Slice((*unsafe.Pointer)(shards), size)
	// 1
	for k, v := range goShard {
		// sharRef[k] = C.CBytes(v)
		// sharRef[k] = unsafe.Pointer(&v[0])
		srk := unsafe.Slice((*byte)(sharRef[k]), len(v))
		copy(srk, v)
	}
	// 2
	/*	var wg sync.WaitGroup
		wkCnt := runtime.NumGoroutine()
		if wkCnt > len(goShard) {
			wkCnt = len(goShard)
		}
		wg.Add(wkCnt)
		for i := 0; i < wkCnt; i++ {
			// sharRef[k] = C.CBytes(v)
			// sharRef[k] = unsafe.Pointer(&v[0])
			go func(idx int) {
				for j := idx; j < len(goShard); j += wkCnt {
					srk := unsafe.Slice((*byte)(sharRef[j]), len(goShard[j]))
					copy(srk, goShard[j])
				}
				wg.Done()
			}(i)
		}
		wg.Wait()*/
	return 0
}

//export encodeCleanup
func encodeCleanup(id int, shards unsafe.Pointer, len int) {
	// sharRef := unsafe.Slice((*unsafe.Pointer)(shards), len)
	// for i := 0; i < len; i++ {
	//	C.free(sharRef[i])
	// }
}

// shards[i] = nil in order
//
//export decode
func decode(id int, shards unsafe.Pointer, shardLen int, fLength int, dataSize int, data unsafe.Pointer) int {
	enc, suc := engines.Get(Integer(id))
	if !suc {
		return -1
	}
	shardsIn := unsafe.Slice((*unsafe.Pointer)(shards), shardLen)
	shardsRaw := make([][]byte, shardLen)
	for i := 0; i < shardLen; i++ {
		if shardsIn[i] != nil {
			shardsRaw[i] = unsafe.Slice((*byte)(shardsIn[i]), fLength)
		}
	}
	// Verify the shards
	//if ok, err := enc.Verify(shardsRaw); !ok || err != nil {
	// log.Println("Verification failed. Reconstructing data, ", err)
	if err := enc.ReconstructData(shardsRaw); err != nil {
		log.Println("Reconstruct failed, ", err)
		return -1
	}
	/*	if ok, err := enc.Verify(shardsRaw); !ok || err != nil {
		log.Println("Verification failed after reconstruction, data likely corrupted, ", err)
		return -1
	}*/
	//}
	buf := new(bytes.Buffer)
	if err := enc.Join(buf, shardsRaw, dataSize); err != nil {
		log.Println("Error decode data, ", err)
		return -1
	}
	dataRet := unsafe.Slice((*byte)(data), dataSize)
	copy(dataRet, buf.Bytes())
	// *data = C.CBytes(buf.Bytes())
	// *data = unsafe.Pointer(&dataGo[0])
	// decodeBuffer.Set(Integer(id), dataGo)
	return 0
}

//export decodeCleanup
func decodeCleanup(id int, data unsafe.Pointer) {
	// C.free(data)
}

func main() {}
