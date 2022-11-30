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

func init() {
	engines = cmap.NewStringer[Integer, reedsolomon.Encoder]()
	encodeBuffer = cmap.NewStringer[Integer, [][]byte]()
	maxIndex = 0
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
func encodeNext(id int, shards *[]unsafe.Pointer) int {
	goShard, _ := encodeBuffer.Get(Integer(id))
	for k, v := range goShard {
		(*shards)[k] = C.CBytes(v)
	}
	return 0
}

//export encodeCleanup
func encodeCleanup(id int, shards *[]unsafe.Pointer) {
	for i := 0; i < len(*shards); i++ {
		C.free((*shards)[i])
	}
}

// shards[i] = nil in order
//
//export decode
func decode(id int, shards []unsafe.Pointer, fLength int, dataSize int, data *unsafe.Pointer) int {
	enc, suc := engines.Get(Integer(id))
	if !suc {
		return -1
	}
	shardsRaw := make([][]byte, len(shards))
	for i := 0; i < len(shards); i++ {
		if shards[i] != nil {
			dataRaw := unsafe.Slice((*byte)(shards[i]), fLength)
			shardsRaw[i] = dataRaw
		}
	}
	// Verify the shards
	if ok, err := enc.Verify(shardsRaw); !ok || err != nil {
		// log.Println("Verification failed. Reconstructing data, ", err)
		if err := enc.Reconstruct(shardsRaw); err != nil {
			log.Println("Reconstruct failed, ", err)
			return -1
		}
		if ok, err := enc.Verify(shardsRaw); !ok || err != nil {
			log.Println("Verification failed after reconstruction, data likely corrupted, ", err)
			return -1
		}
	}
	buf := new(bytes.Buffer)
	if err := enc.Join(buf, shardsRaw, dataSize); err != nil {
		log.Println("Error decode data, ", err)
		return -1
	}
	*data = C.CBytes(buf.Bytes())
	return 0
}

//export decodeCleanup
func decodeCleanup(id int, data *unsafe.Pointer) {
	C.free(*data)
}

func main() {}
