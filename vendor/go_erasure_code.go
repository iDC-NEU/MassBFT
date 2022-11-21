// Copyright 2015, Klaus Post, see LICENSE for details.
//
// Simple decoder example.
//
// The decoder reverses the process of "simple-encoder.go"
//
// To build an executable use:
//
// go build simple-decoder.go
//
// Simple Encoder/Decoder Shortcomings:
// * If the file size of the input isn't divisible by the number of data shards
//   the output will contain extra zeroes
//
// * If the shard numbers isn't the same for the decoder as in the
//   encoder, invalid output will be generated.
//
// * If values have changed in a shard, it cannot be reconstructed.
//
// * If two shards have been swapped, reconstruction will always fail.
//   You need to supply the shards in the same order as they were given to you.
//
// The solution for this is to save a metadata file containing:
//
// * File size.
// * The number of data/parity shards.
// * HASH of each shard.
// * Order of the shards.
//
// If you save these properties, you should abe able to detect file corruption
// in a shard and be able to reconstruct your data if you have the needed number of shards left.

package main

import (
	"C"
	"bytes"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"github.com/orcaman/concurrent-map/v2"
	"log"
	"strconv"
	"sync/atomic"
)

type Integer int64

func (i Integer) String() string {
	return strconv.Itoa(int(i))
}

var encoders cmap.ConcurrentMap[Integer, reedsolomon.Encoder]
var maxIndex int64

func init() {
	encoders = cmap.NewStringer[Integer, reedsolomon.Encoder]()
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
	for atomic.CompareAndSwapInt64(&maxIndex, old, maxIndex+1) {
		old = maxIndex
	}
	encoders.Set(Integer(old), instance)
	return int(old)
}

//export instanceDestroy
func instanceDestroy(id int) int {
	encoders.Remove(Integer(id))
	return 0
}

//export encode
func encode(id int, data []byte, shards *[][]byte, fragmentLen *int) int {
	enc, suc := encoders.Get(Integer(id))
	if !suc {
		return -1
	}
	shardsReal, err := enc.Split(data)
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
	*shards = shardsReal
	return 0
}

//export encodeCleanup
func encodeCleanup(id int, shards *[][]byte) {
	*shards = nil
}

// shards[i] = nil in order
//
//export decode
func decode(id int, shards [][]byte, dataSize int, data *[]byte) int {
	enc, suc := encoders.Get(Integer(id))
	if !suc {
		return -1
	}
	// Verify the shards
	ok, err := enc.Verify(shards)
	if err != nil {
		log.Println("Error verify data")
		return -1
	}
	if !ok {
		log.Println("Verification failed. Reconstructing data")
		err := enc.Reconstruct(shards)
		if err != nil {
			fmt.Println("Reconstruct failed -", err)
			return -1
		}
		ok, err := enc.Verify(shards)
		if !ok {
			fmt.Println("Verification failed after reconstruction, data likely corrupted.")
			return -1
		}
		if err != nil {
			log.Println("Error decode data")
			return -1
		}
	}
	buf := new(bytes.Buffer)
	err = enc.Join(buf, shards, dataSize)
	*data = buf.Bytes()
	return 0
}

//export decodeCleanup
func decodeCleanup(id int, data *[]byte) {
	*data = nil
}

func main() {}
