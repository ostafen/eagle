package util

import "sync/atomic"

type AtomicUint64 struct {
	value uint64
}

func (i *AtomicUint64) Inc() uint64 {
	return atomic.AddUint64(&i.value, 1)
}

func (i *AtomicUint64) Set(value uint64) {
	atomic.StoreUint64(&i.value, value)
}

func (i *AtomicUint64) Get() uint64 {
	return atomic.LoadUint64(&i.value)
}

type AtomicInt32 struct {
	value int32
}

func (i *AtomicInt32) Add(value int32) int32 {
	return atomic.AddInt32(&i.value, value)
}

func (i *AtomicInt32) Inc() int32 {
	return i.Add(1)
}

func (i *AtomicInt32) Set(value int32) {
	atomic.StoreInt32(&i.value, value)
}

func (i *AtomicInt32) Get() int32 {
	return atomic.LoadInt32(&i.value)
}

func (i *AtomicInt32) CompareAndSwap(old int32, new int32) bool {
	return atomic.CompareAndSwapInt32(&i.value, old, new)
}

type AtomicUint32 struct {
	value uint32
}

func (i *AtomicUint32) Inc() uint32 {
	return atomic.AddUint32(&i.value, 1)
}

func (i *AtomicUint32) Set(value uint32) {
	atomic.StoreUint32(&i.value, uint32(value))
}

func (i *AtomicUint32) Get() uint32 {
	return atomic.LoadUint32(&i.value)
}

func (i *AtomicUint32) CompareAndSwap(old uint32, new uint32) bool {
	return atomic.CompareAndSwapUint32(&i.value, old, new)
}

type AtomicBool struct {
	value AtomicUint32
}

func (i *AtomicBool) Set() {
	i.value.Set(1)
}

func (i *AtomicBool) Clear() {
	i.value.Set(0)
}

func (i *AtomicBool) Get() bool {
	return i.value.Get() > 0
}

func boolToInt(v bool) uint32 {
	if v {
		return 1
	}
	return 0
}

func (i *AtomicBool) CompareAndSwap(old bool, new bool) bool {
	return i.value.CompareAndSwap(boolToInt(old), boolToInt(new))
}
