package raft

type SendBuffer struct {
	// the starting index in the buffer
	start int
	// number of SendBuffer in the buffer
	count int

	// the size of the buffer
	size int

	// buffer contains the index of the last entry
	// inside one message.
	buffer []uint64
}

func NewSendBuffer(size int) *SendBuffer {
	return &SendBuffer{
		size: size,
	}
}

// add adds an inflight into SendBuffer
func (in *SendBuffer) Add(inflight uint64) {
	if in.Full() {
		panic("cannot add into a full SendBuffer")
	}
	next := in.start + in.count
	size := in.size
	if next >= size {
		next -= size
	}
	// 按需分配
	if next >= len(in.buffer) {
		in.growBuf()
	}
	in.buffer[next] = inflight
	in.count++
}

// grow the inflight buffer by doubling up to SendBuffer.size. We grow on demand
// instead of preallocating to SendBuffer.size to handle systems which have
// thousands of Raft groups per process.
func (in *SendBuffer) growBuf() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// freeTo frees the SendBuffer smaller or equal to the given `to` flight.
func (in *SendBuffer) FreeTo(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] {
		// out of the left side of the window
		return
	}

	i, idx := 0, in.start
	for i = 0; i < in.count; i++ {
		if to < in.buffer[idx] { // found the first large inflight
			break
		}

		// increase index and maybe rotate
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}
	// free i SendBuffer and set new start index
	in.count -= i
	in.start = idx
	if in.count == 0 {
		// SendBuffer is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}

func (in *SendBuffer) FreeFirstOne() { in.FreeTo(in.buffer[in.start]) }

// full returns true if the SendBuffer is full.
func (in *SendBuffer) Full() bool {
	return in.count == in.size
}

// resets frees all SendBuffer.
func (in *SendBuffer) Reset() {
	in.count = 0
	in.start = 0
}
