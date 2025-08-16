package health

import "sync"

type ringBuffer struct {
	data  []bool
	pos   int
	count int
	mu    sync.Mutex
}

func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{
		data: make([]bool, size),
	}
}

func (r *ringBuffer) Add(success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.data[r.pos] = success
	r.pos = (r.pos + 1) % len(r.data)
	if r.count < len(r.data) {
		r.count++
	}
}

func (r *ringBuffer) SuccessRate() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 {
		return 1.0 // No data = assume healthy
	}

	successes := 0
	for i := 0; i < r.count; i++ {
		if r.data[i] {
			successes++
		}
	}

	return float64(successes) / float64(r.count)
}

func (r *ringBuffer) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}
