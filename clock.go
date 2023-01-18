package kv_bitcask

import "time"

type Clock interface {
	Now() time.Time
}

var _ Clock = new(RealClock)

type RealClock struct {
}

func NewRealClock() Clock {
	return &RealClock{}
}

func (r *RealClock) Now() time.Time {
	return time.Now()
}
