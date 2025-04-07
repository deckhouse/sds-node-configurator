/*
Copyright 2025 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package throttler

import (
	"sync"
	"time"
)

type Throttler interface {
	Do(f func())
}

type throttle struct {
	duration time.Duration
	once     sync.Once
	m        sync.Mutex
}

func (t *throttle) Do(f func()) {
	t.m.Lock()
	defer t.m.Unlock()
	t.once.Do(func() {
		go func() {
			time.Sleep(t.duration)
			t.m.Lock()
			defer t.m.Unlock()
			t.once = sync.Once{}
		}()
		f()
	})
}

func New(duration time.Duration) Throttler {
	return &throttle{
		duration: duration,
	}
}
