package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key     string
	id      string
	version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:  ck,
		key: l,
		id:  kvtest.RandValue(8),
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	var v rpc.Tversion
	free := false
	locked := false

	// loop until acquired
	for !locked {
		val, ver, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			// no lock, free to make
			v = 0
			free = true
		}
		if err == rpc.OK && val == "" {
			// released lock, free to take
			v = ver
			free = true
		}
		if err == rpc.OK && val == lk.id {
			// already got it!
			lk.version = ver
			locked = true
		}

		if free {
			err = lk.ck.Put(lk.key, lk.id, v)
			switch err {
			case rpc.OK:
				lk.version = v + 1
				locked = true
			case rpc.ErrVersion, rpc.ErrMaybe:
				// log.Printf("%s: error acquiring the lock, some one was faster? %+v", lk.id, err)
				free = false
			}
		}
		time.Sleep(time.Millisecond * 50)
	}
}

func (lk *Lock) Release() {
	rt := 0
	for {
		err := lk.ck.Put(lk.key, "", lk.version)

		switch err {
		case rpc.OK, rpc.ErrNoKey, rpc.ErrMaybe:
			return
		case rpc.ErrVersion:
			log.Printf("%s: error releasing the lock (%d) v%+v: %+v", lk.id, rt, lk.version, err)
		}
		rt++
		time.Sleep(time.Millisecond * 50)
	}
}
