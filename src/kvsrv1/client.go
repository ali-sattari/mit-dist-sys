package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	req := rpc.GetArgs{Key: key}
	resp := rpc.GetReply{}

	retries := 0
	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &req, &resp)
		if ok {
			break
		}

		retries++
		time.Sleep(time.Millisecond * 10)
	}

	return resp.Value, resp.Version, resp.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	req := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	resp := rpc.PutReply{}

	retries := 0
	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &req, &resp)
		if ok {
			switch resp.Err {
			case rpc.OK:
				return rpc.OK
			case rpc.ErrVersion:
				if retries == 0 {
					return rpc.ErrVersion
				} else {
					DPrintf("client.Put: undeterminable error (%d): %+v for %v", retries, resp, req)
					return rpc.ErrMaybe
				}
			case rpc.ErrNoKey:
				return rpc.ErrNoKey
			}
		}

		retries++
		time.Sleep(time.Millisecond * 20)
	}
}
