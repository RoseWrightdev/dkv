package main

import (
	"net"
	"time"

	"github.com/rosewrightdev/dkv"
)

func main() {
	eng, err := dkv.NewEngineBuilder().
		SetSssInterval(time.Duration(3) * time.Minute).
		SetWalSyncInterval(time.Duration(500) * time.Microsecond).
		SetSssPath("sss.json").
		SetWalPath("wal.binpb").
		SetWalBufferSize(64 * 1028).
		SetEvictionService(dkv.NewLRU(64*1028, time.Duration(5)*time.Minute)).
		GetEngine()
	if err != nil {
		panic(err)
	}

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}

	s := dkv.NewServer(eng)
	err = s.Run(listener)
	if err != nil {
		panic(err)
	}
	defer s.Stop()
}
