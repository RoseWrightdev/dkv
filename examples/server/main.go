package main

import (
	"net"
	"time"

	"github.com/rosewrightdev/dkv"
)

func main() {
	eng, err := dkv.NewEngineBuilder().
		Default().
		SetSssPath("sss.json").
		SetWalPath("wal.binpb").
		SetSssInterval(3 * time.Minute).
		SetWalSyncInterval(500 * time.Microsecond).
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
