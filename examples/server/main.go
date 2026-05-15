package main

import (
	"fmt"
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

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		panic(err)
	}

	fmt.Println("DKV server listening on :50051...")
	s := dkv.NewServer(eng)
	if err := s.Run(listener); err != nil {
		panic(err)
	}
	defer s.Stop()
}
