package server

import (
	"context"
	"log/slog"
	"net"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/core"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedDkvServiceServer
	eng *core.Engine
}

func (s *server) Get(_ context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	val, ok := s.eng.Get(in.GetKey())
	if !ok {
		return &pb.GetResponse{Value: nil, Exists: ok}, nil
	}
	return &pb.GetResponse{Value: val, Exists: ok}, nil
}

func (s *server) Set(_ context.Context, in *pb.SetRequest) (*pb.SetResponse, error) {
	err := s.eng.Wal.Publish(in)
	if err != nil {
		return &pb.SetResponse{}, err
	}
	s.eng.Set(in.Key, in.Value)
	return &pb.SetResponse{}, nil
}

func (s *server) Delete(_ context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	err := s.eng.Wal.Publish(in)
	if err != nil {
		return &pb.DeleteResponse{}, err
	}
	s.eng.Delete(in.Key)
	return &pb.DeleteResponse{}, nil
}

type Grpc struct {
	inner    *grpc.Server
	handlers *server
}

func NewGrpc(eng *core.Engine) *Grpc {
	s := grpc.NewServer()
	h := &server{eng: eng}
	pb.RegisterDkvServiceServer(s, h)
	return &Grpc{inner: s, handlers: h}
}

func (s *Grpc) Run(listener net.Listener) error {
	slog.Info("Grpc server running on " + listener.Addr().String())
	err := s.inner.Serve(listener)
	return err
}

func (s *Grpc) Stop() {
	s.handlers.eng.Stop()
	s.inner.GracefulStop()
}
