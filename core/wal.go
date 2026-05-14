package core

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/protobuf/proto"
)

type Waler interface {
	Publish(msg proto.Message) error
	Replay() (*map[Key]Value, error)
	clear() error
}

type Wal struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	wrt    *bufio.Writer
	file   *os.File
	path   string
}

func newWal(path string) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	wal := &Wal{
		ctx:    ctx,
		cancel: cancel,
		mu:     sync.Mutex{},
		wrt:    bufio.NewWriter(file),
		file:   file,
		path:   path,
	}

	return wal, nil
}

func (w *Wal) backgroundSync() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.Sync()
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *Wal) Publish(msg proto.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var entry *pb.WalEntry
	switch m := msg.(type) {
	case *pb.WalEntry:
		entry = m
	case *pb.SetRequest:
		entry = &pb.WalEntry{Entry: &pb.WalEntry_Set{Set: m}}
	case *pb.DeleteRequest:
		entry = &pb.WalEntry{Entry: &pb.WalEntry_Delete{Delete: m}}
	default:
		return fmt.Errorf("unsupported message type: %T", msg)
	}

	data, err := proto.Marshal(entry)
	if err != nil {
		return err
	}

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	if _, err := w.wrt.Write(header); err != nil {
		return err
	}

	if _, err := w.wrt.Write(data); err != nil {
		return err
	}

	return nil
}

func (w *Wal) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.wrt.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *Wal) Start() {
	go w.backgroundSync()
}

func (w *Wal) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cancel()
	w.wrt.Flush()
	w.file.Sync()
	w.file.Close()
}

func (w *Wal) Replay() (*map[Key]Value, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.wrt.Flush(); err != nil {
		return nil, err
	}

	results := make(map[Key]Value)

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(w.file)
	for {
		header := make([]byte, 4)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		size := binary.BigEndian.Uint32(header)
		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, err
		}

		var entry pb.WalEntry
		if err := proto.Unmarshal(payload, &entry); err != nil {
			return nil, err
		}

		switch kv := entry.Entry.(type) {
		case *pb.WalEntry_Set:
			results[kv.Set.Key] = kv.Set.Value
		case *pb.WalEntry_Delete:
			delete(results, kv.Delete.Key)
		}
	}
	return &results, nil
}

func (w *Wal) clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.wrt.Reset(w.file)
	return nil
}
