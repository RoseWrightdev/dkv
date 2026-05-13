package core

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/protobuf/proto"
)

type Waler interface {
	Publish(msg proto.Message) error
	Replay() (*map[Key]Value, error)
	flush() error
}

type Wal struct {
	file *os.File
	path string
	mu   sync.Mutex
	wrt  *bufio.Writer
}

func newWal(path string) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &Wal{
		file: file,
		path: path,
		mu:   sync.Mutex{},
		wrt:  bufio.NewWriter(file),
	}, nil
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

func (w *Wal) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
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

func (w *Wal) flush() error {
	return w.file.Truncate(0)
}
