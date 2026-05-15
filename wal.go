package dkv

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
	publish(msg proto.Message) error
	replay() (map[Key]Value, error)
	clear() error
	start()
	stop()
}

type Wal struct {
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	syncInterval time.Duration
	wrt          *bufio.Writer
	file         *os.File
	path         string
	headerPool   sync.Pool
	entryPool    sync.Pool
	bufferPool   sync.Pool
}

func newWal(path string, syncInterval time.Duration, bufferSize uint32) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	wal := &Wal{
		ctx:          ctx,
		cancel:       cancel,
		mu:           sync.Mutex{},
		syncInterval: syncInterval,
		wrt:          bufio.NewWriterSize(file, int(bufferSize)),
		file:         file,
		path:         path,
		headerPool: sync.Pool{
			New: func() any {
				b := make([]byte, 4)
				return &b
			},
		},
		entryPool: sync.Pool{
			New: func() any {
				return &pb.WalEntry{}
			},
		},
		bufferPool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, 2048)
				return &b
			},
		},
	}

	return wal, nil
}

func (w *Wal) start() {
	go w.backgroundSync()
}

func (w *Wal) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cancel()
	w.wrt.Flush()
	w.file.Sync()
	w.file.Close()
}

func (w *Wal) publish(msg proto.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := w.entryPool.Get().(*pb.WalEntry)
	defer w.entryPool.Put(entry)

	switch m := msg.(type) {
	case *pb.WalEntry:
		entry.Entry = m.Entry
	case *pb.SetRequest:
		if wrapper, ok := entry.Entry.(*pb.WalEntry_Set); ok {
			wrapper.Set = m
		} else {
			entry.Entry = &pb.WalEntry_Set{Set: m}
		}
	case *pb.DeleteRequest:
		if wrapper, ok := entry.Entry.(*pb.WalEntry_Delete); ok {
			wrapper.Delete = m
		} else {
			entry.Entry = &pb.WalEntry_Delete{Delete: m}
		}
	default:
		return fmt.Errorf("unsupported message type: %T", msg)
	}

	bufPtr := w.bufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	data, err := proto.MarshalOptions{}.MarshalAppend(buf, entry)
	if err != nil {
		w.bufferPool.Put(bufPtr)
		return err
	}
	*bufPtr = data
	defer w.bufferPool.Put(bufPtr)

	headerPtr := w.headerPool.Get().(*[]byte)
	header := *headerPtr
	defer w.headerPool.Put(headerPtr)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	if _, err := w.wrt.Write(header); err != nil {
		return err
	}

	if _, err := w.wrt.Write(data); err != nil {
		return err
	}

	return nil
}

func (w *Wal) sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.wrt.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *Wal) backgroundSync() {
	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.sync()
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *Wal) replay() (map[Key]Value, error) {
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
			results[kv.Delete.Key] = nil
		}
	}

	// Seek back to end for future writes
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return results, nil
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
