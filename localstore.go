package localstore

import (
	"encoding/gob"
	"io"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/thamaji/files"
)

func New[T any](dir string, opt *Options[T]) *LocalStore[T] {
	s := &LocalStore[T]{
		dir:        dir,
		ext:        ".dat",
		comparator: ComparatorFunc(strings.Compare),
		encoder: EncoderFunc[T](func(w io.Writer, v T) error {
			return gob.NewEncoder(w).Encode(v)
		}),
		decoder: DecoderFunc[T](func(r io.Reader) (T, error) {
			value := *new(T)
			err := gob.NewDecoder(r).Decode(&value)
			return value, err
		}),
	}
	if opt != nil {
		if opt.Ext != "" {
			s.ext = opt.Ext
		}
		if opt.Comparator != nil {
			s.comparator = opt.Comparator
		}
		if opt.Encoder != nil {
			s.encoder = opt.Encoder
		}
		if opt.Decoder != nil {
			s.decoder = opt.Decoder
		}
	}
	return s
}

type LocalStore[T any] struct {
	mutex      sync.RWMutex
	dir        string
	ext        string
	comparator Comparator
	encoder    Encoder[T]
	decoder    Decoder[T]
	index      []string
}

type List[T any] struct {
	Values []T
	Offset int
	Limit  int
	Total  int
}

func (s *LocalStore[T]) Load() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	names := files.MustReadDirnames(s.dir)
	index := make([]string, 0, len(names))
	for _, name := range names {
		if filepath.Ext(name) != s.ext {
			continue
		}
		index = append(index, name)
	}
	sort.Slice(index, func(i, j int) bool {
		return s.comparator.Compare(index[i], index[j]) < 0
	})
	s.index = index

	return nil
}

func (s *LocalStore[T]) List(offset int, limit int) (List[T], error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	cap := limit
	if cap < 0 {
		cap = len(s.index)
	}

	values := make([]T, 0, cap)

	for i := offset; i < cap; i++ {
		f, err := files.Open(filepath.Join(s.dir, s.index[i]))
		if err != nil {
			return List[T]{}, err
		}

		fi, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return List[T]{}, err
		}
		if fi.IsDir() {
			_ = f.Close()
			continue
		}

		value, err := s.decoder.Decode(f)
		_ = f.Close()
		if err != nil {
			return List[T]{}, err
		}

		values = append(values, value)
	}

	list := List[T]{
		Values: values,
		Offset: offset,
		Limit:  limit,
		Total:  len(s.index),
	}

	return list, nil
}

func (s *LocalStore[T]) Get(key string) (T, error) {
	name := url.PathEscape(key) + s.ext
	path := filepath.Join(s.dir, name)

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	_, ok := sort.Find(len(s.index), func(i int) int {
		return s.comparator.Compare(name, s.index[i])
	})
	if !ok {
		return *new(T), ErrNotExist
	}

	f, err := files.OpenFileReader(path)
	if err != nil {
		return *new(T), err
	}
	value, err := s.decoder.Decode(f)
	_ = f.Close()
	if err != nil {
		return value, err
	}

	return value, nil
}

func (s *LocalStore[T]) Put(key string, value T) error {
	name := url.PathEscape(key) + s.ext
	path := filepath.Join(s.dir, name)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	f, err := files.OpenFileWriter(path)
	if err != nil {
		return err
	}
	err = s.encoder.Encode(f, value)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	if err != nil {
		return err
	}

	i, ok := sort.Find(len(s.index), func(i int) int {
		return s.comparator.Compare(name, s.index[i])
	})
	if !ok {
		s.index = append(s.index[:i], append([]string{name}, s.index[i:]...)...)
	}

	return nil
}

func (s *LocalStore[T]) Delete(key string) error {
	name := url.PathEscape(key) + s.ext
	path := filepath.Join(s.dir, name)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	i, ok := sort.Find(len(s.index), func(i int) int {
		return s.comparator.Compare(name, s.index[i])
	})
	if !ok {
		return nil
	}

	if err := files.Remove(path); err != nil {
		return err
	}
	if files.MustIsEmptyDir(path) {
		_ = files.Remove(path)
	}

	s.index = append(s.index[:i], s.index[i+1:]...)

	return nil
}
