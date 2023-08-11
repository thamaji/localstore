package localstore

import "io"

type Options[T any] struct {
	Ext        string
	Comparator Comparator
	Encoder    Encoder[T]
	Decoder    Decoder[T]
}

type Encoder[T any] interface {
	Encode(w io.Writer, value T) error
}

type EncoderFunc[T any] func(w io.Writer, value T) error

func (f EncoderFunc[T]) Encode(w io.Writer, value T) error {
	return f(w, value)
}

type Decoder[T any] interface {
	Decode(r io.Reader) (T, error)
}

type DecoderFunc[T any] func(r io.Reader) (T, error)

func (f DecoderFunc[T]) Decode(r io.Reader) (T, error) {
	return f(r)
}

type Comparator interface {
	Compare(key1, key2 string) int
}

type ComparatorFunc func(key1, key2 string) int

func (f ComparatorFunc) Compare(key1, key2 string) int {
	return f(key1, key2)
}
