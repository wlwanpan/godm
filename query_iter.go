package godm

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
)

type modelFactoryFunc func(string) Model

var (
	newModelFunc modelFactoryFunc
)

func RegisterModelFactoryFunc(modelFunc modelFactoryFunc) {
	newModelFunc = modelFunc
}

type QueryIter struct {
	cur *mongo.Cursor
	m   Model
	err error
}

func (iter *QueryIter) Collection() string {
	return iter.m.Collection()
}

func (iter *QueryIter) Iter(ctx context.Context) <-chan Model {
	ch := make(chan Model)

	go func() {
		defer iter.cur.Close(ctx)
		defer close(ch)

		for iter.cur.Next(ctx) {
			select {
			case <-ctx.Done():
				return
			default:
				m := newModelFunc(iter.Collection())
				if err := iter.cur.Decode(m); err != nil {
					log.Println(err)
					iter.err = err
					continue
				}
				ch <- m
			}
		}
	}()

	return ch
}

func (iter *QueryIter) Close(ctx context.Context) error {
	return iter.cur.Close(ctx)
}

func (iter *QueryIter) Err() error {
	return iter.err
}
