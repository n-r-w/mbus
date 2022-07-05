// package mbus
// based on https://github.com/vardius/message-bus
package mbus

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type TopicID string

type handlersMap map[TopicID][]*handler

type handler struct {
	callback reflect.Value
	queue    chan []reflect.Value
	wg       sync.WaitGroup
	counter  int32
}

type MessageBus struct {
	parallelCall     bool
	handlerQueueSize int
	mtx              sync.RWMutex
	handlers         handlersMap
}

// New creates new MessageBus
// handlerQueueSize sets buffered channel length per subscriber
// parallelCall вызов функции обработчика для каждого подписчика в отдельной горутине не блокируя его
func New(parallelCall bool, handlerQueueSize int) *MessageBus {
	if handlerQueueSize == 0 {
		panic("handlerQueueSize has to be greater then 0")
	}

	return &MessageBus{
		parallelCall:     parallelCall,
		handlerQueueSize: handlerQueueSize,
		mtx:              sync.RWMutex{},
		handlers:         make(handlersMap),
	}
}

// Publish publishes arguments to the given topic subscribers
// Publish block only when the buffer of one of the subscribers is full.
func (b *MessageBus) Publish(topic TopicID, args ...interface{}) {
	if len(topic) == 0 {
		panic("topic name is empty")
	}

	rArgs := buildHandlerArgs(args)

	b.mtx.RLock()
	defer b.mtx.RUnlock()

	if hs, ok := b.handlers[topic]; ok {
		for _, h := range hs {
			h.queue <- rArgs
		}
	}
}

// Subscribe subscribes to the given topic
func (b *MessageBus) Subscribe(topic TopicID, fn interface{}) error {
	if len(topic) == 0 {
		panic("topic name is empty")
	}

	if err := isValidHandler(fn); err != nil {
		return err
	}

	h := &handler{
		callback: reflect.ValueOf(fn),
		queue:    make(chan []reflect.Value, b.handlerQueueSize),
		wg:       sync.WaitGroup{},
	}

	go func() {
		for args := range h.queue {
			atomic.AddInt32(&h.counter, 1)
			h.wg.Add(1)
			if b.parallelCall {
				go func(targs []reflect.Value) {
					defer h.wg.Done()
					defer atomic.AddInt32(&h.counter, -1)
					h.callback.Call(targs)
				}(args)

			} else {
				h.callback.Call(args)
				h.wg.Done()
				atomic.AddInt32(&h.counter, -1)
			}
		}
	}()

	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.handlers[topic] = append(b.handlers[topic], h)

	return nil
}

// Unsubscribe unsubscribe handler from the given topic
func (b *MessageBus) Unsubscribe(topic TopicID, fn interface{}) error {
	if len(topic) == 0 {
		panic("topic name is empty")
	}

	if err := isValidHandler(fn); err != nil {
		return err
	}

	rv := reflect.ValueOf(fn)

	b.mtx.Lock()
	defer b.mtx.Unlock()

	if _, ok := b.handlers[topic]; ok {
		for i, h := range b.handlers[topic] {
			if h.callback == rv {
				close(h.queue)

				if len(b.handlers[topic]) == 1 {
					delete(b.handlers, topic)
				} else {
					b.handlers[topic] = append(b.handlers[topic][:i], b.handlers[topic][i+1:]...)
				}
			}
		}

		return nil
	}

	return fmt.Errorf("topic %s doesn't exist", topic)
}

// Close unsubscribe all handlers from given topic
func (b *MessageBus) Close(topic TopicID) {
	if len(topic) == 0 {
		panic("topic name is empty")
	}

	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.closeHelper(topic)
}

func (b *MessageBus) closeHelper(topic TopicID) {
	if _, ok := b.handlers[topic]; ok {
		for _, h := range b.handlers[topic] {
			close(h.queue)
		}

		delete(b.handlers, topic)

		return
	}
}

// BufferSize размер очереди на обработку для сбора статистики
// Не учитывает текущее количество горутин выполняемых обработчиков, только размер буфера каналов
func (b *MessageBus) BufferSize(topic TopicID) int {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	n := 0
	if _, ok := b.handlers[topic]; ok {
		for _, h := range b.handlers[topic] {
			n += len(h.queue) + int(atomic.LoadInt32(&h.counter))
		}
	}
	return n
}

// Закрыть топик, предварительно дождавшись завершения всех обработчиков и освобождения очередей
func (b *MessageBus) WaitAndClose(topic TopicID) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.waitAndCloseHelper(topic)
}

func (b *MessageBus) waitAndCloseHelper(topic TopicID) {
	if _, ok := b.handlers[topic]; ok {
		for _, h := range b.handlers[topic] {
			for len(h.queue) > 0 {
				time.Sleep(time.Millisecond)
			}

			h.wg.Wait()
		}
		b.closeHelper(topic)
	}
}

// Закрыть все топики, предварительно дождавшись завершения всех обработчиков и освобождения очередей
func (b *MessageBus) WaitAllAndClose() {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	allTopics := make([]TopicID, len(b.handlers))
	for topic := range b.handlers {
		allTopics = append(allTopics, topic)
	}

	for _, t := range allTopics {
		b.waitAndCloseHelper(t)
	}
}

func isValidHandler(fn interface{}) error {
	if reflect.TypeOf(fn).Kind() != reflect.Func {
		return fmt.Errorf("%s is not a reflect.Func", reflect.TypeOf(fn))
	}

	return nil
}

func buildHandlerArgs(args []interface{}) []reflect.Value {
	reflectedArgs := make([]reflect.Value, 0)

	for _, arg := range args {
		reflectedArgs = append(reflectedArgs, reflect.ValueOf(arg))
	}

	return reflectedArgs
}
