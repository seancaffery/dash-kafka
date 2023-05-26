package kafka

import (
	"context"
	"sync"
)

var QUEUE_SIZE int = 1000

type concurrentProcessor struct {
	ctx           context.Context
	workerCount   int
	queue         chan *work
	serialization chan *ConsumerMessage
	errChan       chan error
	workers       []*worker
	wg            sync.WaitGroup
	processor     MessageProcessor
}

type work struct {
	ctx     context.Context
	message *ConsumerMessage
	err     error
}

type worker struct {
	messages    <-chan *work
	wg          *sync.WaitGroup
	processFunc MessageProcessorFunc
}

func newConcurrentProcessor(ctx context.Context, workerCount int, processor MessageProcessor) *concurrentProcessor {
	return &concurrentProcessor{
		ctx:           ctx,
		workerCount:   workerCount,
		errChan:       make(chan error, QUEUE_SIZE),
		queue:         make(chan *work, QUEUE_SIZE),
		serialization: make(chan *ConsumerMessage, QUEUE_SIZE),
		processor:     processor,
	}
}

func (p *concurrentProcessor) Start(ctx context.Context) error {
	for i := 0; i < p.workerCount; i++ {
		worker := newWorker(p.queue, &p.wg, p.processor.ProcessMessage)
		p.workers = append(p.workers, worker)
		p.wg.Add(1)
		go func() {
			worker.start(ctx)
		}()
	}
	return nil
}

func (p *concurrentProcessor) Shutdown() error {
	return nil
}

func (p *concurrentProcessor) Serialization() chan *ConsumerMessage {
	return p.serialization
}

func (p *concurrentProcessor) Enqueue(ctx context.Context, message *ConsumerMessage) error {
	w := &work{
		ctx:     ctx,
		message: message,
	}

	select {
	case <-ctx.Done():
		return nil
	case <-p.ctx.Done():
		return nil
	default:
	}

	p.serialization <- message
	p.queue <- w
	return nil
}

func newWorker(messages <-chan *work, wg *sync.WaitGroup, processFunc MessageProcessorFunc) *worker {
	return &worker{
		wg:          wg,
		messages:    messages,
		processFunc: processFunc,
	}
}

func (worker *worker) start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			worker.wg.Done()
			return nil
		case message := <-worker.messages:
			err := worker.process(ctx, *message)
			if err != nil {
				worker.wg.Done()
				return err
			}
		}
	}
}

func (worker *worker) process(ctx context.Context, work work) error {
	if ctx.Err() != nil {
		return nil
	}
	err := worker.processFunc(work.ctx, *work.message)

	if err != nil {
		return err
	}

	close(work.message.Err)
	return err
}
