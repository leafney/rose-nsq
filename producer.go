/**
 * @Author:      leafney
 * @Date:        2023-03-15 23:47
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package rnsq

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

type BaseProducer interface {
	Publish(data string) error
	PublishB(data []byte) error
	PublishWithTopic(topic, data string) error
	PublishBWithTopic(topic string, data []byte) error
	PublishAsync(data string) error
	PublishBAsync(data []byte) error
	PublishAsyncWithTopic(topic, data string) error
	PublishBAsyncWithTopic(topic string, data []byte) error
	PublishAsyncWithChan(data string, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error
	PublishBAsyncWithChan(data []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error
	PublishAsyncWithChanWithTopic(topic, data string, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error
	PublishBAsyncWithChanWithTopic(topic string, data []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error
	PublishDelay(data string, delay time.Duration) error
	PublishBDelay(data []byte, delay time.Duration) error
	PublishDelayWithTopic(topic, data string, delay time.Duration) error
	PublishBDelayWithTopic(topic string, data []byte, delay time.Duration) error
	PublishDelayAsync(data string, delay time.Duration) error
	PublishBDelayAsync(data []byte, delay time.Duration) error
	PublishDelayAsyncWithChan(data string, delay time.Duration, doneChan chan *nsq.ProducerTransaction,
		args ...interface{}) error
	PublishBDelayAsyncWithChan(data []byte, delay time.Duration, doneChan chan *nsq.ProducerTransaction,
		args ...interface{}) error
	PublishDelayAsyncWithChanWithTopic(topic, data string, delay time.Duration, doneChan chan *nsq.ProducerTransaction,
		args ...interface{}) error
	PublishBDelayAsyncWithChanWithTopic(topic string, data []byte, delay time.Duration, doneChan chan *nsq.ProducerTransaction,
		args ...interface{}) error
	PublishMulti(messages [][]byte) error
	PublishMultiWithTopic(topic string, messages [][]byte) error
	PublishMultiAsync(messages [][]byte) error
	PublishMultiAsyncWithTopic(topic string, messages [][]byte) error
	PublishMultiAsyncWithChan(messages [][]byte, doneChan chan *nsq.ProducerTransaction,
		args ...interface{}) error
	PublishMultiAsyncWithChanWithTopic(topic string, messages [][]byte, doneChan chan *nsq.ProducerTransaction,
		args ...interface{}) error

	Stop()
}

type defBaseProducer struct {
	producer *nsq.Producer
	topic    string
	mutex    sync.Mutex
}

// --------------------

func (p *defBaseProducer) Publish(data string) error {
	return p.PublishWithTopic(p.topic, data)
}

func (p *defBaseProducer) PublishB(data []byte) error {
	return p.PublishBWithTopic(p.topic, data)
}

func (p *defBaseProducer) PublishWithTopic(topic, data string) error {
	return p.PublishBWithTopic(topic, []byte(data))
}

func (p *defBaseProducer) PublishBWithTopic(topic string, data []byte) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.Publish(topic, data)
}

// --------------------

func (p *defBaseProducer) PublishAsync(data string) error {
	return p.PublishBAsync([]byte(data))
}

func (p *defBaseProducer) PublishBAsync(data []byte) error {
	return p.PublishBAsyncWithTopic(p.topic, data)
}

func (p *defBaseProducer) PublishAsyncWithTopic(topic, data string) error {
	return p.PublishBAsyncWithTopic(topic, []byte(data))
}

func (p *defBaseProducer) PublishBAsyncWithTopic(topic string, data []byte) error {
	return p.PublishBAsyncWithChanWithTopic(topic, data, nil)
}

func (p *defBaseProducer) PublishAsyncWithChan(data string, doneChan chan *nsq.ProducerTransaction,
	args ...interface{}) error {
	return p.PublishAsyncWithChanWithTopic(p.topic, data, doneChan, args)
}

func (p *defBaseProducer) PublishBAsyncWithChan(data []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	return p.PublishBAsyncWithChanWithTopic(p.topic, data, doneChan, args)
}

func (p *defBaseProducer) PublishAsyncWithChanWithTopic(topic, data string, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	return p.PublishBAsyncWithChanWithTopic(topic, []byte(data), doneChan, args)
}

func (p *defBaseProducer) PublishBAsyncWithChanWithTopic(topic string, data []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.PublishAsync(topic, data, doneChan, args)
}

// --------------------

func (p *defBaseProducer) PublishDelay(data string, delay time.Duration) error {
	return p.PublishDelayWithTopic(p.topic, data, delay)
}

func (p *defBaseProducer) PublishBDelay(data []byte, delay time.Duration) error {
	return p.PublishBDelayWithTopic(p.topic, data, delay)
}

func (p *defBaseProducer) PublishDelayWithTopic(topic, data string, delay time.Duration) error {
	return p.PublishBDelayWithTopic(topic, []byte(data), delay)
}

func (p *defBaseProducer) PublishBDelayWithTopic(topic string, data []byte, delay time.Duration) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.DeferredPublish(topic, delay, data)
}

// --------------------

func (p *defBaseProducer) PublishDelayAsync(data string, delay time.Duration) error {
	return p.PublishDelayAsyncWithChan(data, delay, nil)
}

func (p *defBaseProducer) PublishBDelayAsync(data []byte, delay time.Duration) error {
	return p.PublishBDelayAsyncWithChan(data, delay, nil)
}

func (p *defBaseProducer) PublishDelayAsyncWithChan(data string, delay time.Duration, doneChan chan *nsq.ProducerTransaction,
	args ...interface{}) error {
	return p.PublishDelayAsyncWithChanWithTopic(p.topic, data, delay, doneChan, args)
}

func (p *defBaseProducer) PublishBDelayAsyncWithChan(data []byte, delay time.Duration, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	return p.PublishBDelayAsyncWithChanWithTopic(p.topic, data, delay, doneChan, args)
}

func (p *defBaseProducer) PublishDelayAsyncWithChanWithTopic(topic, data string, delay time.Duration, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	return p.PublishBDelayAsyncWithChanWithTopic(topic, []byte(data), delay, doneChan, args)
}

func (p *defBaseProducer) PublishBDelayAsyncWithChanWithTopic(topic string, data []byte, delay time.Duration, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.DeferredPublishAsync(topic, delay, data, doneChan, args)
}

// --------------------

func (p *defBaseProducer) PublishMulti(messages [][]byte) error {
	return p.PublishMultiWithTopic(p.topic, messages)
}

func (p *defBaseProducer) PublishMultiWithTopic(topic string, messages [][]byte) error {
	if len(messages) == 0 {
		return errors.New("empty message")
	}
	return p.producer.MultiPublish(topic, messages)
}

func (p *defBaseProducer) PublishMultiAsync(messages [][]byte) error {
	return p.PublishMultiAsyncWithChan(messages, nil)
}

func (p *defBaseProducer) PublishMultiAsyncWithTopic(topic string, messages [][]byte) error {
	return p.PublishMultiAsyncWithChanWithTopic(topic, messages, nil)
}

func (p *defBaseProducer) PublishMultiAsyncWithChan(messages [][]byte, doneChan chan *nsq.ProducerTransaction,
	args ...interface{}) error {
	return p.PublishMultiAsyncWithChanWithTopic(p.topic, messages, doneChan, args)
}

func (p *defBaseProducer) PublishMultiAsyncWithChanWithTopic(topic string, messages [][]byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	if len(messages) == 0 {
		return errors.New("empty message")
	}
	return p.producer.MultiPublishAsync(topic, messages, doneChan, args)
}

// --------------------

func (p *defBaseProducer) Stop() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.producer == nil {
		return
	}
	p.producer.Stop()
	p.producer = nil
}

// --------------------

func NewProducer(addr string, topic string, opts ...Option) (BaseProducer, error) {
	config := nsq.NewConfig()

	// 配置项
	var opt options
	for _, o := range opts {
		o(&opt)
	}
	if len(opt.Secret) > 0 {
		config.AuthSecret = opt.Secret
	}

	producer, err := nsq.NewProducer(addr, config)
	if err != nil {
		return nil, err
	}

	log.Println("[Nsq] NewProducer success")

	return &defBaseProducer{
		producer: producer,
		topic:    topic,
	}, nil
}
