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
	PublishWithTopic(topic, data string) error
	PublishAsync(data string) error
	PublishAsyncWithChan(data string, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error
	PublishDelay(data string, delay time.Duration) error
	PublishDelayWithTopic(topic, data string, delay time.Duration) error
	PublishDelayAsync(data string, delay time.Duration) error
	PublishDelayAsyncWithChan(data string, delay time.Duration, doneChan chan *nsq.ProducerTransaction,
		args ...interface{}) error
	PublishDelayAsyncWithChanWithTopic(topic, data string, delay time.Duration, doneChan chan *nsq.ProducerTransaction,
		args ...interface{}) error
	PublishMulti(messages [][]byte) error
	PublishMultiByTopic(topic string, messages [][]byte) error
	PublishMultiAsync(messages [][]byte) error
	PublishMultiAsyncWithChan(messages [][]byte, doneChan chan *nsq.ProducerTransaction,
		args ...interface{}) error
	PublishMultiAsyncWithChanWithTopic(topic string, messages [][]byte, doneChan chan *nsq.ProducerTransaction,
		args ...interface{}) error
	Close() error
}

type defBaseProducer struct {
	producer *nsq.Producer
	topic    string
	mutex    sync.Mutex
}

func (p *defBaseProducer) Publish(data string) error {
	return p.PublishWithTopic(p.topic, data)
}

func (p *defBaseProducer) PublishWithTopic(topic, data string) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.Publish(topic, []byte(data))
}

func (p *defBaseProducer) PublishAsync(data string) error {
	return p.PublishAsyncWithChan(data, nil)
}

func (p *defBaseProducer) PublishAsyncWithChan(data string, doneChan chan *nsq.ProducerTransaction,
	args ...interface{}) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.PublishAsync(p.topic, []byte(data), doneChan, args)
}

func (p *defBaseProducer) PublishDelay(data string, delay time.Duration) error {
	return p.PublishDelayWithTopic(p.topic, data, delay)
}

func (p *defBaseProducer) PublishDelayWithTopic(topic, data string, delay time.Duration) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.DeferredPublish(topic, delay, []byte(data))
}

func (p *defBaseProducer) PublishDelayAsync(data string, delay time.Duration) error {
	return p.PublishDelayAsyncWithChan(data, delay, nil)
}

func (p *defBaseProducer) PublishDelayAsyncWithChan(data string, delay time.Duration, doneChan chan *nsq.ProducerTransaction,
	args ...interface{}) error {
	return p.PublishDelayAsyncWithChanWithTopic(p.topic, data, delay, doneChan, args)
}

func (p *defBaseProducer) PublishDelayAsyncWithChanWithTopic(topic, data string, delay time.Duration, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.DeferredPublishAsync(topic, delay, []byte(data), doneChan, args)
}

func (p *defBaseProducer) PublishMulti(messages [][]byte) error {
	return p.PublishMultiByTopic(p.topic, messages)
}

func (p *defBaseProducer) PublishMultiByTopic(topic string, messages [][]byte) error {
	if len(messages) == 0 {
		return errors.New("empty message")
	}
	return p.producer.MultiPublish(topic, messages)
}

func (p *defBaseProducer) PublishMultiAsync(messages [][]byte) error {
	return p.PublishMultiAsyncWithChan(messages, nil)
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

func (p *defBaseProducer) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.producer == nil {
		return nil
	}
	p.producer.Stop()
	p.producer = nil
	return nil
}

func NewProducer(addr string, topic string) (BaseProducer, error) {
	config := nsq.NewConfig()
	// 配置项
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
