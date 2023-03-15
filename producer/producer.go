/**
 * @Author:      leafney
 * @Date:        2023-03-15 23:47
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package producer

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

type BaseProductModel interface {
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

type defaultBaseProductModel struct {
	producer *nsq.Producer
	topic    string
	mutex    sync.Mutex
}

func (p *defaultBaseProductModel) Publish(data string) error {
	return p.PublishWithTopic(p.topic, data)
}

func (p *defaultBaseProductModel) PublishWithTopic(topic, data string) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.Publish(topic, []byte(data))
}

func (p *defaultBaseProductModel) PublishAsync(data string) error {
	return p.PublishAsyncWithChan(data, nil)
}

func (p *defaultBaseProductModel) PublishAsyncWithChan(data string, doneChan chan *nsq.ProducerTransaction,
	args ...interface{}) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.PublishAsync(p.topic, []byte(data), doneChan, args)
}

func (p *defaultBaseProductModel) PublishDelay(data string, delay time.Duration) error {
	return p.PublishDelayWithTopic(p.topic, data, delay)
}

func (p *defaultBaseProductModel) PublishDelayWithTopic(topic, data string, delay time.Duration) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.DeferredPublish(topic, delay, []byte(data))
}

func (p *defaultBaseProductModel) PublishDelayAsync(data string, delay time.Duration) error {
	return p.PublishDelayAsyncWithChan(data, delay, nil)
}

func (p *defaultBaseProductModel) PublishDelayAsyncWithChan(data string, delay time.Duration, doneChan chan *nsq.ProducerTransaction,
	args ...interface{}) error {
	return p.PublishDelayAsyncWithChanWithTopic(p.topic, data, delay, doneChan, args)
}

func (p *defaultBaseProductModel) PublishDelayAsyncWithChanWithTopic(topic, data string, delay time.Duration, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	if len(data) == 0 {
		return errors.New("empty message")
	}
	return p.producer.DeferredPublishAsync(topic, delay, []byte(data), doneChan, args)
}

func (p *defaultBaseProductModel) PublishMulti(messages [][]byte) error {
	return p.PublishMultiByTopic(p.topic, messages)
}

func (p *defaultBaseProductModel) PublishMultiByTopic(topic string, messages [][]byte) error {
	if len(messages) == 0 {
		return errors.New("empty message")
	}
	return p.producer.MultiPublish(topic, messages)
}

func (p *defaultBaseProductModel) PublishMultiAsync(messages [][]byte) error {
	return p.PublishMultiAsyncWithChan(messages, nil)
}

func (p *defaultBaseProductModel) PublishMultiAsyncWithChan(messages [][]byte, doneChan chan *nsq.ProducerTransaction,
	args ...interface{}) error {
	return p.PublishMultiAsyncWithChanWithTopic(p.topic, messages, doneChan, args)
}

func (p *defaultBaseProductModel) PublishMultiAsyncWithChanWithTopic(topic string, messages [][]byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	if len(messages) == 0 {
		return errors.New("empty message")
	}
	return p.producer.MultiPublishAsync(topic, messages, doneChan, args)
}

func (p *defaultBaseProductModel) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.producer == nil {
		return nil
	}
	p.producer.Stop()
	p.producer = nil
	return nil
}

func NewProductClient(addr string, topic string) (BaseProductModel, error) {
	config := nsq.NewConfig()
	// 配置项
	producer, err := nsq.NewProducer(addr, config)
	if err != nil {
		return nil, err
	}

	log.Println("[Nsq] NewProductClient success")

	return &defaultBaseProductModel{
		producer: producer,
		topic:    topic,
	}, nil
}
