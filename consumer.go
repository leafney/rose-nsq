/**
 * @Author:      leafney
 * @Date:        2023-03-16 01:35
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package rnsq

import (
	"errors"
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"sync"
)

type (
	BaseConsumer interface {
		Consume(handler Handler) error
		ConsumeWithTopic(topic, channel string, handler Handler, opts ...Option) error
		ConsumeMany(handler Handler, concurrency int) error
		ConsumeManyWithTopic(topic, channel string, handler Handler, concurrency int, opts ...Option) error
		ChangeMaxInFlight(maxInFlight int)
		ChangeMaxInFlightWithTopic(topic, channel string, maxInFlight int)
		Stats() *ConsumerStates
		StatsWithTopic(topic, channel string) *ConsumerStates
		Stop()

		SetMaxInFlight(maxInFlight int) *defBaseConsumer
		SetMaxAttempts(maxAttempts uint16) *defBaseConsumer
		SetSecret(secret string) *defBaseConsumer
	}

	defBaseConsumer struct {
		sync.Mutex
		cType     ConnType
		config    *nsq.Config
		address   []string
		topic     string
		channel   string
		consumers map[string]*nsq.Consumer
	}

	ConsumerStates struct {
		Received    uint64
		Finished    uint64
		Requeued    uint64
		Connections int
	}
)

func (c *defBaseConsumer) StatsWithTopic(topic, channel string) (stats *ConsumerStates) {
	conKey := fmt.Sprintf("%s:%s", topic, channel)
	if conVal, ok := c.consumers[conKey]; ok {
		cs := conVal.Stats()
		stats = &ConsumerStates{
			Received:    cs.MessagesReceived,
			Finished:    cs.MessagesFinished,
			Requeued:    cs.MessagesRequeued,
			Connections: cs.Connections,
		}
	}
	return
}

func (c *defBaseConsumer) Stats() *ConsumerStates {
	return c.StatsWithTopic(c.topic, c.channel)
}

func (c *defBaseConsumer) ChangeMaxInFlightWithTopic(topic, channel string, maxInFlight int) {
	conKey := fmt.Sprintf("%s:%s", topic, channel)
	if conVal, ok := c.consumers[conKey]; ok {
		conVal.ChangeMaxInFlight(maxInFlight)
	}
}

func (c *defBaseConsumer) ChangeMaxInFlight(maxInFlight int) {
	c.ChangeMaxInFlightWithTopic(c.topic, c.channel, maxInFlight)
}

func (c *defBaseConsumer) Consume(handler Handler) error {
	return c.ConsumeWithTopic(c.topic, c.channel, handler)
}

func (c *defBaseConsumer) ConsumeWithTopic(topic, channel string, handler Handler, opts ...Option) error {
	consumer, xHandler, err := c.createConsumer(topic, channel, handler, opts...)
	if err != nil {
		return err
	}

	consumer.AddHandler(xHandler)
	if err = c.connect(consumer); err != nil {
		return fmt.Errorf("[Nsq] Failed to connect to [%v] err [%v]", c.cType, err)
	}

	log.Println("[Nsq] Consume success")
	return nil
}

func (c *defBaseConsumer) ConsumeMany(handler Handler, concurrency int) error {
	return c.ConsumeManyWithTopic(c.topic, c.channel, handler, concurrency)
}

func (c *defBaseConsumer) ConsumeManyWithTopic(topic, channel string, handler Handler, concurrency int, opts ...Option) error {
	consumer, xHandler, err := c.createConsumer(topic, channel, handler, opts...)
	if err != nil {
		return err
	}

	consumer.AddConcurrentHandlers(xHandler, concurrency)
	if err = c.connect(consumer); err != nil {
		return fmt.Errorf("[Nsq] Failed to connect to [%v] err [%v]", c.cType, err)
	}

	log.Println("[Nsq] ConsumeConcurrent success")
	return nil
}

func (c *defBaseConsumer) SetMaxInFlight(maxInFlight int) *defBaseConsumer {
	// MaxInFlight 配置项允许您控制每个消费者可以同时处理的消息数量
	if maxInFlight >= 0 && maxInFlight != 1 {
		c.config.MaxInFlight = maxInFlight
	}
	return c
}

func (c *defBaseConsumer) SetMaxAttempts(maxAttempts uint16) *defBaseConsumer {
	c.config.MaxAttempts = maxAttempts
	return c
}

func (c *defBaseConsumer) SetSecret(secret string) *defBaseConsumer {
	if len(secret) > 0 {
		c.config.AuthSecret = secret
	}
	return c
}

func (c *defBaseConsumer) Stop() {
	for _, con := range c.consumers {
		con.Stop()
	}
}

func (c *defBaseConsumer) connect(consumer *nsq.Consumer) (err error) {
	switch c.cType {
	case NSQD:
		err = consumer.ConnectToNSQDs(c.address)
	case NSQLookupD:
		err = consumer.ConnectToNSQLookupds(c.address)
	}
	return
}

func (c *defBaseConsumer) createConsumer(topic, channel string, handler Handler, opts ...Option) (*nsq.Consumer, *XHandler, error) {
	c.Lock()
	defer c.Unlock()

	xHandler := &XHandler{f: handler}

	if len(topic) == 0 || len(channel) == 0 {
		return nil, xHandler, errors.New("[Nsq] topic or channel can not empty")
	}

	conKey := fmt.Sprintf("%s:%s", topic, channel)
	if conVal, ok := c.consumers[conKey]; ok {
		return conVal, xHandler, nil
	} else {

		// 配置项
		var opt options
		for _, o := range opts {
			o(&opt)
		}
		if len(opt.Secret) > 0 {
			c.config.AuthSecret = opt.Secret
		}

		if consumer, err := nsq.NewConsumer(topic, channel, c.config); err == nil {
			c.consumers[conKey] = consumer
			return consumer, xHandler, nil
		} else {
			return nil, xHandler, err
		}
	}
}

func newConsumer(addr []string, topic, channel string, connType ConnType) BaseConsumer {
	return &defBaseConsumer{
		address:   addr,
		topic:     topic,
		channel:   channel,
		cType:     connType,
		config:    nsq.NewConfig(),
		consumers: make(map[string]*nsq.Consumer),
	}
}

func NewConsumerNSQD(addr []string, topic, channel string) BaseConsumer {
	return newConsumer(addr, topic, channel, NSQD)
}

func NewConsumerNSQLookUpD(addr []string, topic, channel string) BaseConsumer {
	return newConsumer(addr, topic, channel, NSQLookupD)
}
