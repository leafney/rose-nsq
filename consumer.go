/**
 * @Author:      leafney
 * @Date:        2023-03-16 01:35
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package rnsq

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"sync"
)

type (
	BaseConsumer interface {
		Consume(handler Handler) error
		ConsumeWithTopic(topic, channel string, handler Handler) error
		ConsumeMany(handler Handler, concurrency int) error
		ConsumeManyWithTopic(topic, channel string, handler Handler, concurrency int) error
		SetMaxInFlight(maxInFlight int)
		SetMaxAttempts(maxAttempts uint16)
	}

	defBaseConsumer struct {
		sync.Mutex
		config  *nsq.Config
		address []string
		cType   ConnType
		topic   string
		channel string
	}
)

func (c *defBaseConsumer) ConsumeWithTopic(topic, channel string, handler Handler) error {
	consumer, xHandler, err := c.createConsumer(topic, channel, handler)
	if err != nil {
		return err
	}

	consumer.AddHandler(xHandler)
	if err = c.connect(consumer); err != nil {
		return fmt.Errorf("failed to connect to [%v] err [%v]", c.cType, err)
	}

	fmt.Println("[Nsq] Consume success")
	return nil
}

func (c *defBaseConsumer) ConsumeManyWithTopic(topic, channel string, handler Handler, concurrency int) error {

	consumer, xHandler, err := c.createConsumer(topic, channel, handler)
	if err != nil {
		return err
	}

	consumer.AddConcurrentHandlers(xHandler, concurrency)
	if err = c.connect(consumer); err != nil {
		return fmt.Errorf("failed to connect to [%v] err [%v]", c.cType, err)
	}

	fmt.Println("[Nsq] ConsumeConcurrent success")
	return nil
}

func (c *defBaseConsumer) Consume(handler Handler) error {
	return c.ConsumeWithTopic(c.topic, c.channel, handler)
}

func (c *defBaseConsumer) ConsumeMany(handler Handler, concurrency int) error {
	return c.ConsumeManyWithTopic(c.topic, c.channel, handler, concurrency)
}

func (c *defBaseConsumer) SetMaxInFlight(maxInFlight int) {
	// MaxInFlight 配置项允许您控制每个消费者可以同时处理的消息数量
	if maxInFlight >= 0 && maxInFlight != 1 {
		c.config.MaxInFlight = maxInFlight
	}
}

func (c *defBaseConsumer) SetMaxAttempts(maxAttempts uint16) {
	c.config.MaxAttempts = maxAttempts
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

func (c *defBaseConsumer) createConsumer(topic, channel string, handler Handler) (*nsq.Consumer, *XHandler, error) {
	c.Lock()
	defer c.Unlock()

	consumer, err := nsq.NewConsumer(topic, channel, c.config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create consumer err [%v]", err)
	}

	xHandler := &XHandler{f: handler}

	return consumer, xHandler, nil
}

func newConsumer(addr []string, topic, channel string, connType ConnType) BaseConsumer {
	return &defBaseConsumer{
		address: addr,
		topic:   topic,
		channel: channel,
		cType:   connType,
		config:  nsq.NewConfig(),
	}
}

func NewConsumerNSQD(addr []string, topic, channel string) BaseConsumer {
	return newConsumer(addr, topic, channel, NSQD)
}

func NewConsumerNSQLookUpD(addr []string, topic, channel string) BaseConsumer {
	return newConsumer(addr, topic, channel, NSQLookupD)
}
