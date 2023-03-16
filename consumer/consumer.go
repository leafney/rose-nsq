/**
 * @Author:      leafney
 * @Date:        2023-03-16 01:35
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package consumer

import (
	"errors"
	"fmt"
	"github.com/nsqio/go-nsq"
	"sync"
)

type (
	BaseConsumeModel interface {
		Consume(handler Handler) error
		ConsumeMany(handler Handler, concurrency int) error
		SetMaxInFlight(maxInFlight int)
		SetMaxAttempts(maxAttempts uint16)
	}

	defaultBaseConsumeModel struct {
		sync.Mutex
		config  *nsq.Config
		address []string
		cType   ConnType
		topic   string
		channel string
	}
)

func (c *defaultBaseConsumeModel) Consume(handler Handler) error {

	consumer, xHandler, err := c.createConsumer(c.topic, c.channel, handler)
	if err != nil {
		return err
	}

	consumer.AddHandler(xHandler)

	switch c.cType {
	case NSQD:
		err = consumer.ConnectToNSQDs(c.address)
	case NSQLookupd:
		err = consumer.ConnectToNSQLookupds(c.address)
	}

	if err != nil {
		return fmt.Errorf("failed to connect to [%v] err [%v]", c.cType, err)
	}

	fmt.Println("[Nsq] Consume success")
	return nil
}

func (c *defaultBaseConsumeModel) ConsumeMany(handler Handler, concurrency int) error {

	consumer, xHandler, err := c.createConsumer(c.topic, c.channel, handler)
	if err != nil {
		return err
	}

	consumer.AddConcurrentHandlers(xHandler, concurrency)

	switch c.cType {
	case NSQD:
		err = consumer.ConnectToNSQDs(c.address)
	case NSQLookupd:
		err = consumer.ConnectToNSQLookupds(c.address)
	}

	if err != nil {
		return fmt.Errorf("failed to connect to [%v] err [%v]", c.cType, err)
	}

	fmt.Println("[Nsq] ConsumeConcurrent success")
	return nil
}

func (c *defaultBaseConsumeModel) SetMaxInFlight(maxInFlight int) {
	// MaxInFlight 配置项允许您控制每个消费者可以同时处理的消息数量
	if maxInFlight >= 0 && maxInFlight != 1 {
		c.config.MaxInFlight = maxInFlight
	}
}

func (c *defaultBaseConsumeModel) SetMaxAttempts(maxAttempts uint16) {
	c.config.MaxAttempts = maxAttempts
}

//func (c *defaultBaseConsumeModel) SetMaxBackoffDuration(maxBackoffDuration, maxBackoffJitterDuration nsq.Duration) {
//	c.config.MaxBackoffDuration = maxBackoffDuration
//	c.config .MaxBackoffJitter = maxBackoffJitterDuration
//}

func (c *defaultBaseConsumeModel) createConsumer(topic, channel string, handler Handler) (*nsq.Consumer, *XHandler, error) {
	c.Lock()
	defer c.Unlock()

	consumer, err := nsq.NewConsumer(topic, channel, c.config)
	if err != nil {
		return nil, nil, errors.New("failed to create consumer: " + err.Error())
	}

	xHandler := &XHandler{f: handler}

	return consumer, xHandler, nil
}

func newConsumeClient(addr []string, topic, channel string, connType ConnType) BaseConsumeModel {
	return &defaultBaseConsumeModel{
		address: addr,
		topic:   topic,
		channel: channel,
		cType:   connType,
		config:  nsq.NewConfig(),
	}
}

func NewConsumeClientNSQD(addr []string, topic, channel string) BaseConsumeModel {
	return newConsumeClient(addr, topic, channel, NSQD)
}

func NewConsumeClientNSQLookUpd(addr []string, topic, channel string) BaseConsumeModel {
	return newConsumeClient(addr, topic, channel, NSQLookupd)
}
