/**
 * @Author:      leafney
 * @Date:        2023-03-16 01:35
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package consumer

import (
	"github.com/nsqio/go-nsq"
	"log"
	"sync"
)

type (
	BaseConsumeModel interface {
		Consume(handler Handler) error
		ConsumeMany(handler Handler, concurrency int) error
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
	c.Lock()
	defer c.Unlock()

	consumer, err := nsq.NewConsumer(c.topic, c.channel, c.config)
	if err != nil {
		return err
	}

	log.Println("[Nsq] NewConsumeClient success")

	// 必须在Consumer连接之前调用AddHandler()方法，否则会抛panic
	q := &XHandler{f: handler}
	consumer.AddHandler(q)

	switch c.cType {
	case NSQD:
		err = consumer.ConnectToNSQDs(c.address)
	case NSQLookupd:
		err = consumer.ConnectToNSQLookupds(c.address)
	}

	if err != nil {
		log.Printf("[Nsq] Consumer ConnectTo [%v] err %v\n", c.cType, err)
		return err
	}

	log.Println("[Nsq] Consume success")
	return nil
}

func (c *defaultBaseConsumeModel) ConsumeMany(handler Handler, concurrency int) error {
	c.Lock()
	defer c.Unlock()

	consumer, err := nsq.NewConsumer(c.topic, c.channel, c.config)
	if err != nil {
		return err
	}

	log.Println("[Nsq] NewConsumeClient success")

	q := &XHandler{f: handler}
	consumer.AddConcurrentHandlers(q, concurrency)

	switch c.cType {
	case NSQD:
		err = consumer.ConnectToNSQDs(c.address)
	case NSQLookupd:
		err = consumer.ConnectToNSQLookupds(c.address)
	}

	if err != nil {
		log.Printf("[Nsq] ConsumeConcurrent ConnectTo [%v] err %v\n", c.cType, err)
		return err
	}

	log.Println("[Nsq] ConsumeConcurrent success")
	return nil
}

func (c *defaultBaseConsumeModel) SetMaxQueue(queue int) {
	// MaxInFlight 配置项允许您控制每个消费者可以同时处理的消息数量
	if queue >= 0 && queue != 1 {
		c.config.MaxInFlight = queue
	}
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
