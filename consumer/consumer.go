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
		ConsumeConcurrent(handler Handler, concurrency int) error
	}

	defaultBaseConsumeModel struct {
		mux     sync.Mutex
		Config  *nsq.Config
		Address string   // 连接地址 nsqd 或 nsqlookupd address
		CType   ConnType // 连接类型 nsqd:0 nsqlookupd:1
		Topic   string
		Channel string
	}
)

func (c *defaultBaseConsumeModel) ConsumeConcurrent(handler Handler, concurrency int) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	consumer, err := nsq.NewConsumer(c.Topic, c.Channel, c.Config)
	if err != nil {
		log.Fatalf("[Nsq] NewConsumeClient err %v", err)
	}

	log.Println("[Nsq] NewConsumeClient success")

	q := &XHandler{f: handler}
	consumer.AddConcurrentHandlers(q, concurrency)

	switch c.CType {
	case NSQD:
		err = consumer.ConnectToNSQD(c.Address)
	case NSQLookupd:
		err = consumer.ConnectToNSQLookupd(c.Address)
		//case NSQDs:
		//	err = c.Consumer.ConnectToNSQDs()
		//case NSQLookupds:
		//	err = c.Consumer.ConnectToNSQLookupds()
	}

	if err != nil {
		log.Printf("[Nsq] ConsumeConcurrent ConnectTo %v err %v", ConnType(c.CType), err)
		return err
	}

	log.Println("[Nsq] ConsumeConcurrent success")
	return nil
}

//
func (c *defaultBaseConsumeModel) Consume(handler Handler) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	consumer, err := nsq.NewConsumer(c.Topic, c.Channel, c.Config)
	if err != nil {
		log.Fatalf("[Nsq] NewConsumeClient err %v", err)
	}

	log.Println("[Nsq] NewConsumeClient success")

	// 必须在Consumer连接之前调用AddHandler()方法，否则会抛panic
	q := &XHandler{f: handler}
	consumer.AddHandler(q)

	switch c.CType {
	case NSQD:
		err = consumer.ConnectToNSQD(c.Address)
	case NSQLookupd:
		err = consumer.ConnectToNSQLookupd(c.Address)
		//case NSQDs:
		//	err = c.Consumer.ConnectToNSQDs()
		//case NSQLookupds:
		//	err = c.Consumer.ConnectToNSQLookupds()
	}

	if err != nil {
		log.Printf("[Nsq] Consumer ConnectTo %v err %v", ConnType(c.CType), err)
		return err
	}

	log.Println("[Nsq] Consume success")
	return nil
}

// TODO addr 支持多个

func NewConsumeClientNSQD(addr, topic, channel string, queue int) BaseConsumeModel {
	return newConsumeClient(addr, topic, channel, NSQD, queue)
}

func NewConsumeClientNSQLookupd(addr, topic, channel string, queue int) BaseConsumeModel {
	return newConsumeClient(addr, topic, channel, NSQLookupd, queue)
}

func newConsumeClient(addr, topic, channel string, connType ConnType, queueSize int) BaseConsumeModel {
	config := nsq.NewConfig()
	// 配置项
	if queueSize > 1 {
		config.MaxInFlight = queueSize
	}

	return &defaultBaseConsumeModel{
		Config:  config,
		Address: addr,
		Topic:   topic,
		Channel: channel,
		CType:   connType,
	}
}
