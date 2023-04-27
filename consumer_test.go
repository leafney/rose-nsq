/**
 * @Author:      leafney
 * @Date:        2023-03-16 01:36
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package rnsq

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestNewConsumeClient(t *testing.T) {

	// init
	client := NewConsumerNSQD([]string{"127.0.0.1:4150"}, "hello", "111")
	//client := NewConsumerNSQD([]string{"127.0.0.1:4150"}, "hello", "111").SetSecret("abcd")

	//client := NewConsumerNSQLookUpD([]string{""}, "", "")

	// config
	//client.SetMaxInFlight(2)
	//client.SetSecret("abcdef")

	// test Consume
	//if err := client.Consume(func(msg *XMessage) error {
	//	t.Log(msg.ToString())
	//	time.Sleep(1 * time.Second)
	//	msg.Success()
	//	return nil
	//}); err != nil {
	//	log.Panicf("consume err %v", err)
	//}

	// test ConsumeMany
	if err := client.ConsumeMany(func(msg *XMessage) error {
		t.Log(msg.ToString())
		time.Sleep(1 * time.Second)
		msg.Success()
		return nil
	}, 5); err != nil {
		log.Panicf("consume err %v", err)
	}

	defer client.Stop()

	// stats and MaxInFlight
	go func() {
		time.Sleep(8 * time.Second)
		fmt.Println(client.Stats())
		fmt.Println("change maxInFlight")
		client.ChangeMaxInFlight(0)
		time.Sleep(5 * time.Second)
		client.ChangeMaxInFlight(4)
		time.Sleep(2 * time.Second)
		client.ChangeMaxInFlight(1)
	}()

	select {}
}
