/**
 * @Author:      leafney
 * @Date:        2023-03-16 01:36
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package rnsq

import (
	"log"
	"testing"
)

func TestNewConsumeClient(t *testing.T) {

	client := NewConsumerNSQD([]string{"127.0.0.1:4150"}, "hello", "111").SetSecret("abcd")

	//client := NewConsumerNSQLookUpD([]string{""}, "", "")
	//client.SetMaxInFlight(2)
	//client.SetSecret("abcdef")

	if err := client.Consume(func(msg *XMessage) error {
		t.Log(msg.ToString())
		msg.Success()
		return nil
	}); err != nil {
		log.Panicf("consume err %v", err)
	}
	defer client.Stop()
	select {}
}
