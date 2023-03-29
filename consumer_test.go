/**
 * @Author:      leafney
 * @Date:        2023-03-16 01:36
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package rnsq

import "testing"

func TestNewConsumeClient(t *testing.T) {

	client := NewConsumerNSQD([]string{"127.0.0.1:4150"}, "hello", "111")
	//client := NewConsumerNSQLookUpD([]string{""}, "", "")

	//client.SetMaxInFlight(2)

	client.Consume(func(msg *XMessage) error {
		t.Log(string(msg.Body))
		msg.Success()
		return nil
	})
	defer client.Stop()
	select {}
}
