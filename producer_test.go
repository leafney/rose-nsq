/**
 * @Author:      leafney
 * @Date:        2023-03-15 23:47
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package rnsq

import (
	"fmt"
	"testing"
)

func TestNewProductClient(t *testing.T) {

	client, err := NewProducer("127.0.0.1:4150", "hello")
	//client, err := NewProducer("127.0.0.1:4150", "hello",SetSecret("abcd"))
	if err != nil {
		t.Error(err)
		return
	}

	//client.SetLogLevel("error")

	for i := 0; i < 50; i++ {
		msg := fmt.Sprintf("data-%v", i)
		client.Publish(msg)
		fmt.Printf("send %v \n", msg)

		//time.Sleep(1 * time.Second)
	}

}
