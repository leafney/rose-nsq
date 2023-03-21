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
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 20; i++ {
		client.Publish(fmt.Sprintf("data-%v", i))
	}

}
