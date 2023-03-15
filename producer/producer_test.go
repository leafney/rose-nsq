/**
 * @Author:      leafney
 * @Date:        2023-03-15 23:47
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package producer

import "testing"

func TestNewProductClient(t *testing.T) {

	client, err := NewProductClient("", "hello")
	if err != nil {
		t.Error(err)
		return
	}
	client.Publish("")
}
