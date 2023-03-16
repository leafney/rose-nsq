/**
 * @Author:      leafney
 * @Date:        2023-03-16 01:36
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package consumer

import "testing"

func TestNewConsumeClient(t *testing.T) {

	client := NewConsumeClientNSQD([]string{""}, "", "")
	//client := NewConsumeClientNSQLookUpd([]string{""}, "", "")

	client.SetMaxInFlight(2)
	client.Consume(func(msg *XMessage) error {
		t.Log(string(msg.Body))
		return nil
	})
}
