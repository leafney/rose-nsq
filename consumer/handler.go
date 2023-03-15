/**
 * @Author:      leafney
 * @Date:        2023-03-16 01:36
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package consumer

import "github.com/nsqio/go-nsq"

type Handler func(*XMessage) error

type XHandler struct {
	f Handler
}

func (q *XHandler) HandleMessage(message *nsq.Message) error {
	return q.f(&XMessage{message})
}
