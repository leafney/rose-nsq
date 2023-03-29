/**
 * @Author:      leafney
 * @Date:        2023-03-16 01:36
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package rnsq

import (
	"encoding/json"
	"github.com/nsqio/go-nsq"
	"time"
)

type XMessage struct {
	*nsq.Message
}

func (m *XMessage) Finish(success bool) {
	if success {
		m.Message.Finish()
	} else {
		m.Message.Requeue(-1)
	}
}

func (m *XMessage) Success() {
	m.Finish(true)
}

func (m *XMessage) Fail() {
	m.Finish(false)
}

func (m *XMessage) DelayReSend(delay time.Duration) {
	m.Message.Requeue(delay)
}

func (m *XMessage) ToJson(v interface{}) error {
	return json.Unmarshal(m.Body, v)
}

func (m *XMessage) ToString() string {
	return string(m.Body)
}

func (m *XMessage) ToByte() []byte {
	return m.Body
}
