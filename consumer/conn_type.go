/**
 * @Author:      leafney
 * @Date:        2023-03-16 01:37
 * @Project:     rose-nsq
 * @HomePage:    https://github.com/leafney
 * @Description:
 */

package consumer

type ConnType int

const (
	NSQD       ConnType = 0
	NSQLookupd ConnType = 1
)
