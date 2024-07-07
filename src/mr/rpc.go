package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"os"
	"strconv"
)

type MsgType int

var (
	ErrBadMsgType = errors.New("bad message type")
	ErrNoMoreTask = errors.New("no more task left")
)

const (
	ASK_FOR_TASK      MsgType = iota // `Worker`请求任务
	MAP_TASK_ALLOC                   // `Coordinator`分配`Map`任务
	REDUCE_TASK_ALLOC                // `Coordinator`分配`Reduce`任务
	MAP_SUCCESS                      // `Worker`报告`Map`任务的执行成功
	MAP_FAILED                       // `Worker`报告`Map`任务的执行失败
	REDUCE_SUCESS                    // `Worker`报告`Reduce`任务的执行成功
	REDUCE_FAILED                    //`Worker`报告`Reduce`任务的执行失败
	SHUTDOWN                         // `Coordinator`告知`Worker`退出（所有任务执行成功）
	WAIT                             //`Coordinator`告知`Worker`休眠（暂时没有任务需要执行）
)

type MessageSend struct {
	MsgType MsgType
	TaskId  int // `Worker`回复的消息类型如MapSuccess等需要使用
}

type MessageReply struct {
	MsgType  MsgType
	NReduce  int    // MapTaskAlloc需要告诉Map Task 切分的数量
	TaskId   int    // 任务Id用于选取输入文件
	TaskName string // MapSuccess专用: 告知输入.txt文件的名字
}

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
