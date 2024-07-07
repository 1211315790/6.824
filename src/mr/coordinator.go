package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskStatus int

// Task 状态
const (
	IDLE     taskStatus = iota // 闲置未分配
	RUNNING                    // 正在运行
	FINISHED                   // 完成
	FAILED                     //失败
)

// Map Task 执行状态
type MapTaskInfo struct {
	task_id_    int        // Task 序号
	status_     taskStatus // 执行状态
	start_time_ int64      // 开始执行时间戳
}

// Reduce Task 执行状态
type ReduceTaskInfo struct {
	// ReduceTask的 序号 由数组下标决定, 不进行额外存储
	status_     taskStatus // 执行状态
	start_time_ int64      // 开始执行时间戳
}

type Coordinator struct {
	// Your definitions here.
	n_reduce_     int                     // the number of reduce tasks to use.
	map_tasks_    map[string]*MapTaskInfo //MapTaskInfo
	mtx_          sync.Mutex              // 一把大锁保平安
	reduce_tasks_ []*ReduceTaskInfo       // ReduceTaskInfo
}

func (c *Coordinator) initTask(files []string) {
	for idx, file_name := range files {
		c.map_tasks_[file_name] = &MapTaskInfo{
			task_id_: idx,
			status_:  IDLE,
		}
	}
	for idx := range c.reduce_tasks_ {
		c.reduce_tasks_[idx] = &ReduceTaskInfo{
			status_: IDLE,
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) AskForTask(req *MessageSend, reply *MessageReply) error {
	if req.MsgType != ASK_FOR_TASK {
		return ErrBadMsgType
	}
	// 选择一个任务返回给worker
	c.mtx_.Lock()
	defer c.mtx_.Unlock()

	count_map_success := 0
	for file_name, taskinfo := range c.map_tasks_ {
		alloc := false
		// 选择闲置或者失败的任务
		if taskinfo.status_ == IDLE || taskinfo.status_ == FAILED {
			alloc = true
		} else if taskinfo.status_ == RUNNING {
			// 判断其是否超时, 超时则重新派发
			curTime := time.Now().Unix()
			if curTime-taskinfo.start_time_ > 10 {
				taskinfo.start_time_ = curTime
				alloc = true
			}
		} else {
			count_map_success++
		}

		if alloc {
			// 将未分配的任务和已经失败的任务分配给这个worker
			reply.MsgType = MAP_TASK_ALLOC
			reply.TaskName = file_name
			reply.NReduce = c.n_reduce_
			reply.TaskId = taskinfo.task_id_

			// log.Printf("coordinator: apply Map Task: taskID = %v\n", reply.task_id_)

			// 修改状态信息
			taskinfo.status_ = RUNNING
			taskinfo.start_time_ = time.Now().Unix()
			return nil
		}
	}

	if count_map_success < len(c.map_tasks_) {
		// map任务没有可以分配的, 但都还未完成
		reply.MsgType = WAIT
		return nil
	}

	count_reduce_success := 0
	// 运行到这里说明map任务都已经完成
	for idx, taskinfo := range c.reduce_tasks_ {
		alloc := false
		if taskinfo.status_ == IDLE || taskinfo.status_ == FAILED {
			alloc = true
		} else if taskinfo.status_ == RUNNING {
			// 判断其是否超时, 超时则重新派发
			curTime := time.Now().Unix()
			if curTime-taskinfo.start_time_ > 10 {
				taskinfo.start_time_ = curTime
				alloc = true
			}
		} else {
			count_reduce_success++
		}

		if alloc {
			// 分配给其一个Reduce任务
			reply.MsgType = REDUCE_TASK_ALLOC
			reply.TaskId = idx

			// log.Printf("coordinator: apply Reduce Task: taskID = %v\n", reply.task_id_)

			taskinfo.status_ = RUNNING
			taskinfo.start_time_ = time.Now().Unix()
			return nil
		}
	}

	if count_reduce_success < len(c.reduce_tasks_) {
		// reduce任务没有可以分配的, 但都还未完成
		reply.MsgType = WAIT
		return nil
	}

	// 运行到这里说明所有任务都已经完成
	reply.MsgType = SHUTDOWN

	return nil
}
func (c *Coordinator) NoticeResult(req *MessageSend, reply *MessageReply) error {
	c.mtx_.Lock()
	defer c.mtx_.Unlock()
	if req.MsgType == MAP_SUCCESS {
		for _, v := range c.map_tasks_ {
			if v.task_id_ == req.TaskId {
				v.status_ = FINISHED
				// log.Printf("coordinator: map task%v FINISHED\n", v.task_id_)
				break
			}
		}
	} else if req.MsgType == REDUCE_SUCESS {
		c.reduce_tasks_[req.TaskId].status_ = FINISHED
		// log.Printf("coordinator: reduce task%v FINISHED\n", req.task_id_)
	} else if req.MsgType == MAP_FAILED {
		for _, v := range c.map_tasks_ {
			if v.task_id_ == req.TaskId {
				v.status_ = FAILED
				// log.Printf("coordinator: map task%v failed\n", v.task_id_)
				break
			}
		}
	} else if req.MsgType == REDUCE_FAILED {
		c.reduce_tasks_[req.TaskId].status_ = FAILED
		// log.Printf("coordinator: reduce task%v failed\n", req.task_id_)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	// 先确认mapTask完成
	for _, taskinfo := range c.map_tasks_ {
		if taskinfo.status_ != FINISHED {
			return false
		}
	}

	// fmt.Println("Coordinator: All map task finished")

	// 再确认Reduce Task 完成
	for _, taskinfo := range c.reduce_tasks_ {
		if taskinfo.status_ != FINISHED {
			return false
		}
	}

	// fmt.Println("Coordinator: All reduce task finished")

	// time.Sleep(time.Second * 5)

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		n_reduce_:     nReduce,
		map_tasks_:    make(map[string]*MapTaskInfo),
		reduce_tasks_: make([]*ReduceTaskInfo, nReduce),
	}

	// Your code here.
	// 由于每一个文件名就是一个map task ,需要初始化任务状态
	c.initTask(files)

	c.server()
	return &c
}
