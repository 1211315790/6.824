package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func DelFileByReduceId(targetNumber int, path string) error {
	// 创建正则表达式，X 是可变的指定数字
	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	// 读取当前目录中的文件
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// 遍历文件，查找匹配的文件
	for _, file := range files {
		if file.IsDir() {
			continue // 跳过目录
		}
		fileName := file.Name()
		if regex.MatchString(fileName) {
			// 匹配到了文件，删除它
			filePath := filepath.Join(path, file.Name())
			err := os.Remove(filePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
func ReadSpecificFile(targetNumber int, path string) (fileList []*os.File, err error) {
	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	// 读取当前目录中的文件
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	// 遍历文件，查找匹配的文件
	for _, fileEntry := range files {
		if fileEntry.IsDir() {
			continue // 跳过目录
		}
		fileName := fileEntry.Name()
		if regex.MatchString(fileName) {
			filePath := filepath.Join(path, fileEntry.Name())
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatalf("cannot open %v", filePath)
				for _, oFile := range fileList {
					oFile.Close()
				}
				return nil, err
			}
			fileList = append(fileList, file)
		}
	}
	return fileList, nil
}
func CallForReportStatus(msg_type MsgType, task_id int) bool {
	// 报告Task执行情况
	// declare an argument structure.
	args := MessageSend{
		MsgType: msg_type,
		TaskId:  task_id,
	}
	// if succesType == MapSuccess {
	// log.Printf("Worker: Report Map success: %v", taskID)
	// } else {
	// log.Printf("Worker: Report Reduce success: %v", taskID)
	// }

	ret := call("Coordinator.NoticeResult", &args, nil)
	// if err != nil {
	// 	fmt.Printf("Worker: Report success failed: %s\n", err.Error())
	// }
	return ret
}
func CallForTask() *MessageReply {
	// 请求一个Task
	// declare an argument structure.
	args := MessageSend{
		MsgType: ASK_FOR_TASK,
	}

	// declare a reply structure.
	reply := MessageReply{}

	// send the RPC request, wait for the reply.
	ret := call("Coordinator.AskForTask", &args, &reply)
	if ret {
		// fmt.Printf("TaskName %v, NReduce %v, taskID %v\n", reply.TaskName, reply.NReduce, reply.TaskID)
		return &reply
	} else {
		// log.Println(err.Error())
		return nil
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func HandleMapTask(reply *MessageReply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.TaskName)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	// 调用用户自定义的map函数
	kva := mapf(reply.TaskName, string(content))

	tmp_files := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)
	// 将kv分到不同的reduce，写入文件
	for _, kv := range kva {
		reduce_id := ihash(kv.Key) % reply.NReduce
		if encoders[reduce_id] == nil {
			tmp_file, err := os.CreateTemp("", fmt.Sprintf("mr-map-tmp-%d", reduce_id))
			if err != nil {
				return err
			}
			defer tmp_file.Close()
			tmp_files[reduce_id] = tmp_file
			encoders[reduce_id] = json.NewEncoder(tmp_file)
		}
		err := encoders[reduce_id].Encode(&kv)
		if err != nil {
			return err
		}
	}

	for i, file := range tmp_files {
		if file != nil {
			file_name := file.Name()
			file.Close()
			new_name := fmt.Sprintf("mr-out-%d-%d", reply.TaskId, i)
			if err := os.Rename(file_name, new_name); err != nil {
				return err
			}
		}
	}

	return nil
}
func HandleReduceTask(reply *MessageReply, reduce_func func(string, []string) string) error {
	key_id := reply.TaskId
	k_vs := map[string][]string{}
	fileList, err := ReadSpecificFile(key_id, "./")
	if err != nil {
		return err
	}
	// 整理所有的中间文件
	for _, file := range fileList {
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			k_vs[kv.Key] = append(k_vs[kv.Key], kv.Value)
		}
		file.Close()
	}
	// 获取所有的键并排序
	var keys []string
	for k := range k_vs {
		keys = append(keys, k)
	}
	oname := "mr-out-" + strconv.Itoa(reply.TaskId)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()
	for _, key := range keys {
		output := reduce_func(key, k_vs[key])
		_, err := fmt.Fprintf(ofile, "%v %v\n", key, output)
		if err != nil {
			return err
		}
	}
	DelFileByReduceId(reply.TaskId, "./")
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// 循环请求
		reply_msg := CallForTask()
		switch reply_msg.MsgType {
		case MAP_TASK_ALLOC:
			err := HandleMapTask(reply_msg, mapf)
			if err == nil {
				CallForReportStatus(MAP_SUCCESS, reply_msg.TaskId)
			} else {
				CallForReportStatus(MAP_FAILED, reply_msg.TaskId)
			}
		case REDUCE_TASK_ALLOC:
			err := HandleReduceTask(reply_msg, reducef)
			if err == nil {

				CallForReportStatus(REDUCE_SUCESS, reply_msg.TaskId)
			} else {
				// log.Println("Worker: Map Task failed")
				CallForReportStatus(REDUCE_FAILED, reply_msg.TaskId)
			}
		case WAIT:
			time.Sleep(time.Second * 10)
		case SHUTDOWN:
			os.Exit(0)
		}
		time.Sleep(time.Second)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		os.Exit(-1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
