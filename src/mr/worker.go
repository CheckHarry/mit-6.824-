package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	id := os.Getpid()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		args := Args{id}
		reply := Reply{}
		ok := call("Coordinator.RPChandler", &args, &reply)
		if ok {
			switch reply.Stage {
			case StageMap:
				file, err := os.Open(reply.Filepath)
				if err != nil {
					log.Fatalf("cannot open %v", reply.Filepath)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.Filepath)
				}
				file.Close()
				if err != nil {
					fmt.Printf("read file failed!\n")
				}
				map_result := mapf(reply.Filepath, string(content))
				file_mapping := make(map[int]*os.File)
				for i := 0; i < reply.NReduce; i++ {
					ofile_, err := os.Create(fmt.Sprintf("middle_%d_%s_%d.txt", id, reply.Filename, i))
					if err != nil {
						fmt.Println(err.Error())
					}
					file_mapping[i] = ofile_
				}

				for _, kv := range map_result {

					ofile, _ := file_mapping[ihash(kv.Key)%reply.NReduce]
					fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
					//fmt.Printf("%v %v\n", kv.Key, kv.Value)
				}
				outputfiles := make([]string, 0)
				for i := 0; i < reply.NReduce; i++ {
					file_mapping[i].Close()
					outputfiles = append(outputfiles, file_mapping[i].Name())
				}
				args_report := ArgsReport{outputfiles, id}
				reply_report := ReplyReport{}
				call("Coordinator.Report", &args_report, &reply_report)
			case StageReduce:
				//fmt.Println(reply.Filepath)
				file, err := os.Open(reply.Filepath)
				rgx := regexp.MustCompile("middle_(.*).txt")
				res := rgx.FindStringSubmatch(reply.Filename)
				if err != nil {
					log.Fatalf("cannot open %v", reply.Filepath)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.Filepath)
				}
				file.Close()
				if err != nil {
					fmt.Printf("read file failed!\n")
				}
				content_lines := strings.Split(string(content), "\n")
				mapping := make(map[string][]string)
				ofile, _ := os.Create("mr-out-" + res[1])
				for _, line := range content_lines {
					if len(line) == 0 {
						continue
					}
					index := strings.Index(line, " ")
					if index < 0 { // error happend

					}
					key := line[:index]
					value := line[index+1:]
					_, ok := mapping[key]
					if !ok { // no mapping , so we make one
						mapping[key] = make([]string, 0)
					}
					mapping[key] = append(mapping[key], value)
				}
				for key, value := range mapping {
					out := reducef(key, value)
					fmt.Fprintf(ofile, "%s %s\n", key, out)
				}
				ofile.Close()
				args_report := ArgsReport{[]string{ofile.Name()}, id}
				reply_report := ReplyReport{}
				call("Coordinator.Report", &args_report, &reply_report)
			case 0: //
				time.Sleep(time.Second / 10) // sleep 0.1 second
			}

		} else {
			fmt.Printf("call failed!\n")
			return
		}
	}
	/*
		if ok {
			dat, err := os.ReadFile(reply.filename)
			if err != nil {
				fmt.Printf("read file failed!\n")
			}
			content := string(dat)
			map_result := mapf(reply.filename, content)
			oname := "mr-out-" + strconv.Itoa(reply.taskno)
			ofile, _ := os.Create(oname)
			defer ofile.Close()
			for _, kv := range map_result {
				output := reducef(kv.Key, []string{kv.Value})
				fmt.Fprintf(ofile, "%v %v", kv.Key, output)
			}

		} else {
			fmt.Printf("call failed!\n")
		}*/

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
