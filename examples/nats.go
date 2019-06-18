package main

import (
	"fmt"
	"time"
	"strings"
	"flag"
	"sync"
	"bufio"
	"os"
	"strconv"
	"log"
        "net/http"
	"encoding/json"
	"math/rand"
	"github.com/bench"
	"github.com/tylertreat/hdrhistogram-writer"
	"github.com/bench/requester"
)

var wg sync.WaitGroup
var slice []float64
var queue = make(chan float64, 1)
var port = "9080"
var topics = 10
var payload = 1000
var filename = ""
var natsPort = "4222"
var avgLatency = 0.0
var throughput = 0.0

var usageStr = `
Usage: nats [options]

Test Options:
	-port <string>     API port (default: 9080)
	-topics <int>      The number of topics (default: 10)
	-payload <int>     The payload in bytes
	-filename <string> The name of file to store the metrics
	-natsPort <string> Nats Server port (default: 4222)
`

func usage() {
	log.Fatalf(usageStr + "\n")
}

func average(xs[]float64)float64 {
	total:=0.0
	for _,v:=range xs {
		total +=v
	}
	return total/float64(len(xs))
}

func averageInt(xs[]int64)float64 {
	var total int64
	total = 0
	for _,v:=range xs {
		total +=v
	}
	return float64(total)/float64(len(xs))
}

func runBench(topic string) {
	defer wg.Done()
        r := &requester.NATSStreamingRequesterFactory{
		URL:         ":" + natsPort,
                PayloadSize:  payload,
                Subject:     topic,
		ClientID:    "benchmark",
        }
	benchmark := bench.NewBenchmark(r, 0, 1, 3*time.Second, 0)
	summary, err := benchmark.Run()
	if err != nil {
	  panic(err)
	}
	//fmt.Println(summary.Throughput)
	queue <- summary.Throughput
}

func runBenchWithRes(topic string) {
	defer wg.Done()
        r := &requester.NATSStreamingRequesterFactory{
		URL:         ":" + natsPort,
                PayloadSize:  payload,
		Subject:     topic,
		ClientID:    "benchmark",
        }
	benchmark := bench.NewBenchmark(r, 0, 1, 3*time.Second, 0)
	summary, err := benchmark.Run()
	if err != nil {
	  panic(err)
	}
	//fmt.Println(summary.Latencies)
	avgLatency = averageInt(summary.Latencies)
	throughput = summary.Throughput
	summary.GenerateLatencyDistribution(histwriter.Percentiles{50.0, 90.0, 95.0, 99.0}, filename)
	queue <- summary.Throughput
}

//RandomString - Generate a random string of A-Z chars with len = l
func RandomString(len int) string {
      bytes := make([]byte, len)
      for i := 0; i < len; i++ {
          bytes[i] = byte(65 + rand.Intn(25))  //A=65 and Z = 65+25
      }
      return string(bytes)
}


func main() {

	flag.StringVar(&port, "port", "9080", "API Port")
	flag.IntVar(&topics, "topics", 10, "The number of topics")
	flag.IntVar(&payload, "payload", 1000, "The payload in bytes")
	flag.StringVar(&filename, "filename", "", "The name of file to store the metrics")
	flag.StringVar(&natsPort, "natsPort", "4222", "Nats server port")

	flag.Usage = usage
	flag.Parse()

	fmt.Println("Starting benchmark")
	go func() {
	  for {
		fmt.Println("\n\nStarting new iteration")
		wg.Add(topics+1)
		for i := 0; i < topics; i++ {
			//topic := RandomString(26)
			//fmt.Println(topic)
			topic := "foo_" + strconv.Itoa(i)
			go runBench(topic)
		}
		go runBenchWithRes("collector")
		// Poll the queue for data and append it to the slice.
		// Since this happens synchronously and in the same
		// goroutine/thread, this can be considered safe.
		go func() {
			defer wg.Done()
			for t := range queue {
				slice = append(slice, t)
			}
		}()

		wg.Wait()
		//fmt.Println(slice)
		fmt.Println("Average throughput:")
		fmt.Println(average(slice))
		f, _ := os.Open(filename)
		fs := bufio.NewScanner(f)
		lateMap := make(map[string]string)
		for fs.Scan() {
			txt := fs.Text()
			if strings.Contains(txt, "Value"){
				continue
			}
			splitted := strings.Split(txt," ")
			var slc []string
			for _, str := range splitted {
				if str != ""{
					slc = append(slc, str)
				}
			}
			if len(slc) <= 1 {
				continue
			}
			fmt.Println(txt)
			lateMap[slc[1]] = slc[0]
		}
		fmt.Println(lateMap)
		slcB, _ := json.Marshal(lateMap)
		fmt.Println(string(slcB))
	  }
	}()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		f, _ := os.Open(filename)
		fs := bufio.NewScanner(f)
		lateMap := make(map[string]string)
		for fs.Scan() {
			txt := fs.Text()
			if strings.Contains(txt, "Value"){
				continue
			}
			splitted := strings.Split(txt," ")
			var slc []string
			for _, str := range splitted {
				if str != ""{
					slc = append(slc, str)
				}
			}
			if len(slc) <= 1 {
				continue
			}
			//fmt.Println(txt)
			lateMap[slc[1]] = slc[0]
		}
		//fmt.Println(lateMap)
		lateMap["AverageLatency"] = strconv.FormatFloat(avgLatency / 1000000.0, 'f', -1, 64)
		lateMap["Throughput"] = strconv.FormatFloat(throughput, 'f', -1, 64)
		slcB, _ := json.Marshal(lateMap)
		// fmt.Println(string(slcB))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(slcB)
	})

	log.Fatal(http.ListenAndServe(":" + port, nil))
	//summary.GenerateLatencyDistribution(nil, "redis.txt")
}
