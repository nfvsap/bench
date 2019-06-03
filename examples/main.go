package main

import (
	"fmt"
	"time"
	"strings"
	"sync"
	"bufio"
	"os"
	"log"
        "net/http"
	"encoding/json"
	"math/rand"
	"github.com/tylertreat/bench"
	"github.com/tylertreat/hdrhistogram-writer"
	"github.com/tylertreat/bench/requester"
)

var wg sync.WaitGroup
var slice []float64
var queue = make(chan float64, 1)



func average(xs[]float64)float64 {
	total:=0.0
	for _,v:=range xs {
		total +=v
	}
	return total/float64(len(xs))
}



func runBench(topic string) {
	defer wg.Done()
        r := &requester.RedisPubSubRequesterFactory{
                URL:         ":6379",
                PayloadSize:  10048,
                Channel:     topic,
        }
	benchmark := bench.NewBenchmark(r, 20000, 1, 5*time.Second, 0)
	summary, err := benchmark.Run()
	if err != nil {
	  panic(err)
	}
	//fmt.Println(summary.Throughput)
	queue <- summary.Throughput
}

func runBenchWithRes(topic string) {
	defer wg.Done()
        r := &requester.RedisPubSubRequesterFactory{
                URL:         ":6379",
                PayloadSize:  10048,
                Channel:     topic,
        }
	benchmark := bench.NewBenchmark(r, 20000, 1, 5*time.Second, 0)
	summary, err := benchmark.Run()
	if err != nil {
	  panic(err)
	}
	//fmt.Println(summary.Throughput)
	summary.GenerateLatencyDistribution(histwriter.Percentiles{50.0, 90.0, 95.0, 99.0, 99.9}, "redis.txt")
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
	fmt.Println("Starting benchmark")
	go func() {
	  for { 
		fmt.Println("\n\nStarting new iteration")
		wg.Add(301)
		for i := 0; i < 300; i++ {
			topic := RandomString(7)
			//fmt.Println(topic)
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
		f, _ := os.Open("redis.txt")
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
		f, _ := os.Open("redis.txt")
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
		slcB, _ := json.Marshal(lateMap)
		// fmt.Println(string(slcB))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(slcB)
	})

	log.Fatal(http.ListenAndServe(":9080", nil))
	//summary.GenerateLatencyDistribution(nil, "redis.txt")
}
