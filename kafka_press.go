package main

import (
	"github.com/Shopify/sarama"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"
//	"reflect"
	"io/ioutil"
)

var argRole = flag.String("role", "P", "Role choice: [P, C]")
var argWorkId = flag.Int("work_id", 0, "work id")
var argTopic = flag.String("topic", "topic_1", "topic id")
var argRoutineNumber = flag.Int("routine_number", 1, "routine number")
var argKafkaHost = flag.String("kafka_hosts", "localhost:9092", "kafka address")
var argTimeSleep = flag.Int("sleep", 100, "time sleep")
var argTotalMsgNum = flag.Int("total_msg_number", 1000000, "total message number")
var argReplica = flag.Int("replica", 1, "replica of the content")
var argPartitionNumber = flag.Int("partition_number", 1, "partition number")
var argFileName = flag.String("filename", "", "the msg content")
var argThroughput = flag.Int("throughput", 1000, "throughput: ChannelBufferSize")
var argOffset = flag.Int("offset", 0, "offset")


func CreateSyncProducer(kafka_addresses string) sarama.SyncProducer {
	brokerList := strings.Split(kafka_addresses, ",")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Producer.MaxMessageBytes = 1048576000
	config.ChannelBufferSize = *argThroughput
	producer, err := sarama.NewSyncProducer(brokerList, config)
	fmt.Printf("broker list: %v", brokerList)
//	producer, err := sarama.NewSyncProducer(brokerList, nil)
	if err != nil {
		fmt.Printf("Create Producer fail. err: %v", err)
		return nil
	}
	return producer
}

func CreateCunsumer(kafka_addresses string) sarama.Consumer {
	brokerList := strings.Split(kafka_addresses, ",")
	config := sarama.NewConfig()
	config.Consumer.Fetch.Default=524288
	config.ChannelBufferSize = *argThroughput
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		fmt.Printf("Create Cunsumer fail. Err: %v", err)
	}
	return consumer
}

func GetData(consumer sarama.Consumer, partitionnumber int32) {
	partitionConsumer, err := consumer.ConsumePartition(*argTopic, partitionnumber, int64(*argOffset))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
        fmt.Print(err)
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	consumed := 0
	ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Consumed message: %d partition: %d offset %d\n", len(msg.Value), msg.Partition, msg.Offset)
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}
	fmt.Printf("Total msg: %d\n", consumed)
}

func ReplicaData(target_msg *string) {
	base_msg := "1234567890abcdefghijklmnopqrstuvwxyz!@#$%^&*(){}|:>?<+~,.\"/;'[]_"
	for i := 0; i < *argReplica; i++ {
		*target_msg = *target_msg + base_msg
	}
}


func WriteMsg(producer sarama.SyncProducer, routine_number int) {
	defer func() {
		producer.Close()
	}()
	timestamp := time.Now().Unix()
	msg_number := 0
	tm := time.Unix(timestamp, 0)

	var content string
	if len(*argFileName) == 0 {
		target_msg := ""
		ReplicaData(&target_msg)
		fmt.Printf("Target msg len: %d\n", len(target_msg))
		content = fmt.Sprintf("%s time: %s:%d", tm.Format("2006-01-02 03:04:05 PM"), target_msg, time.Now().Nanosecond())
	}else {
		byte_content, err := ioutil.ReadFile(*argFileName)
		if err != nil {
			fmt.Printf("Read %s fail!\n", *argFileName)
			return
		}else {
			content = string(byte_content)
			fmt.Printf("File: %s length: %d\n", *argFileName, len(content))
		}
	}
	for {
		p, o, e := producer.SendMessage(&sarama.ProducerMessage{
		Topic: *argTopic,
//		Value: sarama.StringEncoder(fmt.Sprintf("%s", target_msg)),
		Value: sarama.StringEncoder(content),
		})
		if e != nil {
			fmt.Printf("Send message fail! error: %v\n", e)
		}else{
			fmt.Printf("Partition: %d, offset: %d\n", p, o)
		}

		msg_number = msg_number + 1
		if msg_number >= *argTotalMsgNum {
			break
		}
		fmt.Printf("routine %d send msg: %d\n", routine_number, msg_number)
		if *argTimeSleep == 0 {
			continue
		}

		if *argTimeSleep != 0 {
			time.Sleep(time.Nanosecond * time.Duration(*argTimeSleep))
		}
//		a := 100 * (*argTimeSleep)

	}
}

func main() {
	flag.Parse()
	startTime := time.Now()
	if *argRole == "P" {
		for i := 1; i < *argRoutineNumber; i++ {
			p := CreateSyncProducer(*argKafkaHost)
			go WriteMsg(p, i)
		}
		p := CreateSyncProducer(*argKafkaHost)
		if p == nil {
			return
		}
		WriteMsg(p, *argRoutineNumber)
	}
	if *argRole == "C" {
		for i := 0; i < *argPartitionNumber - 1; i++ {
			c := CreateCunsumer(*argKafkaHost)
			go GetData(c, int32(i))
		}
		c := CreateCunsumer(*argKafkaHost)
		GetData(c, int32(*argPartitionNumber - 1))
	}
	endTime := time.Now()
	fmt.Printf("%d routines each send %d cost: %f\n", *argRoutineNumber, * argTotalMsgNum, endTime.Sub(startTime).Seconds())
}