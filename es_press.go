package main

import (
	"flag"
	"fmt"
	"time"

	"gopkg.in/olivere/elastic.v2"
)

var argEsUrl = flag.String("es_url", "", "Elasticsearch url")
var argBulkSize = flag.Int("bulk_size", 12000, "Bluk size")
var routineNum = flag.Int("routine_number", 10, "Go routine number")
var argIndexNum = flag.Int("index_number", 1, "index number")

type LogData struct {
	Created_at     int64  `json:"time"`
	Machine        string `json:"machine"`
	App_id         string `json:"app_id"`
	Instance_id    string `json:"instance_id"`
	Container_name string `json:"container_name"`
	Log_data       string `json:"log_data"`
	Log_type       string `json:"log_type"`
	Log_Detail     string `json:"log_detail"`
	Log_Level      int8   `json:"log_level"`
}

func GetEsClient(url string) (*elastic.Client, error) {
	client, err := elastic.NewClient(elastic.SetSniff(false),
		elastic.SetHealthcheckTimeoutStartup(1*time.Second), elastic.SetURL(url))
	if err != nil {
		fmt.Printf("Create Elasticsearch fail! error: %v\n", err)
		return nil, err
	}
	return client, nil
}

func CreateLogdata(log_number int64, route int64) *LogData {
	nano := time.Now().Nanosecond()
	timestamp := time.Now().Unix()
	return &LogData{
		Created_at: int64(timestamp*1E6) + (int64(nano) / 1E3),
		Machine:    "127.0.0.1",
		Log_type:   "stdout",
		Log_data: fmt.Sprintf("This is useless sentence just for extend the log length-1."+
			"This is useless sentence just for extend the log length-2 "+
			"This is useless sentence just for extend the log length-3"+
			"This is useless sentence just for extend the log length-4"+
			"This is useless sentence just for extend the log length-5"+
			"This is useless sentence just for extend the log length-6"+
			"This is useless sentence just for extend the log length-7"+
			"This is useless sentence just for extend the log length-8"+
			"This is useless sentence just for extend the log length-9"+
			"route: %d Create log data: data number %d\n", route, log_number+int64(1)),
		Container_name: "test",
		App_id:         "76964db2_1022_4ba0_98b3_6b281214b007",
		Instance_id:    "8c713d9c_c630_11e6_b9df_2e568da87f13",
		Log_Detail:     "",
	}
}

func InsertData(client *elastic.Client, route int64, index_id int) error {
	bulkRequest := client.Bulk()
	totalLogNumber := int64(0)
	for {
		startTime := time.Now()
		for i := 0; i < *argBulkSize; i++ {
			log_data := CreateLogdata(totalLogNumber, route)
			totalLogNumber += int64(1)
			es_index := fmt.Sprintf("log-2016122%d", index_id)
			//			timestamp := time.Now().Unix()
			//            indexNum := fmt.Sprintf("%d", timestamp * 1E9 + int64(time.Now().Nanosecond()))
			new_index := elastic.NewBulkIndexRequest().Index(es_index).Type("log").Doc(*log_data)
			bulkRequest = bulkRequest.Add(new_index)
		}
		_, err := bulkRequest.Do()
		if err != nil {
			fmt.Printf("Bulk fail! error: %v\n", err)
		}
		endTime := time.Now()
		fmt.Printf("Bulk %d %d  cost time: %d\n", route, totalLogNumber, endTime.Sub(startTime).Seconds())
		//		time.Sleep(time.Nanosecond * 1E8)
	}
	return nil
}

func main() {
	flag.Parse()
	url := *argEsUrl
	if len(url) == 0 {
		fmt.Printf("Please spec --es_url\n")
		fmt.Printf("Right now url is: %s\n", *argEsUrl)
		return
	}

	for index_num := 1; index_num < *argIndexNum; index_num++ {
		fmt.Printf("Create index: %d\n", index_num)
		for i := 0; i < *routineNum; i++ {
			client, err := GetEsClient(url)
			if err != nil {
				fmt.Printf("Es fail! %d. err: %v", 1*(*argIndexNum)+i, err)
				return
			}
			go InsertData(client, int64(1*(*argIndexNum)+i), index_num)
		}
	}
	for i := 1; i < *routineNum; i++ {
		client, err := GetEsClient(url)
		if err != nil {
			fmt.Printf("Es fail! %d", i)
			return
		}
		go InsertData(client, int64(i), *argIndexNum)
	}
	client, _ := GetEsClient(url)
	InsertData(client, int64(*routineNum), *argIndexNum)
}
