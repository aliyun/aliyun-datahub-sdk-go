package main

import (
	"fmt"
	"reflect"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func offset_consumption() {

	// add config to examples client
	config := &datahub.Config{
		// create a datahubClint  support binary transmission
		EnableBinary: true,
		//use lz4 compress data
		CompressorType: datahub.LZ4,
	}

	account := datahub.NewAliyunAccount(accessId, accessKey)
	dh := datahub.NewClientWithConfig(endpoint, config, account)

	shardId := "3"
	// add your want to open shardId
	shardIds := []string{"0", "1", "2", "3"}
	session, err := dh.OpenSubscriptionSession(projectName, topicName, subId, shardIds)
	if err != nil {
		fmt.Println("open session failed")
		return
	}

	// check if shardId is open
	so, ok := session.Offsets[shardId]
	if !ok {
		fmt.Printf("shardId %s can not open session", shardId)
		return
	}
	var cursor string
	// get the next cursor that last committed,so sequence+1
	gc, err := dh.GetCursor(projectName, topicName, shardId, datahub.SEQUENCE, so.Sequence+1)
	if err != nil {
		if _, ok := err.(*datahub.SeekOutOfRangeError); ok {
			fmt.Println("sequence is seek out of range, it maybe Expired")
			// sequence invalid,get data from the oldest cursor of valid data
			gc1, err := dh.GetCursor(projectName, topicName, shardId, datahub.OLDEST)
			if err != nil {
				return
			}
			cursor = gc1.Cursor
		} else {
			fmt.Println("get cursor failed")
			fmt.Println(err)
			return
		}
	} else {
		cursor = gc.Cursor
	}

	// get topic from the topic
	topic, err := dh.GetTopic(projectName, topicName)
	if err != nil {
		fmt.Println("get topic failed")
		return
	}
	// get the num of data
	limitNum := 1000
	var readNum int64 = 0
	for {
		res, err := dh.GetTupleRecords(projectName, topicName, shardId, cursor, limitNum, topic.RecordSchema)
		if err != nil {
			fmt.Println("get error failed")
			return
		}
		// no data ,read later
		if res.RecordCount <= 0 {
			time.Sleep(time.Second * 5)
			fmt.Println("no data now, wait 5 seconds")
		}

		for _, record := range res.Records {
			tRecord, ok := record.(*datahub.TupleRecord)
			if !ok {
				fmt.Printf("the record type is %v,not a TupleRecord", reflect.TypeOf(record))
				return
			}
			// consume data
			fmt.Println(tRecord.Values)

			so.Sequence = record.GetSequence()
			so.Timestamp = record.GetSystemTime()
			readNum++
			if readNum%100 == 0 {

				ms := map[string]datahub.SubscriptionOffset{
					shardId: so,
				}
				if _, err := dh.CommitSubscriptionOffset(projectName, topicName, subId, ms); err != nil {
					if _, ok := err.(*datahub.SubscriptionOffsetResetError); ok {
						fmt.Println("subscription is reset in elsewhere")
						return
					} else if _, ok := err.(*datahub.SubscriptionSessionInvalidError); ok {
						fmt.Println("subscription is initialized in elsewhere")
						return
					} else {
						fmt.Println(err)
						return
					}
				}
				fmt.Println("commit offset successful")
				time.Sleep(time.Second * 5)
			}
		}
		cursor = res.NextCursor
	}
}
