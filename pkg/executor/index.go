package executor

import (
	"fmt"
	"nats_vs_kafka/utils"
	"time"

	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
	kafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	nc *nats.Conn
	// generate fake Data
	groupId                = "group1"
	taskBytesBase64 []byte = []byte("ThisIsFakeData 2021.0520.1314 !!!!")
)

func Test_Producer_Nats(id uint8, addr string, number uint64) {
	if addr == "" {
		addr = nats.DefaultURL
	}
	// Connect to a server
	nc, err := nats.Connect(addr)
	defer nc.Close()
	if err != nil {
		utils.Logger.Errorln(fmt.Sprintf("Nats Connect Error[%v]", err))
		panic(fmt.Sprintf("Nats Connect Error[%v]", err))
	}

	timeStart := time.Now()

	for count := uint64(0); count < number; count++ {
		err = nc.Publish(utils.Topics["nats"], taskBytesBase64)
		if err != nil {
			utils.Logger.Errorln(err)
		}
	}
	timeEnd := time.Now()

	utils.Logger.Infof("id[%v] Producer Nats generate msg count[%v] timeStart[%v] timeEnd[%v] timeDiff[%v]", id, number, timeStart.Format("20060102-150405.999999"), timeEnd.Format("20060102-150405.999999"), timeEnd.Sub(timeStart))
}

func Test_Producer_Nats_Replay(id uint8, addr string, number uint64) {
	if addr == "" {
		addr = nats.DefaultURL
	}
	// Connect to a server
	nc, err := nats.Connect(addr)
	if err != nil {
		utils.Logger.Errorln(fmt.Sprintf("Nats Connect Error[%v]", err))
		panic(fmt.Sprintf("Nats Connect Error[%v]", err))
	}
	defer nc.Close()

	sc, err := stan.Connect("test-cluster", "client-p-"+fmt.Sprintf("%v", id), stan.NatsConn(nc))
	if err != nil {
		utils.Logger.Errorln(fmt.Sprintf("Nats Connect sc Error[%v]", err))
		panic(fmt.Sprintf("Nats Connect sc Error[%v]", err))
	}
	defer sc.Close()

	timeStart := time.Now()

	ah := func(nuid string, err error) {
		// process the ack
	}

	for count := uint64(0); count < number; count++ {
		guid, err := sc.PublishAsync(utils.Topics["nats"], taskBytesBase64, ah)
		//err = sc.Publish(utils.Topics["nats"], taskBytesBase64)
		if err != nil {
			utils.Logger.Errorln(guid, err)
		}
	}
	timeEnd := time.Now()

	utils.Logger.Infof("id[%v] Producer Nats generate msg count[%v] timeStart[%v] timeEnd[%v] timeDiff[%v]", id, number, timeStart.Format("20060102-150405.999999"), timeEnd.Format("20060102-150405.999999"), timeEnd.Sub(timeStart))
}

func Test_Consumer_Nats(id uint8, addr string, number uint64) {
	if addr == "" {
		addr = nats.DefaultURL
	}
	nc, err := nats.Connect(addr)
	defer nc.Close()
	if err != nil {
		utils.Logger.Errorln(fmt.Sprintf("Nats Connect Error[%v]", err))
		panic(fmt.Sprintf("Nats Connect Error[%v]", err))
	}

	timeStart := time.Now()
	stopSig := make(chan bool, 1)

	count := uint64(0)
	//nc.QueueSubscribe(utils.Topics["nats"], groupId, func(msg *nats.Msg) {
	nc.Subscribe(utils.Topics["nats"], func(msg *nats.Msg) {
		count += 1
		if count == number {
			timeEnd := time.Now()
			utils.Logger.Infof("id[%v] Consumer Nats get msg count[%v] timeStart[%v] timeEnd[%v] timeDiff[%v]", id, number, timeStart.Format("20060102-150405.999999"), timeEnd.Format("20060102-150405.999999"), timeEnd.Sub(timeStart))
			stopSig <- false
		}
	})

	// wait for receiving msg
	<-stopSig
}

func Test_Consumer_Nats_Replay(id uint8, addr string, number uint64) {
	if addr == "" {
		addr = nats.DefaultURL
	}
	nc, err := nats.Connect(addr)
	defer nc.Close()
	if err != nil {
		utils.Logger.Errorln(fmt.Sprintf("Nats Connect Error[%v]", err))
		panic(fmt.Sprintf("Nats Connect Error[%v]", err))
	}

	sc, err := stan.Connect("test-cluster", "client-c-"+fmt.Sprintf("%v", id), stan.NatsConn(nc))
	defer sc.Close()
	if err != nil {
		utils.Logger.Errorln(fmt.Sprintf("Nats Connect sc Error[%v]", err))
		panic(fmt.Sprintf("Nats Connect sc Error[%v]", err))
	}

	timeStart := time.Now()
	stopSig := make(chan bool, 1)

	parseDuration, _ := time.ParseDuration("30m")
	count := uint64(0)
	//sc.QueueSubscribe(utils.Topics["nats"], groupId, func(msg *nats.Msg) {
	sc.Subscribe(utils.Topics["nats"], func(msg *stan.Msg) {
		count += 1
		if count == number {
			timeEnd := time.Now()
			utils.Logger.Infof("id[%v] ConsumerReplay Nats get msg count[%v] timeStart[%v] timeEnd[%v] timeDiff[%v]", id, number, timeStart.Format("20060102-150405.999999"), timeEnd.Format("20060102-150405.999999"), timeEnd.Sub(timeStart))
			stopSig <- false
		}
	}, stan.StartAtTimeDelta(parseDuration))

	// wait for receiving msg
	<-stopSig
}

func Test_Producer_Kafka(id uint8, addr string, number uint64) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": addr})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := utils.Topics["kafka"]
	timeStart := time.Now()
	for count := uint64(0); count < number; count++ {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          taskBytesBase64,
		}, nil)
	}
	timeEnd := time.Now()
	utils.Logger.Infof("id[%v] Producer Kafka generate msg count[%v] timeStart[%v] timeEnd[%v] timeDiff[%v]", id, number, timeStart.Format("20060102-150405.999999"), timeEnd.Format("20060102-150405.999999"), timeEnd.Sub(timeStart))

	// Wait for message deliveries before shutting down
	p.Flush(5 * 1000)
}

func Test_Consumer_Kafka(id uint8, addr string, number uint64) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": addr,
		"group.id":          groupId,
		"auto.offset.reset": "latest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{utils.Topics["kafka"]}, nil)

	timeStart := time.Now()
	for count := uint64(0); count < number; count++ {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			// The client will automatically try to recover from all errors.
			utils.Logger.Errorf("Consumer error: %v (%v)\n", err, msg)
		}
	}
	timeEnd := time.Now()
	utils.Logger.Infof("id[%v] Consumer Kafka get msg count[%v] timeStart[%v] timeEnd[%v] timeDiff[%v]", id, number, timeStart.Format("20060102-150405.999999"), timeEnd.Format("20060102-150405.999999"), timeEnd.Sub(timeStart))

	c.Close()
}
