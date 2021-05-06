package executor

import (
	"encoding/base64"
	"fmt"
	"nats_vs_kafka/utils"
	"time"

	nats "github.com/nats-io/nats.go"
)

var (
	nc *nats.Conn
)

func TestNatsProducer(addr string) {
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

	// generate fake TaskMeta
	// encoding to base64
	var taskBytesBase64 []byte
	err = nc.PublishRequest(utils.TaskTopics["kafka"], "ok", taskBytesBase64)
	if err != nil {
		utils.Logger.Errorln(err)
	}

	time.Sleep(time.Millisecond * 10)
	utils.Logger.Infof("Nats Producer2")
	err = nc.PublishRequest(utils.TaskTopics["kafka"], "ok", taskBytesBase64)
	if err != nil {
		utils.Logger.Errorln(err)
	}

	time.Sleep(time.Millisecond * 10)
	utils.Logger.Infof("Nats Producer3")
	err = nc.PublishRequest(utils.TaskTopics["kafka"], "ok", taskBytesBase64)
	if err != nil {
		utils.Logger.Errorln(err)
	}
}

func TestNatsAlertConsumer(addr string) {
	if addr == "" {
		addr = nats.DefaultURL
	}
	nc, err := nats.Connect(addr)
	defer nc.Close()
	if err != nil {
		utils.Logger.Errorln(fmt.Sprintf("Nats Connect Error[%v]", err))
		panic(fmt.Sprintf("Nats Connect Error[%v]", err))
	}

	nc.QueueSubscribe(utils.TaskTopics["nats"], "alertTestGroup", func(msg *nats.Msg) {
		utils.Logger.Infof("TestNatsAlertConsumer get msg[%v]", msg.Data)
	})

	// wait 24 hours for receiving msg
	time.Sleep(time.Hour * 24)
}
