package dgnats_test

import (
	"encoding/json"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	dgnats "github.com/darwinOrg/go-nats"
	"testing"
	"time"
)

type TestStruct struct {
	Content string `json:"content"`
}

var testSubject = &dgnats.NatsSubject{
	Category: "test",
	Name:     "test",
	Group:    "group",
}

var testDelaySubject = &dgnats.NatsSubject{
	Category: "test",
	Name:     "test-delay",
	Group:    "group-delay",
}

func TestNats(t *testing.T) {
	ctx := dgctx.SimpleDgContext()

	err := dgnats.Connect(&dgnats.NatsConfig{
		PoolSize:       10,
		Servers:        []string{"nats://localhost:4222"},
		ConnectionName: "startrek_mq",
		Username:       "startrek_mq",
		Password:       "cswjggljrmpypwfccarzpjxG-urepqldkhecvnzxzmngotaqs-bkwdvjgipruectqcowoqb6nj",
	})
	if err != nil {
		dglogger.Panicf(ctx, "connect nats error: %v", err)
		return
	}
	defer dgnats.Close()

	dgnats.SubscribeRawWithTag(ctx, testSubject, "tag1", func(ctx *dgctx.DgContext, data []byte) error {
		jsonBytes, _ := json.Marshal(string(data))
		dglogger.Infof(ctx, "handle message raw1: %s", string(jsonBytes))
		return nil
	})

	dgnats.SubscribeRawWithTag(ctx, testSubject, "tag2", func(ctx *dgctx.DgContext, data []byte) error {
		jsonBytes, _ := json.Marshal(string(data))
		dglogger.Infof(ctx, "handle message raw2: %s", string(jsonBytes))
		return nil
	})

	dgnats.SubscribeJson(ctx, testSubject, func(ctx *dgctx.DgContext, s *TestStruct) error {
		jsonBytes, _ := json.Marshal(s)
		dglogger.Infof(ctx, "handle message json: %s", string(jsonBytes))
		return nil
	})

	dgnats.SubscribeJsonDelay(ctx, testDelaySubject, time.Second*1, func(ctx *dgctx.DgContext, s *TestStruct) error {
		jsonBytes, _ := json.Marshal(s)
		dglogger.Infof(ctx, "handle delay message: %s", string(jsonBytes))
		return nil
	})

	err = dgnats.PublishJson(ctx, testSubject, &TestStruct{Content: "123"})
	if err != nil {
		dglogger.Errorf(ctx, "publish message error: %v", err)
		return
	}

	err = dgnats.PublishJsonDelay(ctx, testDelaySubject, &TestStruct{Content: "456"}, time.Second*3)
	if err != nil {
		dglogger.Errorf(ctx, "publish delay message error: %v", err)
		return
	}

	time.Sleep(time.Second * 5)
}
