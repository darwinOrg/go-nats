package dgnats_test

import (
	"encoding/json"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	dgnats "github.com/darwinOrg/go-nats"
	"github.com/google/uuid"
	"testing"
	"time"
)

type TestStruct struct {
	Content string `json:"content"`
}

var testSubject = &dgnats.Subject{
	Name:  "test",
	Queue: "queue",
}

func TestNats(t *testing.T) {
	dgnats.Connect(&dgnats.NatsConfig{
		PoolSize:       10,
		Servers:        []string{"nats://localhost:4222"},
		ConnectionName: "startrek-mq",
		Username:       "startrek_mq",
		Password:       "cswjggljrmpypwfccarzpjxG-urepqldkhecvnzxzmngotaqs-bkwdvjgipruectqcowoqb6nj",
	})

	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}

	dgnats.SubscribeJson[TestStruct](testSubject, func(ctx *dgctx.DgContext, s *TestStruct) {
		jsonBytes, _ := json.Marshal(s)
		dglogger.Infof(ctx, "handle message: %s", string(jsonBytes))
	})

	time.Sleep(time.Second * 3)

	err := dgnats.PublishJson[TestStruct](ctx, testSubject, &TestStruct{Content: "123456"})
	if err != nil {
		dglogger.Errorf(ctx, "publish error: %v", err)
		return
	}

	time.Sleep(time.Second * 3)

	dgnats.Close()
}
