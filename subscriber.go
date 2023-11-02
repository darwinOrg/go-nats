package dgnats

import (
	"encoding/json"
	"github.com/darwinOrg/go-common/constants"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"strconv"
	"time"
)

func SubscribeJson[T any](subject *Subject, workFn func(*dgctx.DgContext, *T)) {
	_, err := natsConn.Subscribe(subject.Name, func(msg *nats.Msg) {
		subscribeJson(subject.Name, msg, workFn)
	})

	if err != nil {
		logrus.Panicf("subscribe subject[%s] error: %v", subject.Name, err)
	}
}

func QueueSubscribeJson[T any](subject *Subject, workFn func(*dgctx.DgContext, *T)) {
	_, err := natsConn.QueueSubscribe(subject.Name, subject.Queue, func(msg *nats.Msg) {
		subscribeJson(subject.Name+"-"+subject.Queue, msg, workFn)
	})

	if err != nil {
		logrus.Panicf("queue subscribe subject[%s, %s] error: %v", subject.Name, subject.Queue, err)
	}
}

func subscribeJson[T any](name string, msg *nats.Msg, workFn func(*dgctx.DgContext, *T)) {
	ctx := buildDgContextFromMsg(msg)
	if delayQueueUnreachedTime(ctx, msg) {
		return
	}
	dglogger.Infof(ctx, "[%s] receive json message: %s", name, string(msg.Data))
	t := new(T)
	err := json.Unmarshal(msg.Data, t)
	if err != nil {
		dglogger.Errorf(ctx, "unmarshal json[%s] error: %v", msg.Data, err)
		return
	}

	workFn(ctx, t)
}

func SubscribeRaw(subject *Subject, workFn func(*dgctx.DgContext, []byte)) {
	_, err := natsConn.Subscribe(subject.Name, func(msg *nats.Msg) {
		subscribeRaw(msg, workFn)
	})

	if err != nil {
		logrus.Panicf("subscribe subject[%s] error: %v", subject.Name, err)
	}
}

func QueueSubscribeRaw(subject *Subject, workFn func(*dgctx.DgContext, []byte)) {
	_, err := natsConn.QueueSubscribe(subject.Name, subject.Queue, func(msg *nats.Msg) {
		subscribeRaw(msg, workFn)
	})

	if err != nil {
		logrus.Panicf("queue subscribe subject[%s, %s] error: %v", subject.Name, subject.Queue, err)
	}
}

func subscribeRaw(msg *nats.Msg, workFn func(*dgctx.DgContext, []byte)) {
	ctx := buildDgContextFromMsg(msg)
	if delayQueueUnreachedTime(ctx, msg) {
		return
	}
	workFn(ctx, msg.Data)
}

func buildDgContextFromMsg(msg *nats.Msg) *dgctx.DgContext {
	traceIdHeader := msg.Header[constants.TraceId]
	var traceId string
	if len(traceIdHeader) > 0 {
		traceId = msg.Header[constants.TraceId][0]
	}
	if traceId == "" {
		traceId = uuid.NewString()
	}
	return &dgctx.DgContext{TraceId: traceId}
}

func delayQueueUnreachedTime(ctx *dgctx.DgContext, msg *nats.Msg) bool {
	delayHeader := msg.Header[headerDelay]
	if len(delayHeader) == 0 {
		return false
	}

	delay, _ := strconv.ParseInt(msg.Header[headerDelay][0], 10, 64)
	pubAt, _ := strconv.ParseInt(msg.Header[headerPubAt][0], 10, 64)
	now := time.Now().UnixMilli()
	if now <= pubAt+delay {
		dglogger.Debug(ctx, "not due, nak")
		msg.NakWithDelay(time.Duration(delay))
		return true
	}

	return false
}
