package dgnats

import (
	"encoding/json"
	"github.com/darwinOrg/go-common/constants"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"strconv"
	"time"
)

const SubWorkErrorRetryWait = time.Second * 5

var DefaultSubOpts = []nats.SubOpt{
	nats.AckExplicit(),
	nats.ManualAck(),
	nats.DeliverLast(),
}

func SubscribeJson[T any](subject *NatsSubject, workFn func(*dgctx.DgContext, *T) error) {
	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	err := initStream(ctx, subject)
	if err != nil {
		return
	}

	if subject.Group != "" {
		_, err = natsJs.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
			subscribeJson(msg, workFn)
		}, buildSubOpts(subject)...)
	} else {
		_, err = natsJs.Subscribe(subject.Name, func(msg *nats.Msg) {
			subscribeJson(msg, workFn)
		}, buildSubOpts(subject)...)
	}

	if err != nil {
		dglogger.Panicf(ctx, "subscribe subject[%s] error: %v", subject.Name, err)
	}
}

func subscribeJson[T any](msg *nats.Msg, workFn func(*dgctx.DgContext, *T) error) {
	ctx := buildDgContextFromMsg(msg)
	dglogger.Infof(ctx, "[%s] receive json message: %s", msg.Subject, string(msg.Data))
	t := new(T)
	err := json.Unmarshal(msg.Data, t)
	if err != nil {
		dglogger.Errorf(ctx, "unmarshal json[%s] error: %v", msg.Data, err)
		msg.AckSync()
		return
	}

	workAndAck(ctx, msg, t, workFn)
}

func SubscribeRaw(subject *NatsSubject, workFn func(*dgctx.DgContext, []byte) error) {
	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	err := initStream(ctx, subject)
	if err != nil {
		return
	}

	if subject.Group != "" {
		_, err = natsJs.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
			subscribeRaw(msg, workFn)
		}, buildSubOpts(subject)...)
	} else {
		_, err = natsJs.Subscribe(subject.Name, func(msg *nats.Msg) {
			subscribeRaw(msg, workFn)
		}, buildSubOpts(subject)...)
	}

	if err != nil {
		dglogger.Panicf(ctx, "subscribe subject[%s] error: %v", subject.Name, err)
	}
}

func subscribeRaw(msg *nats.Msg, workFn func(*dgctx.DgContext, []byte) error) {
	ctx := buildDgContextFromMsg(msg)
	err := workFn(ctx, msg.Data)
	ackOrNakByError(msg, err)
}

func SubscribeJsonDelay[T any](subject *NatsSubject, sleepDuration time.Duration, workFn func(*dgctx.DgContext, *T) error) {
	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	err := initStream(ctx, subject)
	if err != nil {
		return
	}

	if subject.Group != "" {
		_, err = natsJs.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
			subscribeJsonDelay[T](msg, subject, sleepDuration, workFn)
		}, buildSubOpts(subject)...)
	} else {
		_, err = natsJs.Subscribe(subject.Name, func(msg *nats.Msg) {
			subscribeJsonDelay[T](msg, subject, sleepDuration, workFn)
		}, buildSubOpts(subject)...)
	}

	if err != nil {
		dglogger.Panicf(ctx, "subscribe subject[%s] error: %v", subject.Name, err)
	}
}

func subscribeJsonDelay[T any](msg *nats.Msg, subject *NatsSubject, sleepDuration time.Duration, workFn func(*dgctx.DgContext, *T) error) {
	delayHeader := msg.Header[headerDelay]
	if len(delayHeader) == 0 {
		msg.AckSync()
		return
	}

	ctx := buildDgContextFromMsg(msg)
	delay, _ := strconv.ParseInt(msg.Header[headerDelay][0], 10, 64)
	pubAt, _ := strconv.ParseInt(msg.Header[headerPubAt][0], 10, 64)
	now := time.Now().UnixMilli()

	if now <= pubAt+delay {
		dglogger.Debug(ctx, "not due, nak")
		msg.NakWithDelay(time.Duration(delay))
		time.Sleep(sleepDuration)
		return
	}

	data := msg.Data
	dglogger.Infof(ctx, "[%s] receive delay json message: %s", subject.Name, data)
	t := new(T)
	err := json.Unmarshal(data, t)
	if err != nil {
		dglogger.Errorf(ctx, "unmarshal json[%s] error: %v", data, err)
		msg.AckSync()
		return
	}

	workAndAck(ctx, msg, t, workFn)
}

func Unsubscribe(subject *NatsSubject) error {
	return natsJs.DeleteConsumer(subject.Category, subject.GetDurable())
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

func buildSubOpts(subject *NatsSubject) []nats.SubOpt {
	var subOpts []nats.SubOpt
	for _, opt := range DefaultSubOpts {
		subOpts = append(subOpts, opt)
	}

	subOpts = append(subOpts, nats.Durable(subject.GetDurable()))
	subOpts = append(subOpts, nats.BindStream(subject.Category))
	return subOpts
}

func workAndAck[T any](ctx *dgctx.DgContext, msg *nats.Msg, t *T, workFn func(*dgctx.DgContext, *T) error) {
	err := workFn(ctx, t)
	ackOrNakByError(msg, err)
}

func ackOrNakByError(msg *nats.Msg, err error) {
	if err != nil {
		msg.NakWithDelay(SubWorkErrorRetryWait)
	} else {
		msg.AckSync()
	}
}
