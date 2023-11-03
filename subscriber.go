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

const SubWorkErrorRetryWait = time.Second * 5

var DefaultSubOpts = []nats.SubOpt{
	nats.AckExplicit(),
	nats.ManualAck(),
	nats.DeliverLast(),
}

var subjectCache = map[string]*NatsSubject{}

func SubscribeJson[T any](subject *NatsSubject, workFn func(*dgctx.DgContext, *T) error) {
	_, err := natsJs.Subscribe(subject.Name, func(msg *nats.Msg) {
		subscribeJson(subject.Name, msg, workFn)
	}, buildSubOpts(subject)...)

	if err != nil {
		logrus.Panicf("subscribe subject[%s] error: %v", subject.Name, err)
	}
}

func QueueSubscribeJson[T any](subject *NatsSubject, workFn func(*dgctx.DgContext, *T) error) {
	_, err := natsJs.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
		subscribeJson(subject.Name+"-"+subject.Group, msg, workFn)
	}, buildSubOpts(subject)...)

	if err != nil {
		logrus.Panicf("queue subscribe subject[%s, %s] error: %v", subject.Name, subject.Group, err)
	}
}

func subscribeJson[T any](name string, msg *nats.Msg, workFn func(*dgctx.DgContext, *T) error) {
	ctx := buildDgContextFromMsg(msg)
	dglogger.Infof(ctx, "[%s] receive json message: %s", name, string(msg.Data))
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
	_, err := natsJs.Subscribe(subject.Name, func(msg *nats.Msg) {
		subscribeRaw(msg, workFn)
	}, buildSubOpts(subject)...)

	if err != nil {
		logrus.Panicf("subscribe subject[%s] error: %v", subject.Name, err)
	}
}

func QueueSubscribeRaw(subject *NatsSubject, workFn func(*dgctx.DgContext, []byte) error) {
	_, err := natsJs.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
		subscribeRaw(msg, workFn)
	}, buildSubOpts(subject)...)

	if err != nil {
		logrus.Panicf("queue subscribe subject[%s, %s] error: %v", subject.Name, subject.Group, err)
	}
}

func subscribeRaw(msg *nats.Msg, workFn func(*dgctx.DgContext, []byte) error) {
	ctx := buildDgContextFromMsg(msg)
	err := workFn(ctx, msg.Data)
	ackOrNakByError(msg, err)
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

func SubscribeJsonDelay[T any](subject *NatsSubject, sleepDuration time.Duration, workFn func(*dgctx.DgContext, *T) error) {
	_, err := natsJs.Subscribe(subject.Name, func(msg *nats.Msg) {
		subscribeJsonDelay[T](msg, subject, sleepDuration, workFn)
	}, buildSubOpts(subject)...)

	if err != nil {
		logrus.Panicf("subscribe subject[%s] error: %v", subject.Name, err)
	}
}

func QueueSubscribeJsonDelay[T any](subject *NatsSubject, sleepDuration time.Duration, workFn func(*dgctx.DgContext, *T) error) {
	_, err := natsJs.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
		subscribeJsonDelay[T](msg, subject, sleepDuration, workFn)
	}, buildSubOpts(subject)...)

	if err != nil {
		logrus.Panicf("queue subscribe subject[%s] error: %v", subject.Name, err)
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

func buildSubOpts(subject *NatsSubject) []nats.SubOpt {
	var subOpts []nats.SubOpt
	for _, opt := range DefaultSubOpts {
		subOpts = append(subOpts, opt)
	}

	subOpts = append(subOpts, nats.Durable(subject.GetDurable()))
	// TODO
	//subOpts = append(subOpts, nats.BindStream(subject.Category))
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
