package dgnats

import (
	"encoding/json"
	"github.com/darwinOrg/go-common/constants"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"strconv"
	"time"
)

var (
	SubWorkErrorRetryWait = time.Second * 5
	MaxAckPendingCount    = 1000

	DefaultSubOpts = []nats.SubOpt{
		nats.AckExplicit(),
		nats.ManualAck(),
		nats.DeliverLast(),
		nats.MaxAckPending(MaxAckPendingCount),
	}
)

func SubscribeJson[T any](ctx *dgctx.DgContext, subject *NatsSubject, workFn func(*dgctx.DgContext, *T) error) (*nats.Subscription, error) {
	if ctx == nil {
		ctx = dgctx.SimpleDgContext()
	}
	err := InitStream(ctx, subject)
	if err != nil {
		return nil, err
	}

	js, err := GetJs()
	if err != nil {
		dglogger.Errorf(ctx, "get jet stream error: %v", err)
		return nil, err
	}

	var sub *nats.Subscription
	if subject.Group != "" {
		sub, err = js.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
			subscribeJson(msg, workFn)
		}, buildSubOpts(subject, "")...)
	} else {
		sub, err = js.Subscribe(subject.Name, func(msg *nats.Msg) {
			subscribeJson(msg, workFn)
		}, buildSubOpts(subject, "")...)
	}
	if err != nil {
		dglogger.Errorf(ctx, "subscribe subject[%s] error: %v", subject.Name, err)
		return nil, err
	}

	return sub, nil
}

func subscribeJson[T any](msg *nats.Msg, workFn func(*dgctx.DgContext, *T) error) {
	ctx := buildDgContextFromMsg(msg)
	dglogger.Infof(ctx, "[%s] receive json message: %s", msg.Subject, string(msg.Data))
	t := new(T)
	err := json.Unmarshal(msg.Data, t)
	if err != nil {
		dglogger.Errorf(ctx, "unmarshal json[%s] error: %v", msg.Data, err)
		ase := msg.AckSync()
		if ase != nil {
			dglogger.Errorf(ctx, "msg.AckSync error: %v", ase)
		}
		return
	}

	workAndAck(ctx, msg, t, workFn)
}

func SubscribeRaw(ctx *dgctx.DgContext, subject *NatsSubject, workFn func(*dgctx.DgContext, []byte) error) (*nats.Subscription, error) {
	if ctx == nil {
		ctx = dgctx.SimpleDgContext()
	}
	err := InitStream(ctx, subject)
	if err != nil {
		return nil, err
	}

	js, err := GetJs()
	if err != nil {
		dglogger.Errorf(ctx, "get jet stream error: %v", err)
		return nil, err
	}

	var sub *nats.Subscription
	if subject.Group != "" {
		sub, err = js.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
			subscribeRaw(msg, workFn)
		}, buildSubOpts(subject, "")...)
	} else {
		sub, err = js.Subscribe(subject.Name, func(msg *nats.Msg) {
			subscribeRaw(msg, workFn)
		}, buildSubOpts(subject, "")...)
	}
	if err != nil {
		dglogger.Errorf(ctx, "subscribe subject[%s] error: %v", subject.Name, err)
		return nil, err
	}

	return sub, nil
}

func subscribeRaw(msg *nats.Msg, workFn func(*dgctx.DgContext, []byte) error) {
	ctx := buildDgContextFromMsg(msg)
	err := workFn(ctx, msg.Data)
	ackOrNakByError(msg, err)
}

func SubscribeRawWithTag(ctx *dgctx.DgContext, subject *NatsSubject, tag string, workFn func(*dgctx.DgContext, []byte) error) (*nats.Subscription, error) {
	if tag == "" {
		return SubscribeRaw(ctx, subject, workFn)
	}

	if ctx == nil {
		ctx = dgctx.SimpleDgContext()
	}
	err := InitStream(ctx, subject)
	if err != nil {
		return nil, err
	}

	js, err := GetJs()
	if err != nil {
		dglogger.Errorf(ctx, "get jet stream error: %v", err)
		return nil, err
	}

	var sub *nats.Subscription
	if subject.Group != "" {
		sub, err = js.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
			subscribeRawWithTag(msg, tag, workFn)
		}, buildSubOpts(subject, tag)...)
	} else {
		sub, err = js.Subscribe(subject.Name, func(msg *nats.Msg) {
			subscribeRawWithTag(msg, tag, workFn)
		}, buildSubOpts(subject, tag)...)
	}

	if err != nil {
		dglogger.Errorf(ctx, "subscribe subject[%s] error: %v", subject.Name, err)
		return nil, err
	}

	return sub, nil
}

func subscribeRawWithTag(msg *nats.Msg, tag string, workFn func(*dgctx.DgContext, []byte) error) {
	if len(msg.Header) == 0 {
		return
	}

	header := msg.Header
	if ts, ok := header[headerTag]; !ok || (len(ts) > 0 && ts[0] == tag) {
		subscribeRaw(msg, workFn)
	}
}

func SubscribeJsonDelay[T any](ctx *dgctx.DgContext, subject *NatsSubject, sleepDuration time.Duration, workFn func(*dgctx.DgContext, *T) error) (*nats.Subscription, error) {
	if ctx == nil {
		ctx = dgctx.SimpleDgContext()
	}
	err := InitStream(ctx, subject)
	if err != nil {
		return nil, err
	}

	js, err := GetJs()
	if err != nil {
		dglogger.Errorf(ctx, "get jet stream error: %v", err)
		return nil, err
	}

	var sub *nats.Subscription
	if subject.Group != "" {
		sub, err = js.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
			subscribeJsonDelay[T](msg, subject, sleepDuration, workFn)
		}, buildSubOpts(subject, "")...)
	} else {
		sub, err = js.Subscribe(subject.Name, func(msg *nats.Msg) {
			subscribeJsonDelay[T](msg, subject, sleepDuration, workFn)
		}, buildSubOpts(subject, "")...)
	}

	if err != nil {
		dglogger.Errorf(ctx, "subscribe subject[%s] error: %v", subject.Name, err)
		return nil, err
	}

	return sub, nil
}

func subscribeJsonDelay[T any](msg *nats.Msg, subject *NatsSubject, sleepDuration time.Duration, workFn func(*dgctx.DgContext, *T) error) {
	ctx := buildDgContextFromMsg(msg)
	delayHeader := msg.Header[headerDelay]
	if len(delayHeader) == 0 {
		ase := msg.AckSync()
		if ase != nil {
			dglogger.Errorf(ctx, "msg.AckSync error: %v", ase)
		}
		return
	}

	pubAt, _ := strconv.ParseInt(msg.Header[headerPubAt][0], 10, 64)
	delay, _ := strconv.ParseInt(delayHeader[0], 10, 64)

	if time.Now().UnixNano() <= pubAt+delay {
		dglogger.Debug(ctx, "not due, nak")
		nwde := msg.NakWithDelay(sleepDuration)
		if nwde != nil {
			dglogger.Errorf(ctx, "msg.NakWithDelay error: %v", nwde)
		}
		return
	}

	data := msg.Data
	dglogger.Infof(ctx, "[%s] receive delay json message: %s", subject.Name, data)
	t := new(T)
	err := json.Unmarshal(data, t)
	if err != nil {
		dglogger.Errorf(ctx, "unmarshal json[%s] error: %v", data, err)
		ase := msg.AckSync()
		if ase != nil {
			dglogger.Errorf(ctx, "msg.AckSync error: %v", ase)
		}
		return
	}

	workAndAck(ctx, msg, t, workFn)
}

func Unsubscribe(ctx *dgctx.DgContext, subject *NatsSubject, tag string) error {
	js, err := GetJs()
	if err != nil {
		dglogger.Errorf(ctx, "GetJs error: %v", err)
		return err
	}
	err = js.DeleteConsumer(subject.Category, subject.GetDurable(tag))
	if err != nil {
		dglogger.Errorf(ctx, "js.DeleteConsumer error: %v", err)
	}
	return err
}

func buildDgContextFromMsg(msg *nats.Msg) *dgctx.DgContext {
	traceIdHeader := msg.Header[constants.TraceId]
	var traceId string
	if len(traceIdHeader) > 0 {
		traceId = msg.Header[constants.TraceId][0]
	}
	if traceId == "" {
		traceId = nuid.Next()
	}
	return &dgctx.DgContext{TraceId: traceId}
}

func buildSubOpts(subject *NatsSubject, tag string) []nats.SubOpt {
	var subOpts []nats.SubOpt
	subOpts = append(subOpts, DefaultSubOpts...)
	subOpts = append(subOpts, nats.Durable(subject.GetDurable(tag)), nats.BindStream(subject.Category))
	return subOpts
}

func workAndAck[T any](ctx *dgctx.DgContext, msg *nats.Msg, t *T, workFn func(*dgctx.DgContext, *T) error) {
	err := workFn(ctx, t)
	ackOrNakByError(msg, err)
}

func ackOrNakByError(msg *nats.Msg, err error) {
	if err != nil {
		_ = msg.NakWithDelay(SubWorkErrorRetryWait)
	} else {
		_ = msg.AckSync()
	}
}
