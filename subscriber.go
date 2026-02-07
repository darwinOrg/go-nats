package dgnats

import (
	"strconv"
	"time"

	"github.com/darwinOrg/go-common/constants"
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

var (
	SubWorkErrorRetryWait     = time.Second * 5
	DefaultMaxAckPendingCount = 100

	DefaultSubOpts = []nats.SubOpt{
		nats.AckExplicit(),
		nats.ManualAck(),
		nats.DeliverLast(),
	}
)

func Subscribe(ctx *dgctx.DgContext, subject *NatsSubject, workFn func(*dgctx.DgContext, []byte) error) (*nats.Subscription, error) {
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

	subOpts := buildSubOpts(subject, "")

	var sub *nats.Subscription
	if subject.Group != "" {
		sub, err = js.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
			subscribe(msg, workFn)
		}, subOpts...)
	} else {
		sub, err = js.Subscribe(subject.Name, func(msg *nats.Msg) {
			subscribe(msg, workFn)
		}, subOpts...)
	}
	if err != nil {
		dglogger.Errorf(ctx, "subscribe subject[%s] error: %v", subject.Name, err)
		return nil, err
	}

	return sub, nil
}

func subscribe(msg *nats.Msg, workFn func(*dgctx.DgContext, []byte) error) {
	ctx := buildDgContextFromMsg(msg)
	err := workFn(ctx, msg.Data)
	ackOrNakByError(msg, err)
}

func SubscribeWithTag(ctx *dgctx.DgContext, subject *NatsSubject, tag string, workFn func(*dgctx.DgContext, []byte) error) (*nats.Subscription, error) {
	if tag == "" {
		return Subscribe(ctx, subject, workFn)
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

	subOpts := buildSubOpts(subject, tag)

	var sub *nats.Subscription
	if subject.Group != "" {
		sub, err = js.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
			subscribeWithTag(msg, tag, workFn)
		}, subOpts...)
	} else {
		sub, err = js.Subscribe(subject.Name, func(msg *nats.Msg) {
			subscribeWithTag(msg, tag, workFn)
		}, subOpts...)
	}

	if err != nil {
		dglogger.Errorf(ctx, "subscribe subject[%s] error: %v", subject.Name, err)
		return nil, err
	}

	return sub, nil
}

func subscribeWithTag(msg *nats.Msg, tag string, workFn func(*dgctx.DgContext, []byte) error) {
	if len(msg.Header) == 0 {
		return
	}

	header := msg.Header
	if ts, ok := header[headerTag]; !ok || (len(ts) > 0 && ts[0] == tag) {
		subscribe(msg, workFn)
	}
}

func SubscribeDelay(ctx *dgctx.DgContext, subject *NatsSubject, sleepDuration time.Duration, workFn func(*dgctx.DgContext, []byte) error) (*nats.Subscription, error) {
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

	subOpts := buildSubOpts(subject, "")

	var sub *nats.Subscription
	if subject.Group != "" {
		sub, err = js.QueueSubscribe(subject.Name, subject.Group, func(msg *nats.Msg) {
			subscribeDelay(msg, subject, sleepDuration, workFn)
		}, subOpts...)
	} else {
		sub, err = js.Subscribe(subject.Name, func(msg *nats.Msg) {
			subscribeDelay(msg, subject, sleepDuration, workFn)
		}, subOpts...)
	}

	if err != nil {
		dglogger.Errorf(ctx, "subscribe subject[%s] error: %v", subject.Name, err)
		return nil, err
	}

	return sub, nil
}

func subscribeDelay(msg *nats.Msg, subject *NatsSubject, sleepDuration time.Duration, workFn func(*dgctx.DgContext, []byte) error) {
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

	workAndAck(ctx, msg, workFn)
}

func SubscribeJson[T any](ctx *dgctx.DgContext, subject *NatsSubject, workFn func(*dgctx.DgContext, *T) error) (*nats.Subscription, error) {
	return Subscribe(ctx, subject, func(ctx *dgctx.DgContext, data []byte) error {
		t, err := utils.ConvertJsonBytesToBean[T](data)
		if err != nil {
			return err
		}

		return workFn(ctx, t)
	})
}

func SubscribeJsonWithTag[T any](ctx *dgctx.DgContext, subject *NatsSubject, tag string, workFn func(*dgctx.DgContext, *T) error) (*nats.Subscription, error) {
	return SubscribeWithTag(ctx, subject, tag, func(ctx *dgctx.DgContext, data []byte) error {
		t, err := utils.ConvertJsonBytesToBean[T](data)
		if err != nil {
			return err
		}

		return workFn(ctx, t)
	})
}

func SubscribeJsonDelay[T any](ctx *dgctx.DgContext, subject *NatsSubject, sleepDuration time.Duration, workFn func(*dgctx.DgContext, *T) error) (*nats.Subscription, error) {
	return SubscribeDelay(ctx, subject, sleepDuration, func(ctx *dgctx.DgContext, data []byte) error {
		t, err := utils.ConvertJsonBytesToBean[T](data)
		if err != nil {
			return err
		}

		return workFn(ctx, t)
	})
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
	if subject.MaxAckPendingCount > 0 {
		subOpts = append(subOpts, nats.MaxAckPending(subject.MaxAckPendingCount))
	}
	return subOpts
}

func workAndAck(ctx *dgctx.DgContext, msg *nats.Msg, workFn func(*dgctx.DgContext, []byte) error) {
	err := workFn(ctx, msg.Data)
	ackOrNakByError(msg, err)
}

func ackOrNakByError(msg *nats.Msg, err error) {
	if err != nil {
		_ = msg.NakWithDelay(SubWorkErrorRetryWait)
	} else {
		_ = msg.AckSync()
	}
}
