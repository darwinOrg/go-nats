package dgnats

import (
	"encoding/json"
	"github.com/darwinOrg/go-common/constants"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/nats-io/nats.go"
	"strconv"
	"time"
)

const (
	headerDelay = "delay"
	headerPubAt = "pub-at"
)

func PublishJson[T any](ctx *dgctx.DgContext, subject *Subject, obj *T) error {
	jsonBytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	dglogger.Infof(ctx, "publish subject[%s] json message: %s", subject.Name, string(jsonBytes))

	return PublishRaw(ctx, subject, jsonBytes)
}

func PublishJsonDelay[T any](ctx *dgctx.DgContext, subject *Subject, obj *T, duration time.Duration) error {
	jsonBytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	dglogger.Infof(ctx, "publish subject[%s] json delay message: %s", subject.Name, string(jsonBytes))

	header := buildDelayHeader(ctx, duration)
	return publishMsg(subject, jsonBytes, header)
}

func PublishRaw(ctx *dgctx.DgContext, subject *Subject, data []byte) error {
	header := map[string][]string{constants.TraceId: {ctx.TraceId}}

	return publishMsg(subject, data, header)
}

func PublishRawDelay(ctx *dgctx.DgContext, subject *Subject, data []byte, duration time.Duration) error {
	header := buildDelayHeader(ctx, duration)
	return publishMsg(subject, data, header)
}

func buildDelayHeader(ctx *dgctx.DgContext, duration time.Duration) map[string][]string {
	return map[string][]string{
		constants.TraceId: {ctx.TraceId},
		headerPubAt:       {strconv.FormatInt(time.Now().UnixMilli(), 10)},
		headerDelay:       {strconv.FormatInt(duration.Milliseconds(), 10)},
	}
}

func publishMsg(subject *Subject, data []byte, header nats.Header) error {
	msg := &nats.Msg{
		Subject: subject.Name,
		Header:  header,
		Data:    data,
	}
	return natsConn.PublishMsg(msg)
}
