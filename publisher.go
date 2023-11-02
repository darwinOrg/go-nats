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

func PublishRaw(ctx *dgctx.DgContext, subject *NatsSubject, data []byte) error {
	header := map[string][]string{constants.TraceId: {ctx.TraceId}}

	msg := &nats.Msg{
		Subject: subject.Name,
		Header:  header,
		Data:    data,
	}

	_, err := natsJs.PublishMsg(msg)

	return err
}

func PublishJson[T any](ctx *dgctx.DgContext, subject *NatsSubject, obj *T) error {
	jsonBytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	dglogger.Infof(ctx, "publish subject[%s] json message: %s", subject.Name, string(jsonBytes))

	return PublishRaw(ctx, subject, jsonBytes)
}

func PublishJsonDelay[T any](ctx *dgctx.DgContext, subject *NatsSubject, obj *T, duration time.Duration) error {
	jsonBytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	dglogger.Infof(ctx, "publish subject[%s] json delay message: %s", subject.Name, string(jsonBytes))

	header := map[string][]string{
		constants.TraceId: {ctx.TraceId},
		headerPubAt:       {strconv.FormatInt(time.Now().UnixMilli(), 10)},
		headerDelay:       {strconv.FormatInt(duration.Milliseconds(), 10)},
		headerData:        {string(jsonBytes)},
	}

	msg := &nats.Msg{
		Subject: subject.Name,
		Reply:   subject.Name,
		Header:  header,
	}

	_, err = natsJs.PublishMsg(msg)
	return err
}
