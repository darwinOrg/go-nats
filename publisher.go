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
	}

	msg := &nats.Msg{
		Subject: subject.Name,
		Reply:   subject.Name,
		Header:  header,
		Data:    jsonBytes,
	}

	return publishMsg(ctx, subject, msg)
}

func PublishRaw(ctx *dgctx.DgContext, subject *NatsSubject, data []byte) error {
	return publishRawWithHeader(ctx, subject, map[string][]string{}, data)
}

func PublishRawWithTag(ctx *dgctx.DgContext, subject *NatsSubject, tag string, data []byte) error {
	return publishRawWithHeader(ctx, subject, map[string][]string{headerTag: {tag}}, data)
}

func publishRawWithHeader(ctx *dgctx.DgContext, subject *NatsSubject, header map[string][]string, data []byte) error {
	header[constants.TraceId] = []string{ctx.TraceId}

	msg := &nats.Msg{
		Subject: subject.Name,
		Header:  header,
		Data:    data,
	}

	return publishMsg(ctx, subject, msg)
}

func publishMsg(ctx *dgctx.DgContext, subject *NatsSubject, msg *nats.Msg) error {
	err := InitStream(ctx, subject)
	if err != nil {
		return err
	}

	js, err := getJs()
	if err != nil {
		return err
	}
	_, err = js.PublishMsg(msg, buildPubOpts(subject)...)

	return err
}

func buildPubOpts(subject *NatsSubject) []nats.PubOpt {
	return []nats.PubOpt{
		nats.MsgId(nats.NewInbox()),
		nats.ExpectStream(subject.Category),
	}
}
