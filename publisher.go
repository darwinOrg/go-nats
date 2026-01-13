package dgnats

import (
	"strconv"
	"time"

	"github.com/darwinOrg/go-common/constants"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/nats-io/nats.go"
)

func Publish(ctx *dgctx.DgContext, subject *NatsSubject, obj any) error {
	bytes, err := ToBytes(ctx, obj)
	if err != nil {
		return err
	}
	dglogger.Infof(ctx, "publish subject[%s] json message: %s", subject.Name, string(bytes))

	return PublishRaw(ctx, subject, bytes)
}

func PublishDelay(ctx *dgctx.DgContext, subject *NatsSubject, obj any, duration time.Duration) error {
	bytes, err := ToBytes(ctx, obj)
	if err != nil {
		return err
	}
	dglogger.Infof(ctx, "publish subject[%s] json delay message: %s", subject.Name, string(bytes))

	header := map[string][]string{
		constants.TraceId: {ctx.TraceId},
		headerPubAt:       {strconv.FormatInt(time.Now().UnixNano(), 10)},
		headerDelay:       {strconv.FormatInt(int64(duration), 10)},
	}

	msg := &nats.Msg{
		Subject: subject.Name,
		Reply:   subject.Name,
		Header:  header,
		Data:    bytes,
	}

	return publishMsg(ctx, subject, msg)
}

func PublishRaw(ctx *dgctx.DgContext, subject *NatsSubject, data []byte) error {
	return publishRawWithHeader(ctx, subject, map[string][]string{}, data)
}

func PublishRawWithTag(ctx *dgctx.DgContext, subject *NatsSubject, tag string, data []byte) error {
	if tag == "" {
		return PublishRaw(ctx, subject, data)
	}

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

	js, err := GetJs()
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
