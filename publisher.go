package dgnats

import (
	"encoding/json"
	dgcoll "github.com/darwinOrg/go-common/collection"
	"github.com/darwinOrg/go-common/constants"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/nats-io/nats.go"
	"strconv"
	"time"
)

var streamCache = map[string]*nats.StreamInfo{}

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
	header := map[string][]string{constants.TraceId: {ctx.TraceId}}

	msg := &nats.Msg{
		Subject: subject.Name,
		Header:  header,
		Data:    data,
	}

	return publishMsg(ctx, subject, msg)
}

func publishMsg(ctx *dgctx.DgContext, subject *NatsSubject, msg *nats.Msg) error {
	err := initStream(ctx, subject)
	if err != nil {
		return err
	}
	_, err = natsJs.PublishMsg(msg, buildPutOpts(subject)...)

	return err
}

func initStream(ctx *dgctx.DgContext, subject *NatsSubject) error {
	subjectId := subject.GetId()
	if streamCache[subjectId] != nil {
		dglogger.Debugf(ctx, "hit stream cache: %s", subject.Category)
		return nil
	}

	streamInfo, _ := natsJs.StreamInfo(subject.Category)

	if streamInfo != nil {
		if dgcoll.AnyMatch(streamInfo.Config.Subjects, func(s string) bool {
			return s == subject.Name
		}) {
			return nil
		}
		dglogger.Debugf(ctx, "update stream[%s] for %s", subject.Category, subject.Name)

		streamInfo.Config.Subjects = append(streamInfo.Config.Subjects, subject.Name)
		_, err := natsJs.UpdateStream(&streamInfo.Config)
		if err != nil {
			return err
		}
	} else {
		dglogger.Debugf(ctx, "add stream %s", subject.Category)

		streamInfo, err := natsJs.AddStream(buildStreamConfig(subject))
		if err != nil {
			return err
		}
		streamCache[subjectId] = streamInfo
	}

	return nil
}

func buildStreamConfig(subject *NatsSubject) *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:     subject.Category,
		Subjects: []string{subject.Name},
		Storage:  nats.FileStorage,
	}
}

func buildPutOpts(subject *NatsSubject) []nats.PubOpt {
	return []nats.PubOpt{
		nats.MsgId(nats.NewInbox()),
		nats.ExpectStream(subject.Category),
	}
}
