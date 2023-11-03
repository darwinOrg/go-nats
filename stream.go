package dgnats

import (
	dgcoll "github.com/darwinOrg/go-common/collection"
	dgctx "github.com/darwinOrg/go-common/context"
	dgerr "github.com/darwinOrg/go-common/enums/error"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/nats-io/nats.go"
)

var streamCache = map[string]*nats.StreamInfo{}

func initStream(ctx *dgctx.DgContext, subject *NatsSubject) error {
	if natsJs == nil {
		return dgerr.SYSTEM_ERROR
	}

	subjectId := subject.GetId()
	if streamCache[subjectId] != nil {
		dglogger.Debugf(ctx, "hit stream cache: %s", subject.Category)
		return nil
	}

	streamInfo, _ := natsJs.StreamInfo(subject.Category)
	defer func() {
		streamCache[subjectId] = streamInfo
	}()

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

		si, err := natsJs.AddStream(buildStreamConfig(subject))
		if err != nil {
			dglogger.Errorf(ctx, "add stream[%s] error: %v", subject.Category, err)
			return err
		}
		streamInfo = si
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
