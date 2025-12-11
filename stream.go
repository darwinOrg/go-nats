package dgnats

import (
	"errors"
	"strings"
	"sync"
	"time"

	dgcoll "github.com/darwinOrg/go-common/collection"
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/nats-io/nats.go"
)

var streamCache = sync.Map{}

const defaultMaxAge = 31 * 24 * time.Hour

func InitStream(ctx *dgctx.DgContext, subject *NatsSubject) error {
	subjectId := subject.GetId()
	if _, ok := streamCache.Load(subjectId); ok {
		return nil
	}

	js, err := GetJs()
	if err != nil {
		return err
	}
	streamInfo, _ := js.StreamInfo(subject.Category)
	defer func() {
		streamCache.Store(subjectId, streamInfo)
	}()

	if streamInfo != nil {
		if dgcoll.AnyMatch(streamInfo.Config.Subjects, func(s string) bool {
			return s == subject.Name
		}) {
			return nil
		}
		dglogger.Debugf(ctx, "update stream[%s] for %s", subject.Category, subject.Name)

		streamInfo.Config.Subjects = append(streamInfo.Config.Subjects, subject.Name)
		_, err = js.UpdateStream(&streamInfo.Config)
		if err != nil {
			return err
		}
	} else {
		dglogger.Debugf(ctx, "add stream %s", subject.Category)
		si, err := js.AddStream(buildStreamConfig(subject))
		if err != nil {
			if errors.Is(err, nats.ErrStreamNameAlreadyInUse) || strings.Contains(err.Error(), "existing") {
				return nil
			} else {
				dglogger.Errorf(ctx, "add stream[%s] error: %v", subject.Category, err)
				return err
			}
		}
		streamInfo = si
	}

	return nil
}

func DeleteStream(ctx *dgctx.DgContext, subject *NatsSubject) error {
	js, err := GetJs()
	if err != nil {
		return err
	}
	err = js.DeleteStream(subject.Category)
	if err != nil {
		dglogger.Errorf(ctx, "delete stream[%s] error: %v", subject.Category, err)
		return err
	}

	subjectId := subject.GetId()
	if _, ok := streamCache.Load(subjectId); !ok {
		return nil
	}
	streamCache.Delete(subjectId)

	return nil
}

func buildStreamConfig(subject *NatsSubject) *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:     subject.Category,
		Subjects: []string{subject.Name},
		Storage:  nats.FileStorage,
		MaxAge:   utils.IfReturn(subject.MaxAge > 0, subject.MaxAge, defaultMaxAge),
	}
}
