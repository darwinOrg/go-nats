package dgnats

import (
	"regexp"
	"time"

	"github.com/darwinOrg/go-common/utils"
)

const (
	illegalRegexStr = "[.|*>]"
	dash            = "-"
)

var illegalRegex = regexp.MustCompile(illegalRegexStr)

type NatsSubject struct {
	Category string        `json:"category" binding:"required" remark:"流/topic"`
	Name     string        `json:"name" binding:"required" remark:"tag"`
	Group    string        `json:"group" remark:"队列"`
	MaxAge   time.Duration `json:"maxAge" remark:"最大时长"`
}

func (s *NatsSubject) GetId() string {
	id := s.Category + "-" + s.Name
	if s.Group != "" {
		id = id + "-" + s.Group
	}

	return ReplaceIllegalCharacter(id)
}

func (s *NatsSubject) GetDurable(tag string) string {
	if s.Group != "" {
		return s.GetId() + utils.IfReturn(tag != "", "-"+ReplaceIllegalCharacter(tag), "")
	}

	return ""
}

func ReplaceIllegalCharacter(str string) string {
	return illegalRegex.ReplaceAllString(str, dash)
}
