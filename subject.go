package dgnats

import (
	"github.com/darwinOrg/go-common/utils"
	"regexp"
)

const (
	illegalRegexStr = "[.|*>]"
	dash            = "-"
)

var illegalRegex = regexp.MustCompile(illegalRegexStr)

type NatsSubject struct {
	Category string `json:"category" binding:"required" remark:"流/topic"`
	Name     string `json:"name" binding:"required" remark:"tag"`
	Group    string `json:"group" remark:"队列"`
}

func (s *NatsSubject) GetId() string {
	id := s.Category + "-" + s.Name
	if s.Group != "" {
		id = id + "-" + s.Group
	}

	return illegalRegex.ReplaceAllString(id, dash)
}

func (s *NatsSubject) GetDurable(tag string) string {
	if s.Group != "" {
		return s.GetId() + utils.IfReturn(tag != "", "-"+tag, "")
	}

	return ""
}
