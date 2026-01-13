package dgnats

import (
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
)

func ToBytes(ctx *dgctx.DgContext, message any) ([]byte, error) {
	switch message.(type) {
	case string:
		return []byte(message.(string)), nil
	case []byte:
		return message.([]byte), nil
	default:
		jsonMsg, err := utils.ConvertBeanToJsonString(message)
		if err != nil {
			dglogger.Errorf(ctx, "ConvertBeanToJsonString error | err: %v", err)
			return nil, err
		}

		return []byte(jsonMsg), nil
	}
}
