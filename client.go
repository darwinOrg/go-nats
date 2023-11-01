package dgnats

import (
	"github.com/nats-io/nats.go"
	"time"
)

type NatsConfig struct {
	PoolSize       int      `json:"pool-size" mapstructure:"pool-size"`
	Servers        []string `json:"servers" mapstructure:"servers"`
	ConnectionName string   `json:"connection-name" mapstructure:"connection-name"`
	Username       string   `json:"username" mapstructure:"username"`
	Password       string   `json:"password" mapstructure:"password"`
	CheckPoint     struct {
		Enable bool   `json:"enable" mapstructure:"enable"`
		AppId  string `json:"appId" mapstructure:"appId"`
	} `json:"checkpoint" mapstructure:"checkpoint"`
}

var natsClient *nats.Conn

func InitClient(natsConf *NatsConfig) *nats.Conn {
	opts := nats.GetDefaultOptions()
	opts.MaxReconnect = natsConf.PoolSize
	opts.ReconnectWait = 10 * time.Second
	opts.Servers = natsConf.Servers
	opts.Name = natsConf.ConnectionName
	opts.User = natsConf.Username
	opts.Password = natsConf.Password

	nc, err := opts.Connect()
	if err != nil {
		panic(err)
	}

	return nc
}
