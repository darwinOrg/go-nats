package dgnats

import (
	"github.com/nats-io/nats.go"
)

type NatsConfig struct {
	PoolSize       int      `json:"pool-size" mapstructure:"pool-size"`
	Servers        []string `json:"servers" mapstructure:"servers"`
	ConnectionName string   `json:"connection-name" mapstructure:"connection-name"`
	Username       string   `json:"username" mapstructure:"username"`
	Password       string   `json:"password" mapstructure:"password"`
}

var natsConn *nats.Conn
var natsJs nats.JetStreamContext

func Connect(natsConf *NatsConfig) error {
	opts := nats.GetDefaultOptions()
	opts.Servers = natsConf.Servers
	opts.Name = natsConf.ConnectionName
	opts.User = natsConf.Username
	opts.Password = natsConf.Password
	opts.NoEcho = true

	nc, err := opts.Connect()
	if err != nil {
		return err
	}
	natsConn = nc

	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	natsJs = js

	return nil
}

func Close() {
	if !natsConn.IsClosed() {
		natsConn.Close()
	}
}
