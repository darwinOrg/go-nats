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
}

var natsConn *nats.Conn

func Connect(natsConf *NatsConfig) {
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
	natsConn = nc
}

func Close() {
	if !natsConn.IsClosed() {
		natsConn.Close()
	}
}
