package dgnats

import (
	"errors"
	"github.com/nats-io/nats.go"
	"math/rand"
	"time"
)

var (
	natsConns           []*nats.Conn
	natsJsMap           = map[*nats.Conn]nats.JetStreamContext{}
	connectionFailed    = errors.New("connection failed")
	connectWaitDuration = time.Second * 3
)

type NatsConfig struct {
	PoolSize       int      `json:"pool-size" mapstructure:"pool-size"`
	Servers        []string `json:"servers" mapstructure:"servers"`
	ConnectionName string   `json:"connection-name" mapstructure:"connection-name"`
	Username       string   `json:"username" mapstructure:"username"`
	Password       string   `json:"password" mapstructure:"password"`
}

func Connect(natsConf *NatsConfig) error {
	for i := 0; i < natsConf.PoolSize; i++ {
		nc, err := connect(natsConf)
		if err != nil {
			return err
		}

		if nc.Status() != nats.CONNECTED {
			time.Sleep(time.Second * 3)
		}

		if nc.Status() != nats.CONNECTED {
			return connectionFailed
		}
	}

	return nil
}

func connect(natsConf *NatsConfig) (*nats.Conn, error) {
	opts := nats.GetDefaultOptions()
	opts.Servers = natsConf.Servers
	opts.Name = natsConf.ConnectionName
	opts.User = natsConf.Username
	opts.Password = natsConf.Password

	nc, err := opts.Connect()
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	natsConns = append(natsConns, nc)
	natsJsMap[nc] = js

	return nc, nil
}

func getConn() (*nats.Conn, error) {
	nc := natsConns[rand.Int()%len(natsConns)]

	if nc.Status() != nats.CONNECTED {
		time.Sleep(connectWaitDuration)
	}

	if nc.Status() != nats.CONNECTED {
		return nil, connectionFailed
	}

	return nc, nil
}

func getJs() (nats.JetStreamContext, error) {
	nc, err := getConn()
	if err != nil {
		return nil, err
	}

	return natsJsMap[nc], nil
}

func Flush(timeout time.Duration) error {
	nc, err := getConn()
	if err != nil {
		return err
	}

	return nc.FlushTimeout(timeout)
}

func Close() {
	for _, nc := range natsConns {
		if nc != nil && !nc.IsClosed() {
			nc.Close()
		}
	}
}
