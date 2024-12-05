package dgnats

import (
	"errors"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"math/rand"
	"time"
)

var (
	natsConns             []*nats.Conn
	natsJsMap             = map[*nats.Conn]nats.JetStreamContext{}
	connectionFailedError = errors.New("connection failed")
	noConnectionError     = errors.New("no connection")
	connectWaitDuration   = time.Second * 3
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
			return connectionFailedError
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

	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	opts.ConnectedCB = func(conn *nats.Conn) {
		dglogger.Info(ctx, "nats: connection opened")
	}
	opts.ClosedCB = func(conn *nats.Conn) {
		dglogger.Info(ctx, "nats: connection closed")
	}
	opts.ReconnectedCB = func(conn *nats.Conn) {
		dglogger.Info(ctx, "nats: connection reconnected")
	}
	opts.LameDuckModeHandler = func(conn *nats.Conn) {
		dglogger.Warn(ctx, "nats: lame duck mode")
	}
	opts.DiscoveredServersCB = func(conn *nats.Conn) {
		dglogger.Warn(ctx, "nats: discovered servers")
	}
	opts.DisconnectedErrCB = func(conn *nats.Conn, err error) {
		if err != nil {
			dglogger.Infof(ctx, "nats disconnected error: %v", err)
		}
	}
	opts.AsyncErrorCB = func(conn *nats.Conn, subscription *nats.Subscription, err error) {
		if err != nil {
			dglogger.Infof(ctx, "nats async subscription[%s] error: %v", subscription.Subject, err)
		}
	}

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
	if len(natsConns) == 0 {
		return nil, noConnectionError
	}

	nc := natsConns[rand.Int()%len(natsConns)]

	if nc.Status() != nats.CONNECTED {
		time.Sleep(connectWaitDuration)
	}

	if nc.Status() != nats.CONNECTED {
		return nil, connectionFailedError
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
