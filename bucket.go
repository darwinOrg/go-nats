package dgnats

import "github.com/nats-io/nats.go"

type natsBucket struct {
	kv nats.KeyValue
}

func NewNatsBucket(bucket string) (*natsBucket, error) {
	stream, err := natsClient.JetStream()
	if err != nil {
		return nil, err
	}

	keyValue, err := stream.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket})
	if err != nil {
		return nil, err
	}

	return &natsBucket{keyValue}, nil
}

func (n *natsBucket) PutString(key string, value string) error {
	_, err := n.kv.PutString(key, value)
	return err
}

func (n *natsBucket) GetString(key string) (string, error) {
	entry, err := n.kv.Get(key)
	if err != nil {
		return "", err
	}
	return string(entry.Value()), nil
}
