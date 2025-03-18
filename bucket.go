package dgnats

import "github.com/nats-io/nats.go"

type NatsBucket struct {
	kv nats.KeyValue
}

func NewNatsBucket(bucket string) (*NatsBucket, error) {
	js, err := GetJs()
	if err != nil {
		return nil, err
	}
	keyValue, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket})
	if err != nil {
		return nil, err
	}

	return &NatsBucket{keyValue}, nil
}

func (n *NatsBucket) PutString(key string, value string) error {
	_, err := n.kv.PutString(key, value)
	return err
}

func (n *NatsBucket) GetString(key string) (string, error) {
	entry, err := n.kv.Get(key)
	if err != nil {
		return "", err
	}
	return string(entry.Value()), nil
}
