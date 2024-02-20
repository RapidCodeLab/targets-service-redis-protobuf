package consumer

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Consumer struct {
	reader *kafka.Reader
}

func New(
	addrs,
	username,
	password,
	topic string,
) (*Consumer, error) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	if username != "" && password != "" {
		mechanism, err := scram.Mechanism(
			scram.SHA256,
			username,
			password)
		if err != nil {
			return nil, err
		}
		dialer.SASLMechanism = mechanism
	}

	addrsSplitted := strings.Split(addrs, ",")

	readerConfig := kafka.ReaderConfig{
		Dialer:   dialer,
		Brokers:  addrsSplitted,
		GroupID:  "notify_orders_pipe_group",
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	}

	r := kafka.NewReader(readerConfig)
	return &Consumer{reader: r}, nil
}

func (c *Consumer) Read(
	ctx context.Context,
) ([]byte, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return msg.Value, err
	}

	err = c.reader.CommitMessages(ctx, msg)
	if err != nil {
		return msg.Value, err
	}

	return msg.Value, nil
}

func (c *Consumer) Stop() error {
	return c.reader.Close()
}
