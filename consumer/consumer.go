package consumer

type Consumer struct{}

func New() *Consumer {
	return &Consumer{}
}

func (c *Consumer) Read() ([]byte, error) {
	return []byte{}, nil
}
