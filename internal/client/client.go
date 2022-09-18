package client

import (
	"context"
	"net"

	"github.com/segmentio/kafka-go"
)

type Iproducer interface {
	Write(context.Context, chan kafka.Message, chan kafka.Message) error
}

type producer struct {
	writer *kafka.Writer
}

//init.... initializes local variables
func init() {
	// bServer := fmt.Sprint(viper.Get("KAFKASERVER"))
	// zookeeper = kafka.TCP(bServer)
	// topic = fmt.Sprint(viper.Get("TOPICWRITE"))
}

func NewProducer(zAddr net.Addr, topic string) Iproducer {
	return &producer{
		writer: &kafka.Writer{
			Addr:  zAddr,
			Topic: topic,
		},
	}
}

func (p *producer) Write(ctx context.Context, c chan kafka.Message, comit chan kafka.Message) error {
	defer	p.writer.Close()
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-c:
			p.writer.WriteMessages(ctx, kafka.Message{Value: msg.Value})

			select {
			case <-ctx.Done():
			case comit <- msg:
			}
		}
	}
	// return nil
}
