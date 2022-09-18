package consumer

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type IConsumer interface {
	Read(context.Context, chan kafka.Message) error
	Commit(context.Context, chan kafka.Message) error
	ReadFromTopic(ctx context.Context, m chan string) error
}

type consumer struct {
	reader *kafka.Reader
}

type Logs struct {
	ServiceId  string `json:"service_id"`
	Decription string `json:"description"`
}

//init.... initializes local variables
func init() {
	// bServer := fmt.Sprint(viper.Get("KAFKASERVER"))
	// brokers = []string{bServer}
	// topic = fmt.Sprint(viper.Get("TOPICREAD"))
	// groupId = fmt.Sprint(viper.Get("KAFKGROUPIDASERVER"))
}

func NewConsumer(brokers []string, topic, groupid string) IConsumer {
	config := kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupid,
	}
	fmt.Println(brokers, topic)
	return &consumer{
		reader: kafka.NewReader(config),
	}
}

func (c *consumer) ReadFromTopic(ctx context.Context, m chan string) (err error) {
	defer func() {
		c.reader.Close()
		if r := recover(); r != nil {
			err = fmt.Errorf("%s", r)
		}
	}()
	// log := Logs{}
	var msg kafka.Message
	for {
		msg, err = c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case m <- string(msg.Value):
			log.Printf("Message from topic %s\n", string(msg.Value))
			err = c.reader.CommitMessages(ctx, msg)
			if err != nil {
				log.Println("Error commiting message: ", err)
			}
		}
		//write to file and upload to awsS3

		// json.Unmarshal(msg.Value, &log)
		// fmt.Printf("%+v", log)
	}
}

func (c *consumer) Read(ctx context.Context, m chan kafka.Message) error {
	defer c.reader.Close()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case m <- msg:
			log.Printf("Read message %v from topic %v \n", msg, c.reader.Config().Topic)
		}
	}
}

func (c *consumer) Commit(ctx context.Context, cm chan kafka.Message) error {
	defer c.reader.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-cm:
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				return err
			}
			// log.Printf("Read message %v from topic %v \n", msg, c.reader.Config().Topic)
		}
	}
}
