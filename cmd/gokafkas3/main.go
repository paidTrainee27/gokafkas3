package main

import (
	"context"
	"errors"
	"fmt"
	"gokafkas3/internal/consumer"
	"gokafkas3/internal/repository/aws"
	"gokafkas3/util"
	"log"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
)

var (
	brokers []string
	groupId string
	topic   string
	// kproducer   client.Iproducer
	// messageChan chan string
	// commitChan  chan string
	// ctx     context.Context
	// g       *errgroup.Group
	// topicP  string
	// zAddr       net.Addr
)

func init() {
	config, err := util.LoadConfig("../../.env")
	if err != nil {
		log.Fatal("Unable to load config file ", err)
	}
	bServer := fmt.Sprint(config.KafkaServer)
	brokers = []string{bServer}
	topic = fmt.Sprint(config.KafkaTopic)
	groupId = fmt.Sprint(config.KafkaGroupId)
	// zAddr = kafka.TCP(bServer)
	// topicP = fmt.Sprint(viper.Get("TOPIC"))
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	g, _ := errgroup.WithContext(ctx)

	//TODO:
	bucket := "goawstest"
	region := "ap-south-1"

	kconsumer := consumer.NewConsumer(brokers, topic, groupId)
	//TODO: check if old one already exists
	file := util.NewFile()
	awsS3 := aws.NewS3(bucket, region)

	// kproducer = client.NewProducer(zAddr, topicP)
	messageChan := make(chan string)
	commitChan := make(chan string)

	//TODO: Uncomment
	g.Go(func() error {
		// defer handlePanic("consumerFn")
		return kconsumer.ReadFromTopic(ctx, messageChan)
	})
	g.Go(func() error {
		// defer handlePanic("file write")
		return file.Write(ctx, messageChan, commitChan)
	})
	g.Go(func() error {
		// defer handlePanic("consumerFn")
		return awsS3.Upload(ctx, commitChan)
	})
	// g.Go(producerFn)
	// g.Go(commitFn)
	go func() {
		shutDownSrv(ctx)
		cancel()
		close(messageChan)
		close(commitChan)
	}()

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Fatalf("context cancelled by force. whole process is complete")
			return
		}
		log.Fatalf("Error from goroutine: %v", err)
	}
}

// func consumerFn() error {
// 	defer handlePanic("consumerFn")
// 	return kconsumer.ReadFromTopic(ctx)
// }

// func producerFn() error {
// 	defer handlePanic("producerFn")
// 	return kproducer.Write(ctx, messageChan, commitChan)
// }
// func commitFn() error {
// 	defer handlePanic("commitFn")
// 	return kconsumer.Commit(ctx, commitChan)
// }

// func handlePanic(n string) error {
// 	if r := recover(); r != nil {
// 		log.Println("panic handled at: ",n)
// 		return  fmt.Errorf("%s", r)
// 	}
// 	return nil
// }

func shutDownSrv(ctx context.Context) {
	ctxNotify, stop := signal.NotifyContext(ctx,
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	defer func() {
		stop()
	}()

	<-ctxNotify.Done()
	log.Println("shutdown signal received")
}
