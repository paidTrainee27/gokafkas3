package aws

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type IS3Manager interface {
	Upload(context.Context, chan string) error
}

type s3 struct {
	bucket   string
	region   string
	uploader *s3manager.Uploader
}

func NewS3(bucket, region string) IS3Manager {
	s, _ := session.NewSession(&aws.Config{
		Region: aws.String(region),
		// Credentials: credentials.NewStaticCredentials(accessKey, secretKey, "TOKEN"),
	})
	u := s3manager.NewUploader(s)
	return &s3{
		bucket:   bucket,
		region:   region,
		uploader: u,
	}
}

func (s *s3) Upload(ctx context.Context, c chan string)(err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s", r)
		}
	}()

	for {
		select {
		case filename, open := <-c:
			if open {
				s.uploadFile(filename)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// return
}

func (s *s3) uploadFile(filename string) {
	file, err := os.Open(filename)

	defer func() {
		file.Close()
	}() 

	if err != nil {
		fmt.Printf("Unable to open file %q, %v\n", filename, err)
		return
	}

	_, err = s.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filename),
		Body:   file,
	})

	if err != nil {
		// Print the error and exit.
		fmt.Printf("Unable to upload %q to %q, %v\n", filename, s.bucket, err)
		// file.Close()
		return
	}

	// file.Close()
	// os.Remove(filename) //manage

	fmt.Printf("Successfully uploaded %q to %q\n", filename, s.bucket)
}
