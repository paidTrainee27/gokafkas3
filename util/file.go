package util

import (
	"context"
	"fmt"
	"os"
	"syscall"
)

type File struct {
	Name string
	File *os.File
	//TODO: Mutex
}

func NewFile() *File {

	filename, _ := GetRandomString(4)
	fs, err := os.OpenFile(filename, syscall.O_RDWR|syscall.O_CREAT, 0666)

	if err != nil {
		fmt.Printf("error %v\n", err)
	}

	return &File{
		Name: filename,
		File: fs,
	}
}

func (f *File) Write(ctx context.Context, c chan string, m chan string) (err error) {
	for {
		select {
		case str, open := <-c:
			if open {
				_, err = f.File.WriteString(fmt.Sprintf("%s\n", str))
				if err != nil {
					fmt.Println(err)
					return
				}

				select {
				case m <- f.Name:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
