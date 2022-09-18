package util

import (
	"crypto/rand"
	"fmt"
)

//GetRandomString generates a random string
func GetRandomString(len int) (s string, err error) {
	if len > 8 || len <= 0 {
		err = fmt.Errorf("invalid length %d", len)
		return
	}
	b := make([]byte, 4) //equals 8 characters

	rand.Read(b)
	// s = hex.EncodeToString(b)
	s = fmt.Sprintf("%X", b)
	//fmt.Sprintf("%X-%X", b[0:2], b[2:4])
	return
}
