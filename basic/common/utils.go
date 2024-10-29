package common

import (
	"crypto/md5"
	"encoding/hex"
)

func MD5(text string) string {
	hash := md5.Sum([]byte(text))
	md5String := hex.EncodeToString(hash[:])
	return md5String
}
