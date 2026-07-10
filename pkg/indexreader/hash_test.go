package indexreader

import (
	"crypto/sha256"
	"encoding/hex"
)

func sha256Sum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
