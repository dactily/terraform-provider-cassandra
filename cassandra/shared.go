package cassandra

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/crc32"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func hash(s string) string {
	sha := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sha[:])
}

func stringHashcode(s string) int {
	v := int(crc32.ChecksumIEEE([]byte(s)))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	return 0
}

func setToArray(s interface{}) []string {
	set, ok := s.(*schema.Set)
	if !ok {
		return []string{}
	}

	ret := []string{}
	for _, elem := range set.List() {
		ret = append(ret, elem.(string))
	}
	return ret
}
