package bloom

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

const redisMaxLength = 8 * 512 * 1024 * 1024

type Connection interface {
	Do(cmd string, args ...interface{}) (reply interface{}, err error)
	Send(cmd string, args ...interface{}) error
	Flush() error
}

type RedisBitSet struct {
	keyPrefix   string
	redisClient *redis.ClusterClient
	m           uint
}

func NewRedisBitSet(keyPrefix string, m uint, redisClient *redis.ClusterClient) *RedisBitSet {
	return &RedisBitSet{keyPrefix, redisClient, m}
}

func (r *RedisBitSet) Set(offsets []int64) error {
	for _, offset := range offsets {
		key, thisOffset := r.getKeyOffset(offset)
		res := r.redisClient.SetBit(key, thisOffset, 1)
		err := res.Err()
		if err != nil {
			return err
		}
	}

	res := r.redisClient.FlushAll()
	return res.Err()
}

func (r *RedisBitSet) Test(offsets []int64) (bool, error) {
	for _, offset := range offsets {
		key, thisOffset := r.getKeyOffset(offset)
		res := r.redisClient.GetBit(key, thisOffset)

		err := res.Err()
		if err != nil {
			return false, err
		}
		bitValue := res.Val()
		if bitValue == 0 {
			return false, nil
		}
	}

	return true, nil
}

func (r *RedisBitSet) Expire(seconds int) error {
	n := uint(0)
	for n <= uint(r.m/redisMaxLength) {
		key := fmt.Sprintf("%s:%d", r.keyPrefix, n)
		n = n + 1
		resp := r.redisClient.Expire(key, time.Second*time.Duration(seconds))
		err := resp.Err()
		if err != nil {
			return err
		}
	}
	res := r.redisClient.FlushAll()
	return res.Err()
}

func (r *RedisBitSet) Delete() error {
	n := uint(0)
	keys := make([]string, 0)
	for n <= uint(r.m/redisMaxLength) {
		key := fmt.Sprintf("%s:%d", r.keyPrefix, n)
		keys = append(keys, key)
		n = n + 1
	}
	err := r.redisClient.Del(strings.Join(keys, " "))

	return err.Err()
}

func (r *RedisBitSet) getKeyOffset(offset int64) (string, int64) {
	n := offset / redisMaxLength
	thisOffset := int64(offset - n*redisMaxLength)
	key := fmt.Sprintf("%s:%d", r.keyPrefix, n)
	return key, thisOffset
}
