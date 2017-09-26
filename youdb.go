// Package youdb is a Bolt wrapper that allows easy store hash, zset data.
package youdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
)

const (
	replyOK                 = "ok"
	replyNotFound           = "not_found"
	replyError              = "error"
	replyClientError        = "client_error"
	scoreMin         uint64 = 0
	scoreMax         uint64 = 18446744073709551615
)

var (
	hashPrefix     = []byte{30}
	zetKeyPrefix   = []byte{31}
	zetScorePrefix = []byte{29}
)

type (
	// DB embeds a bolt.DB
	DB struct {
		*bolt.DB
	}

	// Entry represents a holder for a key value pair of a hashmap
	Entry struct {
		Key   string
		Value []byte
	}

	// Reply a holder for a Entry list of a hashmap
	Reply struct {
		State string
		Data  []*Entry
	}

	// ZetEntry represents a holder for a key value pair of a zet
	ZetEntry struct {
		Key   string
		Value uint64
	}

	// ZetReply a holder for a ZetEntry list of a zet
	ZetReply struct {
		State string
		Data  []*ZetEntry
	}
)

// Open creates/opens a bolt.DB at specified path, and returns a DB enclosing the same
func Open(path string) (*DB, error) {
	database, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	db := DB{database}

	return &db, nil
}

// Close closes the embedded bolt.DB
func (db *DB) Close() error {
	return db.DB.Close()
}

// Hset set the byte value in argument as value of the key of a hashmap
func (db *DB) Hset(name, key string, val []byte) error {
	bucketName := Bconcat([][]byte{hashPrefix, []byte(name)})
	return db.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), val)
	})
}

// Hmset set multiple key-value pairs(map) of a hashmap in one method call
func (db *DB) Hmset(name string, kv map[string][]byte) error {
	bucketName := Bconcat([][]byte{hashPrefix, []byte(name)})
	return db.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		for k, v := range kv {
			err := b.Put([]byte(k), v)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Hincr increment the number stored at key in a hashmap by step
func (db *DB) Hincr(name, key string, step uint64) (uint64, error) {
	var i uint64
	bucketName := Bconcat([][]byte{hashPrefix, []byte(name)})
	err := db.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		var oldNum uint64
		v := b.Get([]byte(key))
		if len(v) > 0 {
			oldNum = Btoi(v)
		}
		if step > 0 {
			if (scoreMax - step) < oldNum {
				return errors.New("overflow number")
			}
		} else {
			if (oldNum + step) < scoreMin {
				return errors.New("overflow number")
			}
		}

		oldNum += step
		err = b.Put([]byte(key), Itob(oldNum))
		if err != nil {
			return err
		}
		i = oldNum
		return nil
	})
	return i, err
}

// Hdel delete specified key of a hashmap
func (db *DB) Hdel(name, key string) error {
	bucketName := Bconcat([][]byte{hashPrefix, []byte(name)})
	return db.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b != nil {
			return b.Delete([]byte(key))
		}
		return nil
	})
}

// HdelBucket delete all keys in a hashmap
func (db *DB) HdelBucket(name string) error {
	bucketName := Bconcat([][]byte{hashPrefix, []byte(name)})
	return db.DB.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(bucketName)
	})
}

// Hget get the value related to the specified key of a hashmap
func (db *DB) Hget(name, key string) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []*Entry{},
	}
	bucketName := Bconcat([][]byte{hashPrefix, []byte(name)})
	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return errors.New(replyNotFound)
		}
		v := b.Get([]byte(key))
		if v != nil {
			r.State = replyOK
			r.Data = append(r.Data, &Entry{key, v})
		} else {
			r.State = replyNotFound
		}
		return nil
	})
	if err != nil {
		r.State = replyClientError
	}
	return r
}

// Hmget get the values related to the specified multiple keys of a hashmap
func (db *DB) Hmget(name string, keys []string) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []*Entry{},
	}
	bucketName := Bconcat([][]byte{hashPrefix, []byte(name)})
	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return errors.New(replyNotFound)
		}
		for _, key := range keys {
			k := []byte(key)
			v := b.Get(k)
			if v != nil {
				r.Data = append(r.Data, &Entry{key, v})
			}
		}
		return nil
	})
	if err == nil {
		r.State = replyOK
	}
	return r
}

// Hscan list key-value pairs of a hashmap with keys in range (key_start, key_end]
func (db *DB) Hscan(name, keyStart string, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []*Entry{},
	}
	bucketName := Bconcat([][]byte{hashPrefix, []byte(name)})
	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return errors.New(replyNotFound)
		}
		c := b.Cursor()
		n := 0
		startKey := []byte{}
		if len(keyStart) > 0 {
			startKey = []byte(keyStart)
		}
		for k, v := c.Seek(startKey); k != nil; k, v = c.Next() {
			if bytes.Compare(k, startKey) == 1 {
				n++
				r.Data = append(r.Data, &Entry{string(k), v})
				if n == limit {
					break
				}
			}
		}
		return nil
	})
	if err == nil {
		r.State = replyOK
	}
	return r
}

// Hrscan list key-value pairs of a hashmap with keys in range (key_start, key_end], in reverse order
func (db *DB) Hrscan(name, keyStart string, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []*Entry{},
	}
	bucketName := Bconcat([][]byte{hashPrefix, []byte(name)})
	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return errors.New(replyNotFound)
		}
		c := b.Cursor()
		n := 0
		startKey, k0, v0 := []byte{255}, []byte{}, []byte{}
		if len(keyStart) > 0 {
			startKey = []byte(keyStart)
			k0, v0 = c.Seek(startKey)
		} else {
			k0, v0 = c.Last()
		}
		for k, v := k0, v0; k != nil; k, v = c.Prev() {
			if bytes.Compare(k, startKey) == -1 {
				n++
				r.Data = append(r.Data, &Entry{string(k), v})
				if n >= limit {
					break
				}
			}
		}
		return nil
	})
	if err == nil {
		r.State = replyOK
	}
	return r
}

// Zset set the score of the key of a zset
func (db *DB) Zset(name, key string, val uint64) error {
	score := Itob(val)
	keyB := []byte(key)
	keyBucket := Bconcat([][]byte{zetKeyPrefix, []byte(name)})
	scoreBucket := Bconcat([][]byte{zetScorePrefix, []byte(name)})
	newKey := Bconcat([][]byte{score, keyB})
	return db.DB.Update(func(tx *bolt.Tx) error {
		b1, err1 := tx.CreateBucketIfNotExists(keyBucket)
		if err1 != nil {
			return err1
		}
		b2, err2 := tx.CreateBucketIfNotExists(scoreBucket)
		if err2 != nil {
			return err2
		}

		oldScore := b2.Get(keyB)
		if !bytes.Equal(oldScore, score) {
			err1 = b1.Put(newKey, []byte{})
			if err1 != nil {
				return err1
			}

			err2 = b2.Put(keyB, score)
			if err2 != nil {
				return err2
			}

			if oldScore != nil {
				oldKey := Bconcat([][]byte{oldScore, keyB})
				err1 = b1.Delete(oldKey)
				if err1 != nil {
					return err1
				}
			}
		}
		return nil
	})
}

// Zmset et multiple key-score pairs(map) of a zset in one method call
func (db *DB) Zmset(name string, kv map[string]uint64) error {
	newKv := map[string][]byte{}
	for k, v := range kv {
		newKv[k] = Itob(v)
	}

	keyBucket := Bconcat([][]byte{zetKeyPrefix, []byte(name)})
	scoreBucket := Bconcat([][]byte{zetScorePrefix, []byte(name)})

	return db.DB.Update(func(tx *bolt.Tx) error {
		b1, err1 := tx.CreateBucketIfNotExists(keyBucket)
		if err1 != nil {
			return err1
		}
		b2, err2 := tx.CreateBucketIfNotExists(scoreBucket)
		if err2 != nil {
			return err2
		}

		for k, score := range newKv {
			keyB := []byte(k)
			newKey := Bconcat([][]byte{score, keyB})

			oldScore := b2.Get(keyB)
			if !bytes.Equal(oldScore, score) {
				err1 = b1.Put(newKey, []byte(""))
				if err1 != nil {
					return err1
				}

				err2 = b2.Put(keyB, score)
				if err2 != nil {
					return err2
				}

				if oldScore != nil {
					oldKey := Bconcat([][]byte{oldScore, keyB})
					err1 = b1.Delete(oldKey)
					if err1 != nil {
						return err1
					}
				}
			}
		}
		return nil
	})
}

// Zincr increment the number stored at key in a zset by step
func (db *DB) Zincr(name, key string, step uint64) (uint64, error) {
	var score uint64

	keyB := []byte(key)
	keyBucket := Bconcat([][]byte{zetKeyPrefix, []byte(name)})
	scoreBucket := Bconcat([][]byte{zetScorePrefix, []byte(name)})

	err := db.DB.Update(func(tx *bolt.Tx) error {
		b1, err1 := tx.CreateBucketIfNotExists(keyBucket)
		if err1 != nil {
			return err1
		}
		b2, err2 := tx.CreateBucketIfNotExists(scoreBucket)
		if err2 != nil {
			return err2
		}

		vOld := b2.Get(keyB)
		if vOld != nil {
			score = Btoi(vOld)
		}
		if step > 0 {
			if (scoreMax - step) < score {
				return errors.New("overflow number")
			}
		} else {
			if (score + step) < scoreMin {
				return errors.New("overflow number")
			}
		}

		score += step
		newScoreB := Itob(score)
		newKey := Bconcat([][]byte{newScoreB, keyB})

		err1 = b1.Put(newKey, []byte{})
		if err1 != nil {
			return err1
		}

		err2 = b2.Put(keyB, newScoreB)
		if err2 != nil {
			return err2
		}

		if vOld != nil {
			oldKey := Bconcat([][]byte{vOld, keyB})
			err1 = b1.Delete(oldKey)
			if err1 != nil {
				return err1
			}
		}
		return nil
	})
	return score, err
}

// Zdel delete specified key of a zset
func (db *DB) Zdel(name, key string) error {
	keyB := []byte(key)
	keyBucket := Bconcat([][]byte{zetKeyPrefix, []byte(name)})
	scoreBucket := Bconcat([][]byte{zetScorePrefix, []byte(name)})
	return db.DB.Update(func(tx *bolt.Tx) error {
		b1 := tx.Bucket(keyBucket)
		if b1 == nil {
			return nil
		}
		b2 := tx.Bucket(scoreBucket)
		if b2 == nil {
			return nil
		}

		oldScore := b2.Get(keyB)
		if oldScore != nil {
			oldKey := Bconcat([][]byte{oldScore, keyB})
			err := b1.Delete(oldKey)
			if err != nil {
				return err
			}
			return b2.Delete(keyB)
		}
		return nil
	})
}

// ZdelBucket delete all keys in a zset
func (db *DB) ZdelBucket(name string) error {
	keyBucket := Bconcat([][]byte{zetKeyPrefix, []byte(name)})
	scoreBucket := Bconcat([][]byte{zetScorePrefix, []byte(name)})
	return db.DB.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(keyBucket)
		if err != nil {
			return err
		}
		return tx.DeleteBucket(scoreBucket)
	})
}

// Zget get the score related to the specified key of a zset
func (db *DB) Zget(name, key string) *ZetReply {
	r := &ZetReply{
		State: replyError,
		Data:  []*ZetEntry{},
	}
	scoreBucket := Bconcat([][]byte{zetScorePrefix, []byte(name)})
	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(scoreBucket)
		if b == nil {
			return errors.New(replyNotFound)
		}
		v := b.Get([]byte(key))
		if v != nil {
			r.State = replyOK
			r.Data = append(r.Data, &ZetEntry{key, binary.BigEndian.Uint64(v)})
		} else {
			r.State = replyNotFound
		}
		return nil
	})
	if err != nil {
		r.State = replyClientError
	}
	return r
}

// Zmget get the values related to the specified multiple keys of a zset
func (db *DB) Zmget(name string, keys []string) *ZetReply {
	r := &ZetReply{
		State: replyError,
		Data:  []*ZetEntry{},
	}
	scoreBucket := Bconcat([][]byte{zetScorePrefix, []byte(name)})

	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(scoreBucket)
		if b == nil {
			return errors.New(replyNotFound)
		}
		for _, key := range keys {
			k := []byte(key)
			v := b.Get(k)
			if v != nil {
				r.Data = append(r.Data, &ZetEntry{key, binary.BigEndian.Uint64(v)})
			}
		}

		return nil
	})
	if err == nil {
		r.State = replyOK
	}
	return r
}

// Zscan list key-score pairs in a zset, where key-score in range (key_start+score_start, score_end]
func (db *DB) Zscan(name, keyStart, scoreStart string, limit int) *ZetReply {
	r := &ZetReply{
		State: replyError,
		Data:  []*ZetEntry{},
	}
	keyBucket := Bconcat([][]byte{zetKeyPrefix, []byte(name)})

	startScore := scoreMin
	if len(scoreStart) > 0 {
		i, err := strconv.ParseUint(scoreStart, 10, 64)
		if err != nil {
			return r
		}
		startScore = i
	}

	scoreStartB := Itob(startScore)
	startScoreKeyB := Bconcat([][]byte{scoreStartB, []byte(keyStart)})

	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(keyBucket)
		if b == nil {
			return errors.New(replyNotFound)
		}
		c := b.Cursor()
		n := 0
		// c.Seek(scoreStartB) or c.Seek(startScoreKeyB)
		for k, _ := c.Seek(scoreStartB); k != nil; k, _ = c.Next() {
			if bytes.Compare(k, startScoreKeyB) == 1 {
				n++
				r.Data = append(r.Data, &ZetEntry{string(k[8:]), binary.BigEndian.Uint64(k[0:8])})
				if n == limit {
					break
				}
			}
		}
		return nil
	})
	if err == nil {
		r.State = replyOK
	}
	return r
}

// Zrscan list key-score pairs of a zset, in reverse order
func (db *DB) Zrscan(name, keyStart, scoreStart string, limit int) *ZetReply {
	r := &ZetReply{
		State: replyError,
		Data:  []*ZetEntry{},
	}
	keyBucket := Bconcat([][]byte{zetKeyPrefix, []byte(name)})

	startScore := scoreMax
	if len(scoreStart) > 0 {
		i, err := strconv.ParseUint(scoreStart, 10, 64)
		if err != nil {
			return r
		}
		startScore = i
	}

	startKey := []byte{255}
	if len(keyStart) > 0 {
		startKey = []byte(keyStart)
	}

	scoreStartB := Itob(startScore)
	startScoreKeyB := Bconcat([][]byte{scoreStartB, []byte(startKey)})

	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(keyBucket)
		if b == nil {
			return errors.New(replyNotFound)
		}
		c := b.Cursor()

		k0, v0 := []byte{}, []byte{}
		if len(scoreStart) > 0 {
			k0, v0 = c.Seek(scoreStartB) // or c.Seek(startScoreKeyB)
		} else {
			k0, v0 = c.Last()
		}

		n := 0
		for k, _ := k0, v0; k != nil; k, _ = c.Prev() {
			if bytes.Compare(k, startScoreKeyB) == -1 {
				n++
				r.Data = append(r.Data, &ZetEntry{string(k[8:]), binary.BigEndian.Uint64(k[0:8])})
				if n == limit {
					break
				}
			}
		}
		return nil
	})
	if err == nil {
		r.State = replyOK
	}
	return r
}

// String is a convenience wrapper over Get for string value of a hashmap
func (r *Reply) String() string {
	if len(r.Data) > 0 {
		return string(r.Data[0].Value)
	}
	return ""
}

// Int is a convenience wrapper over Get for int value of a hashmap
func (r *Reply) Int() int {
	return int(r.Int64())
}

// Int64 is a convenience wrapper over Get for int64 value of a hashmap
func (r *Reply) Int64() int64 {
	if len(r.Data) < 1 {
		return 0
	}
	return int64(r.Uint64())
}

// Uint is a convenience wrapper over Get for uint value of a hashmap
func (r *Reply) Uint() uint {
	return uint(r.Uint64())
}

// Uint64 is a convenience wrapper over Get for uint64 value of a hashmap
func (r *Reply) Uint64() uint64 {
	if len(r.Data) < 1 {
		return 0
	}
	if len(r.Data[0].Value) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(r.Data[0].Value)
}

// Dict retrieves the key/value pairs from reply of a hashmap
func (r *Reply) Dict() map[string][]byte {
	dict := make(map[string][]byte)
	if len(r.Data) < 1 {
		return dict
	}
	for _, i := range r.Data {
		dict[i.Key] = i.Value
	}
	return dict
}

// JSON parses the JSON-encoded Reply Entry value and stores the result
// in the value pointed to by v
func (r *Reply) JSON(v interface{}) error {
	return json.Unmarshal(r.Data[0].Value, &v)
}

// String return String value of a zet
func (r *ZetReply) String() string {
	if len(r.Data) > 0 {
		return strconv.FormatUint(r.Data[0].Value, 10)
	}
	return ""
}

// Int return int value of a zet
func (r *ZetReply) Int() int {
	return int(r.Int64())
}

// Int64 return int64 value of a zet
func (r *ZetReply) Int64() int64 {
	if len(r.Data) < 1 {
		return 0
	}
	return int64(r.Uint64())
}

// Uint return uint value of a zet
func (r *ZetReply) Uint() uint {
	return uint(r.Uint64())
}

// Uint64 return uint64 value of a zet
func (r *ZetReply) Uint64() uint64 {
	if len(r.Data) < 1 {
		return 0
	}
	return uint64(r.Data[0].Value)
}

// Dict retrieves the key/value pairs from reply of a zet
func (r *ZetReply) Dict() map[string]uint64 {
	dict := make(map[string]uint64)
	if len(r.Data) < 1 {
		return dict
	}
	for _, i := range r.Data {
		dict[i.Key] = i.Value
	}
	return dict
}

// ValStr return string of Entry value
func (r *Entry) ValStr() string {
	return string(r.Value)
}

// ValInt64 return int64 of Entry value
func (r *Entry) ValInt64() int64 {
	if len(r.Value) < 8 {
		return -1
	}
	return int64(binary.BigEndian.Uint64(r.Value))
}

// JSON parses the JSON-encoded Entry value and stores the result
// in the value pointed to by v
func (r *Entry) JSON(v interface{}) error {
	return json.Unmarshal(r.Value, v)
}

// ValStr return string of ZetEntry value
func (r *ZetEntry) ValStr() string {
	return strconv.FormatUint(r.Value, 10)
}

// ValInt return int of ZetEntry value
func (r *ZetEntry) ValInt() int {
	return int(r.Value)
}

// Bconcat concat a list of byte
func Bconcat(slices [][]byte) []byte {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	tmp := make([]byte, totalLen)
	var i int
	for _, s := range slices {
		i += copy(tmp[i:], s)
	}
	return tmp
}

// Itob returns an 8-byte big endian representation of v
func Itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// Btoi return an int64 of v
func Btoi(v []byte) uint64 {
	return binary.BigEndian.Uint64(v)
}
