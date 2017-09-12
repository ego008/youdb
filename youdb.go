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
	replyOK          string = "ok"
	replyNotFound    string = "not_found"
	replyError       string = "error"
	replyClientError string = "client_error"
	scoreMin         int64  = 0
	scoreMax         int64  = 9223372036854775807 // uint64
)

var (
	hashPrefix     []byte = []byte{30} // hash map
	zetKeyPrefix   []byte = []byte{31} // [score+key]->nil
	zetScorePrefix []byte = []byte{29} // [key]->score
)

type (
	DB struct {
		*bolt.DB
	}

	Entry struct {
		Key string
		Value []byte
	}

	Reply struct {
		State string
		Data  []*Entry
	}

	ZetEntry struct {
		Key   string
		Value int64
	}

	ZetReply struct {
		State string
		Data  []*ZetEntry
	}
)

func Open(path string) (*DB, error) {
	database, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	db := DB{database}

	return &db, nil
}

func (db *DB) Close() error {
	return db.DB.Close()
}

// --- hash ------

func (db *DB) Hset(name, key string, val []byte) error {
	bucketName := bconcat([][]byte{hashPrefix, []byte(name)})
	return db.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), val)
	})
}

func (db *DB) Hmset(name string, kv map[string][]byte) error {
	bucketName := bconcat([][]byte{hashPrefix, []byte(name)})
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

func (db *DB) Hincr(name, key string, step int64) (int64, error) {
	var i int64
	bucketName := bconcat([][]byte{hashPrefix, []byte(name)})
	err := db.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		var oldNum int64
		v := b.Get([]byte(key))
		if len(v) > 0 {
			oldNum = btoi(v)
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
		err = b.Put([]byte(key), itob(oldNum))
		if err != nil {
			return err
		}
		i = oldNum
		return nil
	})
	return i, err
}

func (db *DB) Hdel(name, key string) error {
	bucketName := bconcat([][]byte{hashPrefix, []byte(name)})
	return db.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b != nil {
			return b.Delete([]byte(key))
		}
		return nil
	})
}

func (db *DB) HdelBucket(name string) error {
	bucketName := bconcat([][]byte{hashPrefix, []byte(name)})
	return db.DB.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(bucketName)
	})
}

func (db *DB) Hget(name, key string) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []*Entry{},
	}
	bucketName := bconcat([][]byte{hashPrefix, []byte(name)})
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

func (db *DB) Hmget(name string, keys []string) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []*Entry{},
	}
	bucketName := bconcat([][]byte{hashPrefix, []byte(name)})
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

func (db *DB) Hscan(name, keyStart string, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []*Entry{},
	}
	bucketName := bconcat([][]byte{hashPrefix, []byte(name)})
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

func (db *DB) Hrscan(name, keyStart string, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []*Entry{},
	}
	bucketName := bconcat([][]byte{hashPrefix, []byte(name)})
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

// ---- zet ----

func (db *DB) Zset(name, key string, val int64) error {
	score := itob(val)
	keyB := []byte(key)
	keyBucket := bconcat([][]byte{zetKeyPrefix, []byte(name)})
	scoreBucket := bconcat([][]byte{zetScorePrefix, []byte(name)})
	newKey := bconcat([][]byte{score, keyB})
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
				oldKey := bconcat([][]byte{oldScore, keyB})
				err1 = b1.Delete(oldKey)
				if err1 != nil {
					return err1
				}
			}
		}
		return nil
	})
}

func (db *DB) Zmset(name string, kv map[string]int64) error {
	newKv := map[string][]byte{}
	for k, v := range kv {
		newKv[k] = itob(v)
	}

	keyBucket := bconcat([][]byte{zetKeyPrefix, []byte(name)})
	scoreBucket := bconcat([][]byte{zetScorePrefix, []byte(name)})

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
			newKey := bconcat([][]byte{score, keyB})

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
					oldKey := bconcat([][]byte{oldScore, keyB})
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

func (db *DB) Zincr(name, key string, step int64) (int64, error) {
	var score int64

	keyB := []byte(key)
	keyBucket := bconcat([][]byte{zetKeyPrefix, []byte(name)})
	scoreBucket := bconcat([][]byte{zetScorePrefix, []byte(name)})

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
			score = btoi(vOld)
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
		newScoreB := itob(score)
		newKey := bconcat([][]byte{newScoreB, keyB})

		err1 = b1.Put(newKey, []byte{})
		if err1 != nil {
			return err1
		}

		err2 = b2.Put(keyB, newScoreB)
		if err2 != nil {
			return err2
		}

		if vOld != nil {
			oldKey := bconcat([][]byte{vOld, keyB})
			err1 = b1.Delete(oldKey)
			if err1 != nil {
				return err1
			}
		}
		return nil
	})
	return score, err
}

func (db *DB) Zdel(name, key string) error {
	keyB := []byte(key)
	keyBucket := bconcat([][]byte{zetKeyPrefix, []byte(name)})
	scoreBucket := bconcat([][]byte{zetScorePrefix, []byte(name)})
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
			oldKey := bconcat([][]byte{oldScore, keyB})
			err := b1.Delete(oldKey)
			if err != nil {
				return err
			}
			return b2.Delete(keyB)
		}
		return nil
	})
}

func (db *DB) ZdelBucket(name string) error {
	keyBucket := bconcat([][]byte{zetKeyPrefix, []byte(name)})
	scoreBucket := bconcat([][]byte{zetScorePrefix, []byte(name)})
	return db.DB.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(keyBucket)
		if err != nil {
			return err
		}
		return tx.DeleteBucket(scoreBucket)
	})
}

func (db *DB) Zget(name, key string) *ZetReply {
	r := &ZetReply{
		State: replyError,
		Data:  []*ZetEntry{},
	}
	scoreBucket := bconcat([][]byte{zetScorePrefix, []byte(name)})
	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(scoreBucket)
		if b == nil {
			return errors.New(replyNotFound)
		}
		v := b.Get([]byte(key))
		if v != nil {
			r.State = replyOK
			r.Data = append(r.Data, &ZetEntry{key, int64(binary.BigEndian.Uint64(v))})
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

func (db *DB) Zmget(name string, keys []string) *ZetReply {
	r := &ZetReply{
		State: replyError,
		Data:  []*ZetEntry{},
	}
	scoreBucket := bconcat([][]byte{zetScorePrefix, []byte(name)})

	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(scoreBucket)
		if b == nil {
			return errors.New(replyNotFound)
		}
		for _, key := range keys {
			k := []byte(key)
			v := b.Get(k)
			if v != nil {
				r.Data = append(r.Data, &ZetEntry{key, int64(binary.BigEndian.Uint64(v))})
			}
		}

		return nil
	})
	if err == nil {
		r.State = replyOK
	}
	return r
}

func (db *DB) Zscan(name, keyStart, scoreStart string, limit int) *ZetReply {
	r := &ZetReply{
		State: replyError,
		Data:  []*ZetEntry{},
	}
	keyBucket := bconcat([][]byte{zetKeyPrefix, []byte(name)})

	startScore := scoreMin
	if len(scoreStart) > 0 {
		i, err := strconv.ParseInt(scoreStart, 10, 64)
		if err != nil {
			return r
		}
		startScore = i
	}

	scoreStartB := itob(startScore)
	startScoreKeyB := bconcat([][]byte{scoreStartB, []byte(keyStart)})

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
				r.Data = append(r.Data, &ZetEntry{string(k[8:]), int64(binary.BigEndian.Uint64(k[0:8]))})
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

func (db *DB) Zrscan(name, keyStart, scoreStart string, limit int) *ZetReply {
	r := &ZetReply{
		State: replyError,
		Data:  []*ZetEntry{},
	}
	keyBucket := bconcat([][]byte{zetKeyPrefix, []byte(name)})

	startScore := scoreMax
	if len(scoreStart) > 0 {
		i, err := strconv.ParseInt(scoreStart, 10, 64)
		if err != nil {
			return r
		}
		startScore = i
	}

	startKey := []byte{255}
	if len(keyStart) > 0 {
		startKey = []byte(keyStart)
	}

	scoreStartB := itob(startScore)
	startScoreKeyB := bconcat([][]byte{scoreStartB, []byte(startKey)})

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
				r.Data = append(r.Data, &ZetEntry{string(k[8:]), int64(binary.BigEndian.Uint64(k[0:8]))})
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

// Reply method

func (r *Reply) String() string {
	if len(r.Data) > 0 {
		return string(r.Data[0].Value)
	}
	return ""
}

func (r *Reply) Int() int {
	return int(r.Int64())
}

func (r *Reply) Int64() int64 {
	if len(r.Data) < 1 {
		return 0
	}
	return int64(r.Uint64())
}

func (r *Reply) Uint() uint {
	return uint(r.Uint64())
}

func (r *Reply) Uint64() uint64 {
	if len(r.Data) < 1 {
		return 0
	}
	if len(r.Data[0].Value) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(r.Data[0].Value)
}

func (r *Reply) Dict() map[string][]byte {
	dict := make(map[string][]byte)
	if len(r.Data) < 2 {
		return dict
	}
	for _, i := range r.Data {
		dict[i.Key] = i.Value
	}
	return dict
}

func (r *Reply) Json(v interface{}) error {
	return json.Unmarshal(r.Data[0].Value, &v)
}

func (r *ZetReply) String() string {
	if len(r.Data) > 0 {
		return strconv.FormatInt(r.Data[0].Value,10)
	}
	return ""
}

func (r *ZetReply) Int() int {
	return int(r.Int64())
}

func (r *ZetReply) Int64() int64 {
	if len(r.Data) < 1 {
		return 0
	}
	return int64(r.Uint64())
}

func (r *ZetReply) Uint() uint {
	return uint(r.Uint64())
}

func (r *ZetReply) Uint64() uint64 {
	if len(r.Data) < 1 {
		return 0
	}
	return uint64(r.Data[0].Value)
}

func (r *ZetReply) Dict() map[string]int64 {
	dict := make(map[string]int64)
	if len(r.Data) < 2 {
		return dict
	}
	for _, i := range r.Data {
		dict[i.Key] = i.Value
	}
	return dict
}

// Entry method

func (r *Entry) ValStr() string {
	return string(r.Value)
}

func (r *Entry) ValInt64() int64 {
	if len(r.Value) < 8 {
		return -1
	}
	return int64(binary.BigEndian.Uint64(r.Value))
}

func (r *Entry) Json(v interface{}) error {
	return json.Unmarshal(r.Value, v)
}

func (r *ZetEntry) ValStr() string {
	return strconv.FormatInt(r.Value,10)
}

func (r *ZetEntry) ValInt() int {
	return int(r.Value)
}

// ----- util func ----

func bconcat(slices [][]byte) []byte {
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

func itob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func btoi(v []byte) int64 {
	return int64(binary.BigEndian.Uint64(v))
}
