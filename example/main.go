package main

import (
	"fmt"
	"github.com/ego008/youdb"
)

func main() {
	db, err := youdb.Open("my.db")
	if err != nil {
		fmt.Println("open err", err)
		return
	}
	defer db.Close()

	// hash

	db.Hset("mytest", []byte("key1"), []byte("value1"))

	rs := db.Hget("mytest", []byte("key1"))
	if rs.State == "ok" {
		fmt.Println(rs.Data[0], rs.String())
	} else {
		fmt.Println("key not found")
	}

	var data [][]byte
	data = append(data, []byte("k1"), []byte("12987887762987"))
	data = append(data, []byte("k2"), []byte("abc"))
	data = append(data, []byte("k3"), []byte("qwasww"))
	data = append(data, []byte("k4"), []byte("444"))
	data = append(data, []byte("k5"), []byte("555"))
	data = append(data, []byte("k6"), []byte("aaaa556"))
	data = append(data, []byte("k7"), []byte("77777"))
	data = append(data, []byte("k8"), []byte("88888"))

	db.Hmset("myhmset", data...)

	rs = db.Hmget("myhmset", [][]byte{[]byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")})
	if rs.State == "ok" {
		for _, v := range rs.List() {
			fmt.Println(v.Key.String(), v.Value.String())
		}
	}

	db.Hset("new2", []byte("123456783:abcd:xzaksas"), []byte("value1"))
	db.Hset("new2", []byte("123456781:zsabcd:xzaksas"), []byte("value1"))
	db.Hset("new2", []byte("123456782:1q23wqwqqw:xzaksa3"), []byte("value1"))

	rs = db.Hscan("new2", []byte(""), 4)
	for k, v := range rs.List() {
		fmt.Println(k, v.Key.String(), v.Value.String())
	}

	rs = db.Hscan("new2", []byte("123456781"), 4)
	for k, v := range rs.List() {
		fmt.Println(k, v.Key.String(), v.Value.String())
	}

	fmt.Println(db.Hincr("num", []byte("k1"), 2))

	k := youdb.DS2b("19822112")
	v := uint64(121211121212233)
	db.Hset("mytestnum", k, youdb.I2b(v))
	r := db.Hget("mytestnum2", k)
	if r.State == "ok" {
		fmt.Println(r.Int64(), r.Data[0].Int64(), string(r.Data[0]))
	} else {
		fmt.Println(r.State, r.Int64())
	}

	// zet

	db.Zset("mytest", []byte("key1"), 100)

	rs2 := db.Zget("mytest", []byte("key1"))
	if rs2.State == "ok" {
		fmt.Println(rs2.Int64())
	}

	kk := []byte("b7hko2sfm7j6h5r83fg0:click:40701bdbd9dca8bd62cb446ed1654b8d")
	db.Zset("tt2", kk, 2)
	fmt.Println("new get:", db.Zget("tt2", kk).State)
	if rr := db.Zget("tt2", kk); rr.State == "ok" {
		fmt.Println("okkk")
	}

	fmt.Println(db.Zincr("num", []byte("k1"), 2))

	db.Zset("test", youdb.I2b(2017), 5002)
	db.Zset("test", youdb.I2b(2018), 5001)
	db.Zset("test", youdb.I2b(2016), 4999)
	db.Zset("test", youdb.I2b(2011), 4000)

	rs = db.Zscan("test", youdb.I2b(2016), youdb.I2b(4999), 2)
	fmt.Println(youdb.B2i(rs.Data[0]), youdb.B2i(rs.Data[1]))

	db.Zset("test2", []byte("2017"), 5002)
	db.Zset("test2", []byte("2018"), 5001)
	db.Zset("test2", []byte("2016"), 4999)
	db.Zset("test2", []byte("2011"), 4000)

	fmt.Println("------")
	rs = db.Zscan("test2", []byte("2017"), youdb.I2b(5000), 2)
	fmt.Println(string(rs.Data[0]), youdb.B2i(rs.Data[1]))

	//rs = db.Zrscan("test", youdb.I2b(2017), youdb.I2b(5002), 1)
	//fmt.Println(youdb.B2i(rs.Data[0]), youdb.B2i(rs.Data[1]))
	fmt.Println(db.HnextSequence("test3"))
}
