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
	fmt.Println("----------------hash")
	fmt.Println(db.Hset("mytest", "key1", []byte("value 2")))
	fmt.Println(db.Hget("mytest", "key1"))
	fmt.Println(db.Hset("mytest", "key2", []byte("123000099876521398")))
	rs := db.Hget("mytest", "key2")
	fmt.Println(rs.String(), rs.Int64(), rs.Int(), rs.Uint(), rs.Uint64())

	dataMap := map[string][]byte{}
	dataMap["k1"] = []byte("12987887762987")
	dataMap["k2"] = []byte("abc")
	dataMap["k3"] = []byte("qwertyui")
	dataMap["k4"] = []byte("aaaa")
	dataMap["k5"] = []byte("aaaa555")
	dataMap["k6"] = []byte("aaaa556")
	dataMap["k7"] = []byte("77777")
	dataMap["k8"] = []byte("88888")

	fmt.Println(db.Hmset("myhmset", dataMap))
	fmt.Println(db.Hmget("myhmset", []string{"k1", "k2", "k3", "k8"}))

	fmt.Println("----Hmget")
	rs = db.Hmget("myhmset", []string{"k1", "k2", "k3", "k4", "k8"})
	for _, v := range rs.Data {
		fmt.Println(v.Key, v.ValStr(), v.ValInt64())
	}

	hargs := []string{"", "k2", "k3", "k4", "k6"}

	fmt.Println("----Hscan----")
	for _, v := range hargs {
		fmt.Println("keyStart:", v)
		rs = db.Hscan("myhmset", v, 4)
		for _, v := range rs.Data {
			fmt.Println(v.Key, v.ValStr(), v.ValInt64(), v.Value)
		}
		fmt.Println()
	}

	fmt.Println("----Hrscan----")
	for _, v := range hargs {
		fmt.Println("keyStart:", v)
		rs = db.Hrscan("myhmset", v, 4)
		for _, v := range rs.Data {
			fmt.Println(v.Key, v.ValStr(), v.ValInt64(), v.Value)
		}
		fmt.Println()
	}

	fmt.Println("-----Hincr")
	fmt.Println(db.Hincr("num", "k1", 2))

	fmt.Println(db.Hdel("myhmsetxxx", "xsl"))
	fmt.Println(db.HdelBucket("myhmsetxxx222"))

	// zet
	fmt.Println("----------------zet")
	fmt.Println(db.Zset("mytest", "key1", 100))
	fmt.Println(db.Zget("mytest", "key1"))
	fmt.Println(db.Zget("mytest22", "key1")) // bucket not exist
	fmt.Println(db.Zget("mytest", "key1xxx")) // key not exist
	fmt.Println(db.Zset("mytest", "key2", 123000099876521398))
	rs2 := db.Zget("mytest", "key2")
	fmt.Println(rs2.String(), rs2.Int64(), rs2.Int(), rs2.Uint(), rs2.Uint64())

	dataMap2 := map[string]int64{}
	dataMap2["k1"] = 1
	dataMap2["k2"] = 2
	dataMap2["k3"] = 3
	dataMap2["k9"] = 3
	dataMap2["k8"] = 3
	dataMap2["k4"] = 4
	dataMap2["k5"] = 5
	dataMap2["k6"] = 6
	dataMap2["k7"] = 7
	dataMap2["k10"] = 8
	dataMap2["k11"] = 89

	fmt.Println(db.Zmset("myhmset", dataMap2))
	fmt.Println(db.Zmget("myhmset", []string{"k1", "k2", "k3"}))

	fmt.Println("----Zmget")
	rs2 = db.Zmget("myhmset", []string{"k1", "k2", "k3", "k4"})
	for _, v := range rs2.Data {
		fmt.Println(v.Key, v.Value)
	}

	zargs := [][]string{
		[]string{"", ""},
		[]string{"k2", "2"},
		[]string{"k3", "2"},
		[]string{"k6", "6"},
		[]string{"", "6"},
	}

	fmt.Println("----Zscan----")
	for _, v := range zargs {
		fmt.Println("keystart:", v[0], "scoreStart:", v[1])
		rs2 = db.Zscan("myhmset", v[0], v[1], 4)
		for _, v := range rs2.Data {
			fmt.Println(v.Key, v.Value)
		}
		fmt.Println()
	}

	fmt.Println("----Zrscan----")
	for _, v := range zargs {
		fmt.Println("keystart:", v[0], "scoreStart:", v[1])
		rs2 = db.Zrscan("myhmset", v[0], v[1], 4)
		for _, v := range rs2.Data {
			fmt.Println(v.Key, v.Value)
		}
		fmt.Println()
	}

	fmt.Println("-----Zincr")
	fmt.Println(db.Zincr("num", "k1", -1234))

	fmt.Println("-----Zdel not exist")
	fmt.Println(db.Zdel("notexist", "test"))

	fmt.Println("-----Zdel")
	fmt.Println(db.Zset("mytest2", "key1", 101))
	fmt.Println(db.Zget("mytest2", "key1").Uint64())
	fmt.Println(db.Zdel("mytest2", "key1"))
	fmt.Println("get again", db.Zget("mytest2", "key1"))

	fmt.Println(db.Zdel("myhmsetxxx", "xsl"))
	fmt.Println(db.ZdelBucket("myhmsetxxx222"))
}
