# youdb
A Bolt wrapper that allows easy store hash, zset data.

## Install

```
go get -u github.com/ego008/youdb
```

## Example

```
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

	db.Hset("mytest", "key1", []byte("value1"))

	rs := db.Hget("mytest", "key1")
	if rs.State == "ok" {
		fmt.Println(rs.Data[0], rs.String())
	} else {
		fmt.Println("key not found")
	}

	dataMap := map[string][]byte{}
	dataMap["k1"] = []byte("12987887762987")
	dataMap["k2"] = []byte("abc")
	dataMap["k3"] = []byte("qwertyui")
	dataMap["k4"] = []byte("aaaa")
	dataMap["k5"] = []byte("aaaa555")
	dataMap["k6"] = []byte("aaaa556")
	dataMap["k7"] = []byte("77777")
	dataMap["k8"] = []byte("88888")

	db.Hmset("myhmset", dataMap)

	rs = db.Hmget("myhmset", []string{"k1", "k2", "k3", "k8"})
	if rs.State == "ok" {
		for _, v := range rs.Data {
			fmt.Println(v.Key, v.ValStr())
		}
	}

	fmt.Println(db.Hincr("num", "k1", 2))

	// zet

	db.Zset("mytest", "key1", 100)

	rs2 := db.Zget("mytest", "key1")
	if rs2.State == "ok" {
		fmt.Println(rs2.Int64())
	}

	fmt.Println(db.Zincr("num", "k1", 2))
}
```


