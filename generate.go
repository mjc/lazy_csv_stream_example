package main

// https://stackoverflow.com/a/15622206/19881
// go run generate.go  82.40s user 8.28s system 101% cpu 1:29.26 total
import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	fileSize := int64(10e9) // 10GB
	f, err := os.Create("large.csv")
	if err != nil {
		fmt.Println(err)
		return
	}
	w := bufio.NewWriter(f)
	prefixes := []string{"name", "time", "number", "butts", "foo"}
	names := []string{"jbill", "dkennedy"}
	timeStart := time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC)
	timeDur := timeStart.AddDate(1, 0, 0).Sub(timeStart)
	rand.Seed(time.Now().UnixNano())
	size := int64(0)
	w.WriteString(strings.Join(prefixes, ", ") + "\n")
	for size < fileSize {
		name := names[int(rand.Int31n(int32(len(names))))]
		time := timeStart.Add(time.Duration(rand.Int63n(int64(timeDur)))).Format("2006/1/2")
		number := strconv.Itoa(int(rand.Int31n(100) + 1))
		butts := "yes"
		foo := "bar"
		row := []string{name, time, number, butts, foo}
		line := strings.Join(row, ",") + "\n"
		n, err := w.WriteString(line)
		if err != nil {
			fmt.Println(n, err)
			return
		}
		size += int64(len(line))
	}
	err = w.Flush()
	if err != nil {
		fmt.Println(err)
		return
	}
	err = f.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Size:", size)
}
