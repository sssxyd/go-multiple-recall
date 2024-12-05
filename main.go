package main

import (
	"fmt"
	"log"
	"multiple-recall/basic/com"
	"multiple-recall/service/radix"
	"path/filepath"
	"time"
)

func main() {

	// sentence := "哆啦A梦 添乐●儿童滋养洗发沐浴露2合1"
	// start_time := time.Now().UnixMicro()
	// is := radix.NewIndexSentence(sentence)
	// words := is.SplitToIndexWords(2, true)
	// end_time := time.Now().UnixMicro()
	// for _, w := range words {
	// 	println(w)
	// }
	// fmt.Printf("cost: %d us, total: %d\n", end_time-start_time, len(words))
	// fmt.Println(sentence)
	// fmt.Println(is.ToString())
	// fmt.Println(is.ToPinyin())
	// fmt.Println(is.ToWords())

	log_path := filepath.Join(com.GetExecutionPath(), "logs", "app.log")
	log_file, err := com.InitializeLogFile(log_path, true)
	if err != nil {
		fmt.Printf("Failed to initialize log file: %v\n", err)
	}
	defer log_file.Close()

	start := time.Now().UnixMilli()

	base_dir := com.GetExecutionPath()

	// dict_dir := filepath.Join(base_dir, "dict")
	// index_dir := filepath.Join(base_dir, "index")
	// index_name := ""
	// index_path, err := radix.NewIndex(dict_dir, index_dir, index_name, 2, 100)
	// if err != nil {
	// 	log.Printf("Error creating index: %v\n", err)
	// } else {
	// 	log.Printf("Index created at: %s\n", index_path)
	// }

	index_path := filepath.Join(base_dir, "index", "1122.bin")
	_, err = radix.DebugIndex(index_path, 2, 100)
	if err != nil {
		log.Printf("Error creating index: %v\n", err)
	}

	end := time.Now().UnixMilli()
	println("cost: ", end-start, "ms\n")
}
