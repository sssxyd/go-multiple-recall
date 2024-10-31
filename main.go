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

	log_path := filepath.Join(com.GetExecutionPath(), "logs", "app.log")
	log_file, err := com.InitializeLogFile(log_path, true)
	if err != nil {
		fmt.Printf("Failed to initialize log file: %v\n", err)
	}
	defer log_file.Close()

	start := time.Now().UnixMilli()

	base_dir := com.GetExecutionPath()
	dict_dir := filepath.Join(base_dir, "dict")
	index_dir := filepath.Join(base_dir, "index")
	index_name := ""
	index_path, err := radix.NewIndex(dict_dir, index_dir, index_name)
	if err != nil {
		log.Printf("Error creating index: %v\n", err)
	} else {
		log.Printf("Index created at: %s\n", index_path)
	}

	// word := "奥利司他胶囊"
	// words := radix.TrieSplitWord(word, true, 2)
	// for _, w := range words {
	// 	println(w)
	// }

	end := time.Now().UnixMilli()
	println("cost: ", end-start, "ms\n")
}
