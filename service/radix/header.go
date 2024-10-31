package radix

import (
	"log"
	"multiple-recall/basic/com"
	"path/filepath"
	"time"
)

func NewIndex(dict_dir string, index_dir string, index_name string) (string, error) {
	com.TouchDir(index_dir)
	if index_name == "" {
		index_name = time.Now().Format("20060102150405")
	}
	index_path := filepath.Join(index_dir, index_name+".bin")
	db, err := initialize_indexdb(index_path)
	if err != nil {
		return "", err
	}

	defer db.Close()

	csv_cnt, dict_cnt := step1_main_collect_dict_words(db, dict_dir)
	log.Printf("Step1: 共读取 %d 条记录，成功插入 %d 条词条", csv_cnt, dict_cnt)

	repeat_count := step2_main_collect_word_repeat_parts(db)
	log.Printf("Step2: 计算得出 %d 个高频出现的前缀后缀", repeat_count)

	return index_path, nil
}
