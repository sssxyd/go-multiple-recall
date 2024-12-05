package radix

import (
	"log"
	"multiple-recall/basic/com"
	"path/filepath"
	"time"
)

func NewIndex(dict_dir string, index_dir string, index_name string, maskCount int, minFreq int) (string, error) {
	start_time := time.Now().UnixMilli()
	com.TouchDir(index_dir)
	if index_name == "" {
		index_name = time.Now().Format("20060102150405")
	}
	index_path := filepath.Join(index_dir, index_name+".bin")
	db, err := initialize_indexdb(index_path, true)
	if err != nil {
		return "", err
	}
	log.Printf(">>>Step0: 初始化索引数据库 %s，耗时 %d ms", index_path, time.Now().UnixMilli()-start_time)

	defer db.Close()

	start_time = time.Now().UnixMilli()
	csv_cnt, dict_cnt := step1_main_collect_dict_words(db, dict_dir)
	log.Printf(">>>Step1: 共读取 %d 条记录，成功插入 %d 条词条，耗时 %d ms", csv_cnt, dict_cnt, time.Now().UnixMilli()-start_time)

	start_time = time.Now().UnixMilli()
	repeat_count := step2_main_collect_word_repeat_parts(db)
	log.Printf(">>>Step2: 计算得出 %d 个高频出现的前缀后缀，耗时 %d ms", repeat_count, time.Now().UnixMilli()-start_time)

	start_time = time.Now().UnixMilli()
	index_count := step3_main_create_index_words(db, maskCount, minFreq)
	log.Printf(">>>Setp3: 创建索引 %d 条记录，耗时 %d ms", index_count, time.Now().UnixMilli()-start_time)

	start_time = time.Now().UnixMilli()
	node_count := step4_main_create_radix_node(db)
	log.Printf(">>>Setp4: 创建节点 %d 条记录，耗时 %d ms", node_count, time.Now().UnixMilli()-start_time)

	return index_path, nil
}

func DebugIndex(index_path string, maskCount int, minFreq int) (string, error) {
	start_time := time.Now().UnixMilli()
	db, err := initialize_indexdb(index_path, false)
	if err != nil {
		return "", err
	}
	log.Printf(">>>Step0: 初始化索引数据库 %s，耗时 %d ms", index_path, time.Now().UnixMilli()-start_time)

	defer db.Close()

	start_time = time.Now().UnixMilli()
	// index_count := step3_main_create_index_words(db, maskCount, minFreq)
	// log.Printf(">>>Setp3: 创建索引 %d 条记录，耗时 %d ms", index_count, time.Now().UnixMilli()-start_time)

	// index_count := step4_main_create_radix_node(db)
	// log.Printf(">>>Setp4: 创建平铺节点 %d 条记录，耗时 %d ms", index_count, time.Now().UnixMilli()-start_time)

	step5_main_clac_heirarchy(db)
	log.Printf(">>>Setp5: 耗时 %d ms", time.Now().UnixMilli()-start_time)

	return index_path, nil
}
