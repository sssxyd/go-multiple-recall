package radix

// 根据索引词 index_words, 创建索引树 str_radix_nodes，根据word_len逐层创建索引节点，不压缩

import (
	"fmt"
	"log"
	"sync"

	"github.com/jmoiron/sqlx"
)

func _step4_create_or_update_radix_node(db *sqlx.DB, recordch <-chan []StrRadixNode) int {
	return 0
}

func _step4_parse_index_word_to_radix_node(iw IndexWord) []StrRadixNode {

	return nil
}

func _step4_read_index_word_to_radix_node(db *sqlx.DB, recordCh chan<- []StrRadixNode, idrange IDRange, word_len int) {
	sql := "select id, word, word_len from index_words where word_len = ? and id >= ? and id <= ?"
	var records []IndexWord
	err := db.Select(&records, sql, word_len, idrange.MinId, idrange.MaxId)
	if err != nil {
		log.Printf("parse index word to radix node failed: %v", err)
		return
	}
	batch := 500
	for _, r := range records {
		rns := make([]StrRadixNode, 0, batch)
		for _, rn := range _step4_parse_index_word_to_radix_node(r) {
			rns = append(rns, rn)
			if len(rns) >= batch {
				recordCh <- rns
				rns = make([]StrRadixNode, 0, batch)
			}
		}
	}
}

func _step4_create_radix_node_level(db *sqlx.DB, level int) int {
	level_range := getTableRange(db, "index_words", fmt.Sprintf("where word_len = %d", level))
	if level_range.Count == 0 {
		return 0
	}

	// 创建通道
	recordCh := make(chan []StrRadixNode, 100)

	ranges := level_range.Split(3000, 0)
	var wg sync.WaitGroup

	for _, r := range ranges {
		wg.Add(1)
		go func(r IDRange) {
			defer wg.Done()
			_step4_read_index_word_to_radix_node(db, recordCh, r, level)
		}(r)
	}

	// 等待所有读取协程完成
	go func() {
		wg.Wait()
		close(recordCh)
	}()

	// 用当前协程写数据
	return _step4_create_or_update_radix_node(db, recordCh)
}

func _step4_get_max_word_len_in_index_words(db *sqlx.DB) int {
	sql := `select max(word_len) from index_words`
	var max_word_len int
	err := db.Get(&max_word_len, sql)
	if err != nil {
		log.Printf("get max word len in index_words failed: %v", err)
	}
	return max_word_len
}

func step4_main_create_radix_node(db *sqlx.DB) int {
	max_len := _step4_get_max_word_len_in_index_words(db)
	log.Printf("逐层创建[2-%d]索引节点", max_len)
	total := 0
	for i := 2; i <= max_len; i++ {
		count := _step4_create_radix_node_level(db, i)
		total += count
		log.Printf("创建 %d 级索引节点 %d 条", i, count)
	}
	log.Printf("共创建 %d 条索引节点", total)
	return total
}
