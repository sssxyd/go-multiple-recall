package radix

// 创建索引第二部：根据字典词，创建多维度索引词，包括：字符、拼音、拼音缩写

import (
	"log"
	"sync"

	"github.com/jmoiron/sqlx"
)

// 切词规则，每个汉字算2个字符, 索引词最小长度为4
// 1. 每个字典词作为一个切分索引词
// 2. 每个字典词按前缀切分，作为多个索引词，索引词最小长度为4，每个汉字算2个字符，作为切分索引词
// 3. 每个切分索引词，按unicode排序，作为一个乱序索引词
// 4. 每个切分索引词，除首尾各一个rune以外，中间的rune，按1个和2个掩码打码，作为掩码索引词
// 5. 每个切分索引词、乱序索引词、掩码索引词，如果包含至少一个汉字，则按拼音排序，作为拼音索引词
// 6. 每个切分索引词、乱序索引词、掩码索引词，如果包含至少一个汉字，则按拼音缩写排序，作为拼音缩写索引词
func split_dict_word_to_index_words(dictWord []DictWord) []IndexWord {
	return []IndexWord{}
}

// 读取字典词 dict_words 表；每批次1000条，通过通道传递
func step3_proc_range_read_dict_words(db *sqlx.DB, idrange IDRange, recordCh chan<- []IndexWord) error {
	range_batch := idrange.GetRangeBatch(1000)
	for i := idrange.MinId; i <= idrange.MaxId; i += range_batch {
		var records []DictWord
		err := db.Select(&records, "SELECT id, word_chars FROM dict_words WHERE id >= ? AND id <= ? ORDER BY id", i, i+range_batch)
		if err != nil {
			log.Printf("Query failed: %v", err)
			continue
		}
		index_records := split_dict_word_to_index_words(records)
		if len(index_records) == 0 {
			continue
		}
		recordCh <- index_records
	}
	return nil
}

func step3_proc_create_index_words(db *sqlx.DB, recordCh <-chan []IndexWord) (int, int) {
	return 0, 0
}

func step3_main_create_index_words(db *sqlx.DB) (int, int) {
	// 创建通道
	recordCh := make(chan []IndexWord, 100)

	table_range := getTableRange(db, "dict_words", "")
	if table_range.Count == 0 {
		return 0, 0
	}

	var wg sync.WaitGroup

	worker_ranges := table_range.Split(10000, 0)
	log.Printf("词典共计 %d 条记录，分为 %d 个协程并行读取", table_range.Count, len(worker_ranges))
	for _, wr := range worker_ranges {
		wg.Add(1)
		go func(idrange IDRange) {
			defer wg.Done()
			if err := step3_proc_range_read_dict_words(db, idrange, recordCh); err != nil {
				log.Printf("Error reading dict words: %v\n", err)
			}
		}(wr)
	}

	// 等待所有读取协程完成
	go func() {
		wg.Wait()
		close(recordCh)
	}()

	// 用当前主协程处理读取的字典词
	return step3_proc_create_index_words(db, recordCh)
}
