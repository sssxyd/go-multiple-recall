package radix

// 根据字典词，去除其中的高频前缀后缀，创建多维度索引词，包括：字符、拼音、拼音缩写

import (
	"log"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
)

func _step3_calc_index_word_weight(word string) int {
	subs := strings.Split(word, " ")
	if len(subs) > 1 {
		return len(subs)
	}
	if HasHanChar(word) {
		return len([]rune(word))
	}
	return 1
}

func _step3_split_dict_word_to_index_words(dictWords []DictWord, dictPrefixs map[string][]string, dictSuffixs map[string][]string, maskCount int) []IndexWord {
	charWordIndexSet := make(map[string]IndexWord)
	for _, dw := range dictWords {
		words := strings.Split(dw.WordChars, "|")
		sentence := CreateIndexSentence(words)
		sentence.IndexSentenceTrim(dictPrefixs[dw.Dict], dictSuffixs[dw.Dict], 6)
		index_words := sentence.SplitToIndexWords(maskCount, true)
		for _, sub := range index_words {
			sub = strings.TrimSpace(sub)
			if len(sub) == 0 {
				continue
			}
			if iw, exist := charWordIndexSet[sub]; !exist {
				charWordIndexSet[sub] = IndexWord{
					Type:    0,
					Word:    sub,
					WordLen: _step3_calc_index_word_weight(sub),
					DictId:  map[int]bool{dw.ID: true},
				}
			} else {
				iw.DictId[dw.ID] = true
				charWordIndexSet[sub] = iw
			}
		}
	}

	results := make([]IndexWord, 0, len(charWordIndexSet))
	for _, iw := range charWordIndexSet {
		results = append(results, iw)
		if HasHanChar(iw.Word) {
			py, ok := PinyinOfWord(iw.Word)
			if !ok {
				continue
			}
			pyWord := IndexWord{
				Type:    1,
				Word:    py,
				WordLen: _step3_calc_index_word_weight(py),
				DictId:  iw.DictId,
			}
			results = append(results, pyWord)
		}
	}

	return results
}

func _step3_load_prefix_suffix(db *sqlx.DB, minFreq int) (map[string][]string, map[string][]string) {
	prefixMap := make(map[string][]string)
	suffixMap := make(map[string][]string)

	var repeatWords []DictWordRepeat
	err := db.Select(&repeatWords, "SELECT id, dict, type, word, word_len, repeat_count FROM dict_word_repeats WHERE repeat_count >= ? and ((type = 0 and word_len > 3) or type = 1) order by type, dict, word_len desc", minFreq)
	if err != nil {
		log.Printf("Query failed: %v", err)
		return prefixMap, suffixMap
	}

	for _, rw := range repeatWords {
		if rw.Type == 0 {
			if _, exists := prefixMap[rw.Dict]; !exists {
				prefixMap[rw.Dict] = []string{}
			}
			prefixMap[rw.Dict] = append(prefixMap[rw.Dict], rw.Word)
		} else {
			if _, exists := suffixMap[rw.Dict]; !exists {
				suffixMap[rw.Dict] = []string{}
			}
			suffixMap[rw.Dict] = append(suffixMap[rw.Dict], rw.Word)
		}
	}

	return prefixMap, suffixMap
}

// 读取字典词 dict_words 表；每批次100条，通过通道传递
func step3_proc_range_read_dict_words(db *sqlx.DB, idrange IDRange, recordCh chan<- []IndexWord, dictPrefixs map[string][]string, dictSuffixs map[string][]string, maskCount int) error {
	range_batch := 150
	for i := idrange.MinId; i <= idrange.MaxId; i += range_batch {
		var records []DictWord
		err := db.Select(&records, "SELECT id, dict, word_chars FROM dict_words WHERE id >= ? AND id < ? ORDER BY id", i, i+range_batch)
		if err != nil {
			log.Printf("Query failed: %v", err)
			continue
		}
		index_records := _step3_split_dict_word_to_index_words(records, dictPrefixs, dictSuffixs, maskCount)
		if len(index_records) == 0 {
			continue
		}
		log.Printf("Read %d dict words from range [%d - %d] and split to %d index words", len(records), i, i+range_batch, len(index_records))
		recordCh <- index_records
	}
	return nil
}

func _step3_query_exist_index_dict_relations(tx *sqlx.Tx, indexIds []int) (map[int][]int, error) {
	indexToDictMap := make(map[int][]int)
	if len(indexIds) == 0 {
		return indexToDictMap, nil
	}

	const batchSize = 800 // 每批次的最大参数数量，根据数据库的限制调整, sqlite现在最大999个参数

	for i := 0; i < len(indexIds); i += batchSize {
		end := i + batchSize
		if end > len(indexIds) {
			end = len(indexIds)
		}

		batch := indexIds[i:end]

		query, args, err := sqlx.In("SELECT index_id, dict_id FROM dict_index_ids WHERE index_id IN (?)", batch)
		if err != nil {
			log.Printf("构建查询语句失败: %v", err)
			return nil, err
		}
		query = tx.Rebind(query)

		rows, err := tx.Queryx(query, args...)
		if err != nil {
			log.Printf("查询失败: %v", err)
			return nil, err
		}
		defer rows.Close()

		// 遍历结果并填充到 map 中
		for rows.Next() {
			var indexID, dictID int
			if err := rows.Scan(&indexID, &dictID); err != nil {
				log.Printf("扫描结果失败: %v", err)
				continue
			}
			// 检查是否已初始化切片
			if _, ok := indexToDictMap[indexID]; !ok {
				indexToDictMap[indexID] = []int{}
			}
			indexToDictMap[indexID] = append(indexToDictMap[indexID], dictID)
		}
	}

	return indexToDictMap, nil
}

func _step3_query_index_words(tx *sqlx.Tx, index_words []string) (map[string]int, error) {
	const batchSize = 800 // 每批次的最大参数数量，根据数据库的限制调整, sqlite现在最大999个参数
	exists_index_words := make(map[string]int)

	// 分批处理 index_keys
	for i := 0; i < len(index_words); i += batchSize {
		end := i + batchSize
		if end > len(index_words) {
			end = len(index_words)
		}

		batch := index_words[i:end]

		query, args, err := sqlx.In("SELECT id, type, word FROM index_words WHERE word IN (?)", batch)
		if err != nil {
			log.Printf("构建查询语句失败: %v", err)
			tx.Rollback()
			return nil, err
		}
		query = tx.Rebind(query)

		var batchResults []IndexWord
		err = tx.Select(&batchResults, query, args...)
		if err != nil {
			log.Printf("查询失败: %v", err)
			tx.Rollback()
			return nil, err
		}

		for _, iw := range batchResults {
			exists_index_words[iw.Word] = iw.ID
		}
	}

	return exists_index_words, nil
}

func _step3_insert_index_word(tx *sqlx.Tx, indexWords []IndexWord) int {
	const batchSize = 1000
	count := 0

	// 分批处理 indexWords
	for i := 0; i < len(indexWords); i += batchSize {
		end := i + batchSize
		if end > len(indexWords) {
			end = len(indexWords)
		}

		batch := indexWords[i:end]

		// 插入新的索引词
		stmt, err := tx.Prepare("INSERT INTO index_words (type, word, word_len) VALUES (?, ?, ?)")
		if err != nil {
			tx.Rollback()
			log.Fatalf("准备语句失败: %v", err)
		}
		for i := range batch {
			result, err := stmt.Exec(batch[i].Type, batch[i].Word, batch[i].WordLen)
			if err != nil {
				tx.Rollback()
				log.Fatalf("插入记录失败: %v", err)
			}
			count++
			nid, _ := result.LastInsertId()
			batch[i].ID = int(nid)
		}
		stmt.Close()
	}

	return count
}

func _step3_insert_index_dict_relation(tx *sqlx.Tx, indexWords []IndexWord) int {
	const batchSize = 1000
	count := 0

	// 分批处理 indexWords
	for i := 0; i < len(indexWords); i += batchSize {
		end := i + batchSize
		if end > len(indexWords) {
			end = len(indexWords)
		}

		batch := indexWords[i:end]

		// 插入新的索引词与字典词的关系
		stmt, err := tx.Prepare("INSERT INTO dict_index_ids (index_id, dict_id) VALUES (?, ?)")
		if err != nil {
			tx.Rollback()
			log.Fatalf("准备语句失败: %v", err)
		}

		// 插入新的索引词与字典词的关系
		for _, indexWord := range batch {
			if len(indexWord.DictId) == 0 {
				continue
			}
			for dictId, _ := range indexWord.DictId {
				_, err := stmt.Exec(indexWord.ID, dictId)
				if err != nil {
					tx.Rollback()
					log.Fatalf("插入记录失败: %v", err)
				}
				count++
			}
		}
		stmt.Close()
	}

	return count
}

func step3_proc_create_index_words(db *sqlx.DB, recordCh <-chan []IndexWord) int {
	count := 0
	for batch := range recordCh {
		tx, err := db.Beginx() // 开启事务
		if err != nil {
			log.Fatalf("事务开启失败: %v", err)
		}
		insert_count := 0
		relation_count := 0

		index_words := make([]string, 0, len(batch))
		for _, rec := range batch {
			index_words = append(index_words, rec.Word)
		}

		// 查询已存在的索引词
		exists_index_words, err := _step3_query_index_words(tx, index_words)
		if err != nil {
			tx.Rollback()
			log.Fatalf("查询失败: %v", err)
		}

		exist_index_ids := []int{}
		update_index_word_set := make(map[int]IndexWord)
		insert_index_words := []IndexWord{}
		for _, rec := range batch {
			if indexId, exist := exists_index_words[rec.Word]; exist {
				rec.ID = indexId
				exist_index_ids = append(exist_index_ids, indexId)
				update_index_word_set[indexId] = rec
			} else {
				insert_index_words = append(insert_index_words, rec)
			}
		}

		// 更新已存在的索引词与字典词的关系
		indexToDictMap, err := _step3_query_exist_index_dict_relations(tx, exist_index_ids)
		if err != nil {
			tx.Rollback()
			log.Fatalf("查询失败: %v", err)
		}
		for indexId, dictIds := range indexToDictMap {
			if iw, exist := update_index_word_set[indexId]; exist {
				for _, dictId := range dictIds {
					delete(iw.DictId, dictId)
				}
				// 将修改后的索引词加入到更新集合中
				update_index_word_set[indexId] = iw
			}
		}

		// 插入新的索引词
		insert_count = _step3_insert_index_word(tx, insert_index_words)

		// 插入索引词与字典词的关系
		for _, indexWord := range update_index_word_set {
			if len(indexWord.DictId) == 0 {
				continue
			}
			insert_index_words = append(insert_index_words, indexWord)
		}
		relation_count = _step3_insert_index_dict_relation(tx, insert_index_words)

		// 提交事务
		if err := tx.Commit(); err != nil {
			tx.Rollback()
			log.Fatalf("事务提交失败: %v", err)
		}
		log.Printf("插入索引词 %d 条，插入索引词与字典词关系 %d 条", insert_count, relation_count)
		count += insert_count
	}
	return count
}

func step3_main_create_index_words(db *sqlx.DB, maskCount int, minFreq int) int {
	// 创建通道
	recordCh := make(chan []IndexWord, 100)

	table_range := getTableRange(db, "dict_words", "")
	if table_range.Count == 0 {
		return 0
	}

	dictPrefixs, dictSuffixs := _step3_load_prefix_suffix(db, minFreq)

	var wg sync.WaitGroup

	worker_ranges := table_range.Split(10000, 0)
	// worker_ranges := []IDRange{table_range}
	log.Printf("词典共计 %d 条记录，分为 %d 个协程并行读取", table_range.Count, len(worker_ranges))
	for _, wr := range worker_ranges {
		wg.Add(1)
		go func(idrange IDRange) {
			defer wg.Done()
			if err := step3_proc_range_read_dict_words(db, idrange, recordCh, dictPrefixs, dictSuffixs, maskCount); err != nil {
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
