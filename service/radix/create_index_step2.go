package radix

// 从字典词条的 word_chars 里，收集重复的中文前缀、后缀；要求至少两个及以上连续的汉字

import (
	"log"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
)

// 获取高频的前缀和后缀
func findCommonPrefixesAndSuffixes(words []string, minFreq int) (map[string]int, map[string]int) {
	if minFreq <= 0 {
		minFreq = 10
	}

	prefixFreq := make(map[string]int)
	suffixFreq := make(map[string]int)

	for _, word := range words {
		// 遍历前缀
		runes := []rune(word)
		rune_len := len(runes)
		if rune_len < 4 {
			continue
		}
		for i := 2; i <= 6 && i <= rune_len && rune_len-i > 2; i++ {
			prefix := string(runes[:i])
			if HasHanChar(prefix) {
				prefixFreq[prefix]++
			}
		}

		// 遍历后缀
		for i := 2; i <= 6 && i <= rune_len && rune_len-i > 2; i++ {
			suffix := string(runes[rune_len-i:])
			if HasHanChar(suffix) {
				suffixFreq[suffix]++
			}
		}
	}

	// 筛选出频率高于 minFreq 的前缀和后缀
	commonPrefixes := make(map[string]int)
	commonSuffixes := make(map[string]int)

	for prefix, freq := range prefixFreq {
		if freq >= minFreq {
			commonPrefixes[prefix] = freq
		}
	}

	for suffix, freq := range suffixFreq {
		if freq >= minFreq {
			commonSuffixes[suffix] = freq
		}
	}

	return commonPrefixes, commonSuffixes
}

func _step2_list_distinct_dicts(db *sqlx.DB) []string {
	var dicts []string
	err := db.Select(&dicts, "SELECT DISTINCT dict FROM dict_words")
	if err != nil {
		log.Printf("Query failed: %v", err)
		return make([]string, 0)
	}
	return dicts
}

func _step2_collect_dict_chinese_words(db *sqlx.DB, dict string) []string {
	idrange := getTableRange(db, "dict_words", "where dict = '"+dict+"'")
	if idrange.Count == 0 {
		return make([]string, 0)
	}
	words := make(map[string]bool)
	subs := idrange.Split(5000, 0)
	for _, sub := range subs {
		var dict_words []string
		err := db.Select(&dict_words, "SELECT word_chars FROM dict_words WHERE dict = ? AND id >= ? AND id <= ?", dict, sub.MinId, sub.MaxId)
		if err != nil {
			log.Printf("Query failed: %v", err)
			continue
		}
		for _, word := range dict_words {
			split_words := strings.Split(word, "|")
			for _, w := range split_words {
				if HasHanChar(w) {
					words[w] = true
				}
			}
		}
	}
	results := make([]string, 0, len(words))
	for word := range words {
		results = append(results, word)
	}
	log.Printf("从字典[%s]中读取 %d 条词条，用于计算高频前缀后缀；\n", dict, len(results))
	return results
}

func step2_proc_collect_dict_word_repeats(db *sqlx.DB, dict string, minFreq int, recordCh chan<- []DictWordRepeat) {
	words := _step2_collect_dict_chinese_words(db, dict)
	commonPrefixes, commonSuffixes := findCommonPrefixesAndSuffixes(words, minFreq)

	batch := make([]DictWordRepeat, 1000)
	count := 0
	for k, v := range commonPrefixes {
		batch = append(batch, DictWordRepeat{Dict: dict, Type: 0, Word: k, WordLen: len([]rune(k)), RepeatCount: int(v)})
		count++

		// 如果达到批次大小，则加入批次，并清空当前 batch
		if count >= 1000 {
			recordCh <- batch
			batch = make([]DictWordRepeat, 1000)
			count = 0
		}
	}

	// 添加最后一批（如果有剩余）
	if count > 0 {
		recordCh <- batch
	}

	batch = make([]DictWordRepeat, 1000)
	count = 0
	for k, v := range commonSuffixes {
		batch = append(batch, DictWordRepeat{Dict: dict, Type: 1, Word: k, WordLen: len([]rune(k)), RepeatCount: int(v)})
		count++

		// 如果达到批次大小，则加入批次，并清空当前 batch
		if count >= 1000 {
			recordCh <- batch
			batch = make([]DictWordRepeat, 1000)
			count = 0
		}
	}

	// 添加最后一批（如果有剩余）
	if count > 0 {
		recordCh <- batch
	}
}

func step2_proc_write_dict_word_repeats(db *sqlx.DB, recordCh <-chan []DictWordRepeat) int {
	count := 0
	for batch := range recordCh {
		tx, err := db.Begin() // 开启事务
		if err != nil {
			log.Printf("事务开启失败: %v", err)
			continue
		}

		stmt, err := tx.Prepare("INSERT INTO dict_word_repeats (dict, type, word, word_len, repeat_count) VALUES (?, ?, ?, ?, ?)")
		if err != nil {
			log.Printf("准备语句失败: %v", err)
			tx.Rollback()
			continue
		}

		for _, rec := range batch {
			if rec.Dict == "" || rec.RepeatCount <= 0 || rec.WordLen < 2 {
				continue
			}
			_, err := stmt.Exec(rec.Dict, rec.Type, rec.Word, rec.WordLen, rec.RepeatCount)
			if err != nil {
				log.Printf("插入记录失败: %v", err)
			} else {
				count += 1
			}
		}
		stmt.Close()

		if err := tx.Commit(); err != nil {
			log.Printf("事务提交失败: %v", err)
			tx.Rollback()
		} else {
			log.Printf("成功插入 %d 条记录\n", len(batch))
		}
	}
	return count
}

func step2_main_collect_word_repeat_parts(db *sqlx.DB) int {
	dicts := _step2_list_distinct_dicts(db)
	if len(dicts) == 0 {
		return 0
	}

	// 创建通道
	recordCh := make(chan []DictWordRepeat, 10)
	var wg sync.WaitGroup

	for _, dict := range dicts {
		wg.Add(1)
		go func(name string) {
			step2_proc_collect_dict_word_repeats(db, name, 10, recordCh)
			wg.Done()
		}(dict)
	}

	// 等待所有读取协程完成
	go func() {
		wg.Wait()
		close(recordCh)
	}()

	// 用当前主协程处理读取的字典词
	return step2_proc_write_dict_word_repeats(db, recordCh)
}
