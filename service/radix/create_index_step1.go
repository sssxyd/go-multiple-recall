package radix

// 从字典文件中读取词条并插入数据库

import (
	"encoding/csv"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

func list_dicts(dirPath string) []string {
	// 读取目录
	dirs, err := os.ReadDir(dirPath)
	if err != nil {
		return make([]string, 0)
	}

	// 过滤出 .dict 后缀的文件
	var dicts []string
	for _, dir := range dirs {
		if !dir.IsDir() {
			fileName := dir.Name()
			if strings.HasSuffix(strings.ToLower(fileName), ".csv") {
				dicts = append(dicts, fileName[:len(fileName)-4])
			}
		}
	}
	return dicts
}

func step1_proc_read_csv_dict_words(dictName string, filePath string, recordCh chan<- []DictWord) {
	start_time := time.Now().UnixMilli()
	count := 0
	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Printf("无法打开文件 %s: %v", filePath, err)
		return
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	_, _ = reader.Read() // 跳过标题行
	batch := make([]DictWord, 0, 1000)

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Printf("读取文件 %s 失败: %v", filePath, err)
			continue
		}

		name := strings.TrimSpace(record[0])
		data := strings.TrimSpace(record[1])
		if name == "" {
			continue
		}
		if data == "" {
			data = "{}"
		}

		sentence := NewIndexSentence(name)
		batch = append(batch, DictWord{
			Dict:       dictName,
			Name:       name,
			Data:       data,
			WordChars:  sentence.ToString(),
			WordPinyin: sentence.ToPinyin(),
		})

		if len(batch) >= 1000 { // 每1000条发送一次
			count += len(batch)
			log.Printf("从字典[%s]读取 %d 条记录", dictName, len(batch))
			recordCh <- batch
			batch = make([]DictWord, 0, 1000) // 清空批量缓存
		}
	}

	if len(batch) > 0 { // 发送剩余不足1000的记录
		count += len(batch)
		recordCh <- batch
	}

	log.Printf("字典[%s]读取完成，共 %d 条记录，耗时 %d 毫秒", dictName, count, time.Now().UnixMilli()-start_time)
}

func step1_proc_write_dict_words(db *sqlx.DB, recordCh <-chan []DictWord) (int, int) {
	read := 0
	count := 0
	for batch := range recordCh {
		read += len(batch)
		tx, err := db.Begin() // 开启事务
		if err != nil {
			log.Printf("事务开启失败: %v", err)
			continue
		}

		stmt, err := tx.Prepare("INSERT INTO dict_words (dict, name, data, word_chars, word_pinyin) VALUES (?, ?, ?, ?, ?)")
		if err != nil {
			log.Printf("准备语句失败: %v", err)
			tx.Rollback()
			continue
		}

		for _, rec := range batch {
			_, err := stmt.Exec(rec.Dict, rec.Name, rec.Data, rec.WordChars, rec.WordPinyin)
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
			tx.Commit()
		}
	}
	return read, count
}

func step1_main_collect_dict_words(db *sqlx.DB, dict_dir string) (int, int) {
	dicts := list_dicts(dict_dir)
	if len(dicts) == 0 {
		return 0, 0
	}

	recordCh := make(chan []DictWord, 10) // 用于传输批量记录
	var wg sync.WaitGroup

	// 启动多个协程来读取不同的CSV文件
	for _, dictName := range dicts {
		wg.Add(1)
		go func(dict string, csv_path string) {
			defer wg.Done()
			step1_proc_read_csv_dict_words(dict, csv_path, recordCh)
		}(dictName, filepath.Join(dict_dir, dictName+".csv"))
	}

	// 启动一个协程来等待所有读取协程完成
	go func() {
		wg.Wait()       // 等待所有读取协程完成
		close(recordCh) // 关闭通道以停止写协程
	}()

	// 用当前协程来写入数据库
	return step1_proc_write_dict_words(db, recordCh)
}
