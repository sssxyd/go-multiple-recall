package radix

import (
	"fmt"
	"log"
	"math"
	"multiple-recall/basic/com"
	"os"
	"path/filepath"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type DictWord struct {
	ID         int    `db:"id"`
	Dict       string `db:"dict"`
	Name       string `db:"name"`
	Data       string `db:"data"`
	WordChars  string `db:"word_chars"`
	WordPinyin string `db:"word_pinyin"`
}

type DictWordRepeat struct {
	ID          int    `db:"id"`
	Dict        string `db:"dict"`
	Type        int    `db:"type"`
	Word        string `db:"word"`
	WordLen     int    `db:"word_len"`
	RepeatCount int    `db:"repeat_count"`
}

type IndexWord struct {
	ID      int    `db:"id"`
	Type    int    `db:"type"`
	Word    string `db:"word"`
	WordLen int    `db:"word_len"`
	DictId  map[int]bool
}

func (iw *IndexWord) Merge(dictId map[int]bool) {
	if iw.DictId == nil {
		iw.DictId = make(map[int]bool)
	}
	for k, v := range dictId {
		iw.DictId[k] = v
	}
}

type StrRadixNode struct {
	ID          int    `db:"id"`
	ParentID    int    `db:"parent_id"`
	HierarchyID string `db:"hierarchy_id"`
	Key         string `db:"key"`
	IndexCount  int    `db:"index_count"`
	ChildCount  int    `db:"child_count"`
}

type IDRange struct {
	MinId int
	MaxId int
	Count int
}

func (r *IDRange) GetRangeBatch(batch int) int {
	if r.Count <= 0 {
		return 0
	}
	density := float64((r.MaxId - r.MinId + 1)) / float64(r.Count)
	return int(math.Ceil(density * float64(batch)))
}

func (r *IDRange) Split(batch int, worker int) []IDRange {
	if batch > r.Count { // 批次数大于总数，则只有一个批次
		return []IDRange{{r.MinId, r.MaxId, r.Count}}
	}

	if worker <= 0 { // 默认分成2n个批次
		worker = 2 * com.GetCpuCount()
	}

	// 每个 worker 处理的批次数
	worker_batch := int(math.Ceil(float64(r.Count) / float64(worker)))
	if worker_batch < batch { // 每个 worker 处理的批次数小于 batch，则以 batch 为准，重新计算 worker 数
		worker_batch = batch
		worker = int(math.Ceil(float64(r.Count) / float64(worker_batch)))
	}

	// 等比膨胀，计算Range内真实的批次数
	worker_batch = r.GetRangeBatch(worker_batch)
	var ranges []IDRange = make([]IDRange, worker)
	for i := 0; i < worker; i++ {
		start := r.MinId + i*worker_batch
		end := start + worker_batch - 1
		if end > r.MaxId {
			end = r.MaxId
		}
		ranges[i] = IDRange{start, end, end - start}
	}

	return ranges
}

/**
 * 创建索引数据库
 * @param index_path 索引数据库路径
 * @return *sqlx.DB 可写数据库连接，最大连接数为1
 * @return error 错误信息
 */
func initialize_indexdb(index_path string, create_table bool) (*sqlx.DB, error) {

	// 检查文件是否存在
	if create_table {
		// 创建目录
		err := os.MkdirAll(filepath.Dir(index_path), os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}

		if _, err := os.Stat(index_path); err == nil {
			return nil, fmt.Errorf("database file[%s] already exists", index_path)
		}

		// 文件不存在，创建空文件
		file, err := os.Create(index_path)
		if err != nil {
			return nil, fmt.Errorf("failed to create database file: %w", err)
		}
		file.Close()
	}

	// 连接数据库
	db, err := sqlx.Connect("sqlite3", index_path)
	if err != nil {
		log.Printf("failed to connect to database: %v", err)
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	// 设置数据库 WAL 模式
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set journal mode: %w", err)
	}

	// 设置数据库连接模式
	db.SetMaxOpenConns(com.GetCpuCount() * 10)
	db.SetMaxIdleConns(0)
	db.SetConnMaxLifetime(6 * time.Hour)

	// 定义各表和索引的 SQL 语句
	if create_table {
		statements := []string{
			`CREATE TABLE "dict_words" (
				"id" INTEGER NOT NULL UNIQUE,
				"dict" TEXT NOT NULL,
				"name" TEXT NOT NULL,
				"data" TEXT NOT NULL,
				"word_chars" TEXT NOT NULL,
				"word_pinyin" TEXT NOT NULL,
				PRIMARY KEY("id" AUTOINCREMENT)
			)`,

			`CREATE TABLE "dict_word_repeats" (
				"id"	INTEGER NOT NULL UNIQUE,
				"dict"	TEXT NOT NULL,
				"type"	INTEGER NOT NULL DEFAULT 0,
				"word"	TEXT NOT NULL,
				"word_len" INTEGER NOT NULL DEFAULT 0,
				"repeat_count"	INTEGER NOT NULL,
				PRIMARY KEY("id" AUTOINCREMENT)
			)`,

			`CREATE TABLE "index_words" (
				"id"	INTEGER NOT NULL UNIQUE,
				"type"	INTEGER NOT NULL DEFAULT 0,
				"word"	TEXT NOT NULL,
				"word_len" INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY("id" AUTOINCREMENT)
			)`,

			`CREATE UNIQUE INDEX "idx_index_words_word" ON "index_words" (
				"word"
			);`,

			`CREATE TABLE "dict_index_ids" (
				"id" INTEGER NOT NULL UNIQUE,
				"dict_id" INTEGER NOT NULL DEFAULT 0,
				"index_id" INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY("id" AUTOINCREMENT)
			)`,

			`CREATE INDEX "idx_dict_index_ids_index_id" ON "dict_index_ids" (
				"index_id" ASC
			)`,

			`CREATE TABLE "str_radix_nodes" (
				"id" INTEGER NOT NULL UNIQUE,
				"parent_id" INTEGER NOT NULL DEFAULT 0,
				"hierarchy_id"	TEXT NOT NULL DEFAULT '',
				"key" TEXT NOT NULL,
				"weight" INTEGER NOT NULL DEFAULT 0,
				"index_count" INTEGER NOT NULL DEFAULT 0,
				"child_count" INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY("id" AUTOINCREMENT)
			)`,

			`CREATE INDEX "idx_str_radix_nodes_parent_id" ON "str_radix_nodes" (
				"parent_id" ASC
			)`,

			`CREATE INDEX "idx_str_radix_nodes_hierarchy_id" ON "str_radix_nodes" (
				"hierarchy_id"
			)`,

			`CREATE TABLE "node_index_ids" (
				"id"	INTEGER NOT NULL UNIQUE,
				"node_id"	INTEGER NOT NULL,
				"index_id"	INTEGER NOT NULL,
				PRIMARY KEY("id" AUTOINCREMENT)
			)`,

			`CREATE INDEX "idx_node_index_ids_node_id" ON "node_index_ids" (
				"node_id" ASC
			)`,
		}

		// 逐条执行 SQL 语句
		for _, stmt := range statements {
			if _, err := db.Exec(stmt); err != nil {
				db.Close()
				return nil, fmt.Errorf("failed to execute statement: %v, error: %w", stmt, err)
			}
		}
	}
	return db, nil
}

func ClearIndex(db *sqlx.DB) error {
	_, err := db.Exec("DROP TABLE IF EXISTS index_words")
	if err != nil {
		return fmt.Errorf("failed to drop table index_words: %w", err)
	}
	_, err = db.Exec("DROP TABLE IF EXISTS rune_trie_nodes")
	if err != nil {
		return fmt.Errorf("failed to drop table rune_trie_nodes: %w", err)
	}
	return nil
}

func ReadIndex(index_path string, max_memory_map_size uint64) (*sqlx.DB, error) {
	fileInfo, err := os.Stat(index_path)
	if err != nil {
		return nil, fmt.Errorf("database file does not exist: %w", err)
	}

	// 获取可用内存的一半，默认64MB
	freeMemory := com.GetAvailableMemory()
	if freeMemory <= 0 {
		freeMemory = 64 * 1024 * 1024
	} else {
		freeMemory /= 2
	}

	fileSize := uint64(fileInfo.Size())
	mmapSize := calculateMmapSize(fileSize, freeMemory, max_memory_map_size)

	// 设置缓存大小：全部映射时象征性设置为1MB，否则为映射大小的1/10
	cacheSize := calculateCacheSize(mmapSize, fileSize)

	// 获取CPU数量并确保最小为1
	cpuCount := com.GetCpuCount()
	if cpuCount < 1 {
		cpuCount = 1
	}

	// 连接数据库
	db, err := sqlx.Connect("sqlite3", fmt.Sprintf("file:%s?cache=shared&mode=ro", index_path))
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	// 设置数据库参数并处理错误
	if _, err := db.Exec("PRAGMA cache_size = -" + fmt.Sprint(cacheSize/1024)); err != nil {
		return nil, fmt.Errorf("failed to set cache size: %w", err)
	}
	if _, err := db.Exec("PRAGMA mmap_size = " + fmt.Sprint(mmapSize)); err != nil {
		return nil, fmt.Errorf("failed to set mmap size: %w", err)
	}
	if _, err := db.Exec("PRAGMA temp_store = MEMORY"); err != nil {
		return nil, fmt.Errorf("failed to set temp_store: %w", err)
	}
	if _, err := db.Exec("PRAGMA synchronous = OFF"); err != nil {
		return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA query_only = true"); err != nil {
		return nil, fmt.Errorf("failed to set journal mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		return nil, fmt.Errorf("failed to disable foreign keys: %w", err)
	}

	// 设置连接池
	db.SetMaxOpenConns(cpuCount * 10)
	db.SetMaxIdleConns(cpuCount)
	db.SetConnMaxLifetime(6 * time.Hour)
	db.SetConnMaxIdleTime(10 * time.Minute)

	return db, nil
}

// calculateMmapSize 计算 mmap 大小
func calculateMmapSize(fileSize, freeMemory, maxMemoryMapSize uint64) uint64 {
	switch {
	case fileSize < 64*1024*1024:
		return fileSize
	case maxMemoryMapSize > 0:
		return min(fileSize, maxMemoryMapSize, freeMemory)
	default:
		return min(fileSize, freeMemory)
	}
}

// calculateCacheSize 计算缓存大小
func calculateCacheSize(mmapSize, fileSize uint64) uint64 {
	baseCacheSize := uint64(1024 * 1024) // 1 MB
	if mmapSize < fileSize {
		return max(uint64(mmapSize/10), baseCacheSize)
	}
	return baseCacheSize
}

func getTableRange(db *sqlx.DB, table string, where_clause string) IDRange {
	var minId int
	err := db.QueryRow("SELECT MIN(id) FROM " + table + " " + where_clause).Scan(&minId)
	if err != nil {
		log.Printf("Query failed: %v", err)
		return IDRange{}
	}

	var maxId int
	err = db.QueryRow("SELECT MAX(id) FROM " + table + " " + where_clause).Scan(&maxId)
	if err != nil {
		log.Printf("Query failed: %v", err)
		return IDRange{}
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM " + table + " " + where_clause).Scan(&count)
	if err != nil {
		log.Printf("Query failed: %v", err)
		return IDRange{}
	}

	return IDRange{minId, maxId, count}
}
