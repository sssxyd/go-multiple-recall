package radix

// 根据索引词 index_words, 创建索引树 str_radix_nodes，根据word_len逐层创建索引节点，不压缩

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
)

func _step4_query_and_merge_radix_node(tx *sqlx.Tx, nodes []StrRadixNode) ([]StrRadixNode, error) {
	hierarchyKeys := make([]string, 0, len(nodes))
	batchNodes := make(map[string]StrRadixNode, len(nodes))
	for _, n := range nodes {
		hierarchyKeys = append(hierarchyKeys, n.HierarchyKey)
		batchNodes[n.HierarchyKey] = n
	}

	query, args, err := sqlx.In("select id, parent_id, key, hierarchy_key, index_id, weight, child_count from str_radix_nodes where hierarchy_key in (?)", hierarchyKeys)
	if err != nil {
		log.Printf("构建查询语句失败: %v", err)
		tx.Rollback()
		return nil, err
	}
	query = tx.Rebind(query)

	var batchResults []StrRadixNode
	err = tx.Select(&batchResults, query, args...)
	if err != nil {
		log.Printf("查询失败: %v", err)
		tx.Rollback()
		return nil, err
	}

	update_nodes := make([]StrRadixNode, 0, len(nodes))
	for _, rn := range batchResults {
		src, exists := batchNodes[rn.HierarchyKey]
		if exists {
			if rn.IndexID == 0 && src.IndexID > 0 {
				rn.IndexID = src.IndexID
				update_nodes = append(update_nodes, rn)
			}
			delete(batchNodes, rn.HierarchyKey)
		}
	}

	sql := `update str_radix_nodes set index_id = :index_id where id = :id`
	for _, rn := range update_nodes {
		_, err = tx.NamedExec(sql, rn)
		if err != nil {
			log.Printf("更新失败: %v", err)
			tx.Rollback()
			return nil, err
		}
	}

	not_exist_nodes := make([]StrRadixNode, 0, len(batchNodes))
	for _, v := range batchNodes {
		not_exist_nodes = append(not_exist_nodes, v)
	}

	return not_exist_nodes, nil
}

func _step4_insert_radix_node(tx *sqlx.Tx, nodes []StrRadixNode) error {
	sql := `insert into str_radix_nodes (parent_id, key, hierarchy_key, index_id, weight, child_count) values (:parent_id, :key, :hierarchy_key, :index_id, :weight, :child_count)`
	for _, rn := range nodes {
		_, err := tx.NamedExec(sql, rn)
		if err != nil {
			log.Printf("插入失败: %v", err)
			tx.Rollback()
			return err
		}
	}
	return nil
}

func _step4_create_or_update_radix_node(db *sqlx.DB, recordCh <-chan []StrRadixNode) int {
	total := 0
	for batch := range recordCh {
		batch_len := len(batch)
		if batch_len == 0 {
			continue
		}

		tx, err := db.Beginx() // 开启事务
		if err != nil {
			log.Fatalf("事务开启失败: %v", err)
		}

		// 查询已存在的索引词
		not_exist_nodes, err := _step4_query_and_merge_radix_node(tx, batch)
		if err != nil {
			tx.Rollback()
			log.Fatalf("查询失败: %v", err)
		}
		insert_len := len(not_exist_nodes)
		update_len := len(batch) - len(not_exist_nodes)

		// 插入不存在的索引词
		if len(not_exist_nodes) > 0 {
			err = _step4_insert_radix_node(tx, not_exist_nodes)
			if err != nil {
				tx.Rollback()
				log.Fatalf("插入失败: %v", err)
			}
		}

		// 提交事务
		if err := tx.Commit(); err != nil {
			tx.Rollback()
			log.Fatalf("事务提交失败: %v", err)
		}
		log.Printf("插入 %d 条, 更新 %d 条", insert_len, update_len)
		total += insert_len
	}
	return total
}

func _step4_parse_index_word_to_radix_node(iw IndexWord) []StrRadixNode {
	word := strings.TrimSpace(iw.Word)
	charstrs := strings.Split(word, " ")
	multipart := len(charstrs) > 1
	if !multipart {
		runes := []rune(word)
		charstrs = make([]string, 0, len(runes))
		for _, r := range runes {
			charstrs = append(charstrs, string(r))
		}
	}
	rune_len := len(charstrs)
	nodes := make([]StrRadixNode, 0, rune_len-1)

	first := StrRadixNode{
		Key: func() string {
			if multipart {
				return strings.Join(charstrs[:2], " ")
			} else {
				return strings.Join(charstrs[:2], "")
			}
		}(),
		HierarchyKey: func() string {
			if multipart {
				return strings.Join(charstrs[:2], " ")
			} else {
				return strings.Join(charstrs[:2], "")
			}
		}(),
		IndexID: func() int {
			if rune_len == 2 {
				return iw.ID
			} else {
				return 0
			}
		}(),
		Weight:     2,
		ParentID:   0,
		ChildCount: 0,
	}
	nodes = append(nodes, first)

	for i := 3; i <= rune_len; i++ {
		sr := StrRadixNode{
			Key: charstrs[i-1],
			HierarchyKey: func() string {
				if multipart {
					return strings.Join(charstrs[:i], " ")
				} else {
					return strings.Join(charstrs[:i], "")
				}
			}(),
			IndexID: func() int {
				if rune_len == i {
					return iw.ID
				} else {
					return 0
				}
			}(),
			Weight:     i,
			ParentID:   0,
			ChildCount: 0,
		}
		nodes = append(nodes, sr)
	}
	return nodes
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
	rns := make(map[string]StrRadixNode, batch)
	for _, r := range records {
		for _, rn := range _step4_parse_index_word_to_radix_node(r) {
			src, exists := rns[rn.HierarchyKey]
			if exists {
				src.Merge(&rn)
				rns[src.HierarchyKey] = src
			} else {
				rns[rn.HierarchyKey] = rn
			}
		}
		if len(rns) >= batch {
			nodes := make([]StrRadixNode, 0, len(rns))
			for _, v := range rns {
				nodes = append(nodes, v)
			}
			recordCh <- nodes
			rns = make(map[string]StrRadixNode, batch)
		}
	}
	if len(rns) > 0 {
		nodes := make([]StrRadixNode, 0, len(rns))
		for _, v := range rns {
			nodes = append(nodes, v)
		}
		recordCh <- nodes
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
	for i := 1; i <= max_len; i++ {
		count := _step4_create_radix_node_level(db, i)
		total += count
		log.Printf("创建 %d 级索引节点 %d 条", i, count)
	}
	log.Printf("共创建 %d 条索引节点", total)
	return total
}
