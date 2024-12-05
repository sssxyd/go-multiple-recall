package radix

import (
	"log"
	"sync"

	"github.com/jmoiron/sqlx"
)

type NodeChild struct {
	Pid  int
	Cids []int
}

func _step5_get_max_weight(db *sqlx.DB) int {
	sql := `select max(weight) from str_radix_nodes`
	var max_weight int
	err := db.Get(&max_weight, sql)
	if err != nil {
		log.Printf("get max weight in str_radix_nodes failed: %v", err)
	}
	return max_weight
}

func _step5_calc_parent_and_child_count(db *sqlx.DB, weight int, recordCh chan<- []NodeChild) {
	parent_weight := weight - 1
	sql := `
	with c as (
		select id as cid, SUBSTR(hierarchy_key, 1, LENGTH(hierarchy_key) - LENGTH(key)) AS ckey 
		from str_radix_nodes where weight = $1
	)
	select id as pid, cid from str_radix_nodes p inner join c on p.hierarchy_key = c.ckey 
	where p.weight = $2 
	`
	var parent_child []struct { // parent_child is a slice of struct
		Pid int `db:"pid"`
		Cid int `db:"cid"`
	}
	err := db.Select(&parent_child, sql, weight, parent_weight)
	if err != nil {
		log.Printf("get parent and child failed: %v", err)
	}
	if len(parent_child) == 0 {
		return
	}

	parent_child_map := make(map[int][]int)
	for _, pc := range parent_child {
		if _, ok := parent_child_map[pc.Pid]; !ok {
			parent_child_map[pc.Pid] = make([]int, 0)
		}
		parent_child_map[pc.Pid] = append(parent_child_map[pc.Pid], pc.Cid)
	}

	batch_size := 100
	batch_nodes := make([]NodeChild, 0, batch_size)
	for pid, cids := range parent_child_map {
		batch_nodes = append(batch_nodes, NodeChild{Pid: pid, Cids: cids})
		if len(batch_nodes) == batch_size {
			recordCh <- batch_nodes
			batch_nodes = make([]NodeChild, 0, batch_size)
		}
	}
	if len(batch_nodes) > 0 {
		recordCh <- batch_nodes
	}
}

func _step5_update_parent_and_child_count(db *sqlx.DB, recordCh <-chan []NodeChild) {
	sql_child_count := `update str_radix_nodes set child_count = :cnt where id = :pid`
	sql_parent_id := `update str_radix_nodes set parent_id = :pid where id = :cid`
	for batch := range recordCh {
		batch_len := len(batch)
		if batch_len == 0 {
			continue
		}

		tx, err := db.Beginx()
		if err != nil {
			log.Fatalf("transaction begin failed: %v", err)
		}

		for _, node := range batch {
			_, err = tx.NamedExec(sql_child_count, map[string]interface{}{"cnt": len(node.Cids), "pid": node.Pid})
			if err != nil {
				log.Printf("update failed: %v", err)
				tx.Rollback()
				return
			}
			for _, cid := range node.Cids {
				_, err = tx.NamedExec(sql_parent_id, map[string]interface{}{"pid": node.Pid, "cid": cid})
				if err != nil {
					log.Printf("update failed: %v", err)
					tx.Rollback()
					return
				}
			}
		}

		err = tx.Commit()
		if err != nil {
			log.Fatalf("transaction commit failed: %v", err)
		}

		log.Printf("update parent and child count success: %d", batch_len)
	}
}

func step5_main_clac_heirarchy(db *sqlx.DB) {
	recordCh := make(chan []NodeChild, 100)

	max_weight := _step5_get_max_weight(db)
	var wg sync.WaitGroup
	for i := 2; i < max_weight; i++ {
		wg.Add(1)
		go func(weight int) {
			defer wg.Done()
			_step5_calc_parent_and_child_count(db, weight, recordCh)
		}(i)
	}

	// 等待所有读取协程完成
	go func() {
		wg.Wait()
		close(recordCh)
	}()

	// 用当前协程写数据
	_step5_update_parent_and_child_count(db, recordCh)
}
