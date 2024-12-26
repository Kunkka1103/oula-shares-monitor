package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var (
	QuaiDSN = flag.String("quaiDSN", "", "Quai DSN, e.g. postgres://user:password@host:5432/quai")
	AleoDSN = flag.String("aleoDSN", "", "Aleo DSN, e.g. postgres://user:password@host:5432/aleo")
	OpsDSN  = flag.String("opsDSN", "", "Ops DSN, e.g. user:password@tcp(host:3306)/ops_db")
	interval = flag.Int("interval", 10, "Check interval in minutes")

	//// 数据库连接
	quaiDB *sql.DB
	aleoDB *sql.DB
	opsDB  *sql.DB

	// 记录上一次检查到的最大 epoch（内存变量示例，可持久化到文件/数据库）
	lastMaxEpochQuai int64
	lastMaxEpochAleo int64
)

func main() {
	flag.Parse()
	if *QuaiDSN == "" || *AleoDSN == "" || *OpsDSN == "" {
		log.Panicln("All DSN parameters (quaiDSN, aleoDSN, opsDSN) are required.")
	}

	// 初始化数据库连接
	var err error
	quaiDB, err := InitDB(*QuaiDSN, "postgres")
	if err != nil {
		log.Panicf("init QuaiDSN failed,Error: %v", err)
	}
	defer quaiDB.Close()

	aleoDB, err := InitDB(*AleoDSN, "postgres")
	if err != nil {
		log.Panicf("init AleoDSN failed,Error: %v", err)
	}
	defer aleoDB.Close()

	opsDB, err := InitDB(*OpsDSN, "mysql")
	if err != nil {
		log.Panicf("init OpsDSN failed,Error: %v", err)
	}
	defer opsDB.Close()

	// 使用用户指定的 interval 作为 Ticker 周期
	ticker := time.NewTicker(time.Duration(*interval) * time.Minute)
	defer ticker.Stop()

	log.Printf("Starting check loop with interval = %d minute(s)\n", *interval)
	for {
		select {
		case <-ticker.C:
			CheckAndUpdate()
		}
	}
}

// InitDB 初始化数据库连接
func InitDB(DSN string, DBType string) (*sql.DB, error) {
	db, err := sql.Open(DBType, DSN)
	if err != nil {
		return nil, fmt.Errorf("sql.Open failed: %w", err)
	}
	// 测试连接
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("db.Ping failed: %w", err)
	}
	return db, nil
}

// CheckAndUpdate 主逻辑：定期检查 Quai 和 Aleo DB 的 shares 表，插入新 epoch 数据到 Ops DB
func CheckAndUpdate() {
	log.Println("CheckAndUpdate triggered.")

	if err := checkEpochChanges("quai", quaiDB, &lastMaxEpochQuai); err != nil {
		log.Printf("checkEpochChanges failed for Quai: %v\n", err)
	}

	if err := checkEpochChanges("aleo", aleoDB, &lastMaxEpochAleo); err != nil {
		log.Printf("checkEpochChanges failed for Aleo: %v\n", err)
	}
}

// checkEpochChanges 查询 sourceDB 中的最新 epoch，若大于本地记录，则插入区间内的 epoch 信息到 opsDB。
func checkEpochChanges(chainName string, sourceDB *sql.DB, lastMaxEpoch *int64) error {
	// 1. 查询 sourceDB 中的最新 epoch
	var newMaxEpoch sql.NullInt64
	err := sourceDB.QueryRow("SELECT MAX(epoch_number) FROM shares").Scan(&newMaxEpoch)
	if err != nil {
		return fmt.Errorf("failed to query MAX(epoch_number): %w", err)
	}
	if !newMaxEpoch.Valid {
		// 数据库里还没有任何 epoch，跳过
		log.Printf("[%s] No epoch found in sourceDB.\n", chainName)
		return nil
	}

	if newMaxEpoch.Int64 <= *lastMaxEpoch {
		// 没有新的 epoch
		log.Printf("[%s] No new epoch. lastMaxEpoch=%d, newMaxEpoch=%d\n",
			chainName, *lastMaxEpoch, newMaxEpoch.Int64)
		return nil
	}

	// 2. 将区间 (lastMaxEpoch+1, newMaxEpoch-1) 的 epoch 数据写入 Ops
	start := *lastMaxEpoch + 1
	end := newMaxEpoch.Int64 - 1
	if start <= end {
		if err := insertEpochRangeToOps(chainName, sourceDB, opsDB, start, end); err != nil {
			return fmt.Errorf("failed to insert epoch range [%d,%d] for %s: %w", start, end, chainName, err)
		}
	}

	// 3. 更新 lastMaxEpoch
	*lastMaxEpoch = newMaxEpoch.Int64
	log.Printf("[%s] Updated lastMaxEpoch to %d\n", chainName, *lastMaxEpoch)
	return nil
}

// insertEpochRangeToOps 从 sourceDB 中查询指定区间 [startEpoch, endEpoch] 的 epoch 及其 count
// 然后插入到 opsDB 的 shares_epoch_counts (或你自定义的) 表中。
func insertEpochRangeToOps(chainName string, sourceDB, opsDB *sql.DB, startEpoch, endEpoch int64) error {
	// 查询 sourceDB
	query := `
        SELECT epoch_number, COUNT(*) AS share_count
        FROM shares
        WHERE epoch_number BETWEEN $1 AND $2
        GROUP BY epoch_number
        ORDER BY epoch_number;
    `
	rows, err := sourceDB.Query(query, startEpoch, endEpoch)
	if err != nil {
		return fmt.Errorf("sourceDB.Query failed: %w", err)
	}
	defer rows.Close()

	// 准备往 Ops 写入
	insertSQL := `
        INSERT INTO shares_epoch_counts (chain, epoch, share_count, created_at)
        VALUES (?, ?, ?, NOW());
    `
	tx, err := opsDB.Begin()
	if err != nil {
		return fmt.Errorf("opsDB.Begin failed: %w", err)
	}

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("tx.Prepare failed: %w", err)
	}
	defer stmt.Close()

	countRecords := 0
	for rows.Next() {
		var epochVal, shareCount int64
		if err := rows.Scan(&epochVal, &shareCount); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("rows.Scan failed: %w", err)
		}

		if _, err = stmt.Exec(chainName, epochVal, shareCount); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("stmt.Exec failed: %w", err)
		}
		countRecords++
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("rows iteration err: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("tx.Commit failed: %w", err)
	}

	log.Printf("[%s] Inserted %d epochs into Ops DB, range [%d,%d]\n",
		chainName, countRecords, startEpoch, endEpoch)
	return nil
}
