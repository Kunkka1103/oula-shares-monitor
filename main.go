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
	quaiDB, err = InitDB(*QuaiDSN, "postgres")
	if err != nil {
		log.Panicf("init QuaiDSN failed,Error: %v", err)
	}
	defer quaiDB.Close()

	aleoDB, err = InitDB(*AleoDSN, "postgres")
	if err != nil {
		log.Panicf("init AleoDSN failed,Error: %v", err)
	}
	defer aleoDB.Close()

	opsDB, err = InitDB(*OpsDSN, "mysql")
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

	// Quai: 表名 shares
	if err := checkEpochChanges("quai", "shares", quaiDB, &lastMaxEpochQuai); err != nil {
		log.Printf("checkEpochChanges failed for Quai: %v\n", err)
	}

	// Aleo: 表名 user_shares
	if err := checkEpochChanges("aleo", "user_shares", aleoDB, &lastMaxEpochAleo); err != nil {
		log.Printf("checkEpochChanges failed for Aleo: %v\n", err)
	}
}


// checkEpochChanges 查询 sourceDB 中的最新 epoch，若大于本地记录，则插入区间内的 epoch 信息到 opsDB。
func checkEpochChanges(chainName, tableName string, sourceDB *sql.DB, lastMaxEpoch *int64) error {
	// 1. 动态拼 SQL，把 tableName 带进来
	query := fmt.Sprintf("SELECT MAX(epoch_number) FROM %s", tableName)

	var newMaxEpoch sql.NullInt64
	err := sourceDB.QueryRow(query).Scan(&newMaxEpoch)
	if err != nil {
		return fmt.Errorf("failed to query MAX(epoch_number): %w", err)
	}
	if !newMaxEpoch.Valid {
		log.Printf("[%s] No epoch found in table %s.\n", chainName, tableName)
		return nil
	}

	if newMaxEpoch.Int64 <= *lastMaxEpoch {
		log.Printf("[%s] No new epoch. lastMaxEpoch=%d, newMaxEpoch=%d\n",
			chainName, *lastMaxEpoch, newMaxEpoch.Int64)
		return nil
	}

	// 2.插入区间...
	start := *lastMaxEpoch + 1
	end := newMaxEpoch.Int64 - 1
	if start <= end {
		if err := insertEpochRangeToOps(chainName, tableName, sourceDB, opsDB, start, end); err != nil {
			return fmt.Errorf("failed to insert epoch range [%d,%d] for %s: %w", start, end, chainName, err)
		}
	}

	*lastMaxEpoch = newMaxEpoch.Int64
	log.Printf("[%s] Updated lastMaxEpoch to %d\n", chainName, *lastMaxEpoch)
	return nil
}


// insertEpochRangeToOps 从 sourceDB 中查询指定区间 [startEpoch, endEpoch] 的 epoch 及其 count
// 然后插入到 opsDB 的 shares_epoch_counts (或你自定义的) 表中。
func insertEpochRangeToOps(chainName, tableName string, sourceDB, opsDB *sql.DB, startEpoch, endEpoch int64) error {
	query := fmt.Sprintf(`
        SELECT epoch_number, COUNT(*) AS share_count
        FROM %s
        WHERE epoch_number BETWEEN $1 AND $2
        GROUP BY epoch_number
        ORDER BY epoch_number;
    `, tableName)

	rows, err := sourceDB.Query(query, startEpoch, endEpoch)
	if err != nil {
		return fmt.Errorf("sourceDB.Query failed: %w", err)
	}
	defer rows.Close()

	insertSQL := `
        INSERT INTO shares_epoch_counts (chain, epoch, share_count, created_at)
        VALUES (?, ?, ?, NOW());
    `
	tx, err := opsDB.Begin()
	if err != nil {
		return fmt.Errorf("opsDB.Begin failed: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

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

