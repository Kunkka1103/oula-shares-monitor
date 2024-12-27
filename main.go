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
	// 命令行参数
	quaiDSN    = flag.String("quaiDSN", "", "Quai DSN, e.g. postgres://user:password@host:5432/quai")
	aleoDSN    = flag.String("aleoDSN", "", "Aleo DSN, e.g. postgres://user:password@host:5432/aleo")
	opsDSN     = flag.String("opsDSN", "", "Ops DSN, e.g. user:password@tcp(host:3306)/ops_db")
	checkMins  = flag.Int("interval", 10, "Check interval in minutes")

	// 数据库连接（全局）
	quaiDB *sql.DB
	aleoDB *sql.DB
	opsDB  *sql.DB

	// 记录上一次处理的最大 epoch
	lastMaxEpochQuai int64
	lastMaxEpochAleo int64
)

func main() {
	flag.Parse()
	if *quaiDSN == "" || *aleoDSN == "" || *opsDSN == "" {
		log.Panicln("All DSN parameters (quaiDSN, aleoDSN, opsDSN) are required.")
	}

	// 1. 初始化数据库连接
	var err error
	quaiDB, err = initDB(*quaiDSN, "postgres")
	if err != nil {
		log.Panicf("Failed to init QuaiDB: %v", err)
	}
	defer quaiDB.Close()

	aleoDB, err = initDB(*aleoDSN, "postgres")
	if err != nil {
		log.Panicf("Failed to init AleoDB: %v", err)
	}
	defer aleoDB.Close()

	opsDB, err = initDB(*opsDSN, "mysql")
	if err != nil {
		log.Panicf("Failed to init OpsDB: %v", err)
	}
	defer opsDB.Close()

	// 2. 程序启动时，从 shares_epoch_counts 中获取这两个链的最新进度
	lastMaxEpochQuai = loadLastEpochFromTable(opsDB, "quai")
	lastMaxEpochAleo = loadLastEpochFromTable(opsDB, "aleo")
	log.Printf("Startup - lastMaxEpochQuai=%d, lastMaxEpochAleo=%d\n",
		lastMaxEpochQuai, lastMaxEpochAleo)

	// 3. 定时器
	ticker := time.NewTicker(time.Duration(*checkMins) * time.Minute)
	defer ticker.Stop()

	log.Printf("Starting check loop with interval = %d minute(s)\n", *checkMins)
	for {
		select {
		case <-ticker.C:
			CheckAndUpdate()
		}
	}
}

// initDB 初始化数据库连接并测试连通性
func initDB(dsn, dbType string) (*sql.DB, error) {
	db, err := sql.Open(dbType, dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open failed: %w", err)
	}
	// 测试连接
	if err = db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("db.Ping failed: %w", err)
	}
	return db, nil
}

// loadLastEpochFromTable 从 MySQL 的 shares_epoch_counts 表中，查找某个 chain 已有的最大 epoch
func loadLastEpochFromTable(db *sql.DB, chain string) int64 {
	var epoch sql.NullInt64
	err := db.QueryRow(`
        SELECT MAX(epoch) 
        FROM shares_epoch_counts
        WHERE chain = ?
    `, chain).Scan(&epoch)
	if err != nil {
		log.Printf("Failed to load last epoch for chain=%s: %v\n", chain, err)
		return 0
	}
	if epoch.Valid {
		return epoch.Int64
	}
	return 0
}

// CheckAndUpdate 同时检查 QuaiDB 和 AleoDB
func CheckAndUpdate() {
	// Quai: 表名是 shares
	if err := checkEpochChanges("quai", "shares", quaiDB, &lastMaxEpochQuai); err != nil {
		log.Printf("checkEpochChanges(quai) error: %v\n", err)
	}

	// Aleo: 表名是 user_shares
	if err := checkEpochChanges("aleo", "user_shares", aleoDB, &lastMaxEpochAleo); err != nil {
		log.Printf("checkEpochChanges(aleo) error: %v\n", err)
	}
}

// checkEpochChanges(方案B)：只写 [lastMaxEpoch+1, newMaxEpoch-1]
func checkEpochChanges(chainName, tableName string, sourceDB *sql.DB, lastMaxEpoch *int64) error {
	// 1. 查询源数据库中最新 epoch_number
	var newMaxEpoch sql.NullInt64
	query := fmt.Sprintf("SELECT MAX(epoch_number) FROM %s", tableName)
	if err := sourceDB.QueryRow(query).Scan(&newMaxEpoch); err != nil {
		return fmt.Errorf("queryRow failed: %w", err)
	}
	if !newMaxEpoch.Valid {
		log.Printf("[%s] No epoch found in %s\n", chainName, tableName)
		return nil
	}
	curMax := newMaxEpoch.Int64

	// 如果当前最大 epoch <= lastMaxEpoch，说明无新 epoch
	if curMax <= *lastMaxEpoch {
		log.Printf("[%s] No new epoch. lastMaxEpoch=%d, newMaxEpoch=%d\n",
			chainName, *lastMaxEpoch, curMax)
		return nil
	}

	// 2. 写入区间 [lastMaxEpoch+1, curMax-1]
	start := *lastMaxEpoch + 1
	end := curMax - 1

	if start <= end {
		if err := insertEpochRange(chainName, tableName, sourceDB, opsDB, start, end); err != nil {
			return fmt.Errorf("insertEpochRange [%d, %d] error: %w", start, end, err)
		}
		// 写完后，把 lastMaxEpoch 更新到 end
		*lastMaxEpoch = end
		log.Printf("[%s] Updated lastMaxEpoch to %d\n", chainName, *lastMaxEpoch)
	} else {
		// 如果 start > end，说明 just +1
		log.Printf("[%s] Possibly only one new epoch? lastMaxEpoch=%d, newMaxEpoch=%d\n",
			chainName, *lastMaxEpoch, curMax)
	}

	return nil
}

// insertEpochRange 将 [startEpoch, endEpoch] 区间的 epoch_number & share_count 插入到 MySQL
func insertEpochRange(chainName, tableName string, sourceDB, opsDB *sql.DB, startEpoch, endEpoch int64) error {
	// 1. 在源库查询
	query := fmt.Sprintf(`
        WITH epochs AS (
    	SELECT generate_series($1::integer, $2::integer) AS e  -- 生成从 startEpoch 到 endEpoch 的所有整数
		)	
		SELECT e AS epoch_number,
        COALESCE(t.cnt, 0) AS share_count
		FROM epochs
		LEFT JOIN (
    	SELECT epoch_number, COUNT(*) AS cnt
    	FROM %s
    	WHERE epoch_number BETWEEN $1 AND $2
    	GROUP BY epoch_number
		) AS t
  		ON epochs.e = t.epoch_number
		ORDER BY e;
		`, tableName)

	rows, err := sourceDB.Query(query, startEpoch, endEpoch)
	if err != nil {
		return fmt.Errorf("sourceDB.Query failed: %w", err)
	}
	defer rows.Close()

	// 2. 写 MySQL
	// 确保 shares_epoch_counts 上有 UNIQUE KEY (chain, epoch)
	insertSQL := `
        INSERT IGNORE INTO shares_epoch_counts (chain, epoch, share_count, created_at)
        VALUES (?, ?, ?, NOW())
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
		var epochNumber, shareCount int64
		if err := rows.Scan(&epochNumber, &shareCount); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("rows.Scan failed: %w", err)
		}

		if _, err = stmt.Exec(chainName, epochNumber, shareCount); err != nil {
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

	log.Printf("[%s] Inserted %d record(s) into shares_epoch_counts, range=[%d,%d]\n",
		chainName, countRecords, startEpoch, endEpoch)
	return nil
}
