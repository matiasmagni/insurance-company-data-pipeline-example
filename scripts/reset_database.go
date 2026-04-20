package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
)

const (
	DBHost     = "localhost"
	DBPort     = 5432
	DBUser     = "insurance_user"
	DBPassword = "insurance_pass"
	DBName     = "insurance_db"
)

func main() {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		DBHost, DBPort, DBUser, DBPassword, DBName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Println("Connected to PostgreSQL")

	if err := truncateTables(db); err != nil {
		log.Fatalf("Failed to truncate tables: %v", err)
	}

	log.Println("Database cleaned successfully!")

	if len(os.Args) > 1 && os.Args[1] == "--regenerate" {
		log.Println("Regenerating data with synthetic_data_generator.go...")
		execCmd()
	}
}

func truncateTables(db *sql.DB) error {
	tables := []string{"claims", "customers"}

	for _, table := range tables {
		log.Printf("Truncating table: %s", table)

		_, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", table))
		if err != nil {
			return fmt.Errorf("failed to truncate %s: %w", table, err)
		}
		log.Printf("Truncated: %s", table)
	}

	return nil
}

func execCmd() error {
	_, err := os.Stat("synthetic_data_generator.go")
	if err != nil {
		log.Printf("Warning: synthetic_data_generator.go not found in current directory")
		log.Println("Please run: go run synthetic_data_generator.go")
		return err
	}
	return nil
}
