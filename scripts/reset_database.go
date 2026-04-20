package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

const (
	DBHost     = "localhost"
	DBPort     = 5435
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

	if err := initializeSchema(db); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}

	log.Println("Database reset and schema initialized successfully!")
}

func initializeSchema(db *sql.DB) error {
	log.Println("Dropping existing tables...")
	_, err := db.Exec(`
		DROP TABLE IF EXISTS claims CASCADE;
		DROP TABLE IF EXISTS customers CASCADE;
	`)
	if err != nil {
		return err
	}

	log.Println("Creating customers table...")
	_, err = db.Exec(`
		CREATE TABLE customers (
			customer_id SERIAL PRIMARY KEY,
			first_name VARCHAR(100),
			last_name VARCHAR(100),
			email VARCHAR(255) UNIQUE,
			phone_number VARCHAR(20),
			date_of_birth DATE,
			address VARCHAR(255),
			city VARCHAR(100),
			state VARCHAR(2),
			zip_code VARCHAR(10),
			country VARCHAR(100) DEFAULT 'USA',
			credit_score INTEGER,
			annual_income DECIMAL(12,2),
			occupation VARCHAR(100),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		return err
	}

	log.Println("Creating claims table...")
	_, err = db.Exec(`
		CREATE TABLE claims (
			claim_id SERIAL PRIMARY KEY,
			customer_id INTEGER REFERENCES customers(customer_id),
			claim_date DATE,
			claim_type VARCHAR(50),
			claim_status VARCHAR(20),
			claim_amount DECIMAL(12,2),
			claim_paid_amount DECIMAL(12,2),
			vehicle_type VARCHAR(20),
			agent_id INTEGER,
			agent_name VARCHAR(100),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	return err
}
