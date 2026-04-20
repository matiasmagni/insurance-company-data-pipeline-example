package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/lib/pq"
)

const (
	DBHost     = "localhost"
	DBPort     = 5432
	DBUser     = "insurance_user"
	DBPassword = "insurance_pass"
	DBName     = "insurance_db"
)

type Customer struct {
	CustomerID   int
	FirstName    string
	LastName     string
	Email        string
	PhoneNumber  string
	DateOfBirth  time.Time
	Address      string
	City         string
	State        string
	ZipCode      string
	Country      string
	CreditScore  int
	AnnualIncome float64
	Occupation   string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type Claim struct {
	ClaimID         int
	CustomerID      int
	ClaimDate       time.Time
	ClaimType       string
	ClaimStatus     string
	ClaimAmount     float64
	ClaimPaidAmount float64
	VehicleType     string
	AgentID         int
	AgentName       string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

var firstNames = []string{"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"}
var lastNames = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"}
var occupations = []string{"Software Engineer", "Doctor", "Lawyer", "Teacher", "Accountant", "Nurse", "Manager", "Consultant", "Chef", "Architect", "Designer", "Writer", "Analyst", "Sales Representative", "Project Manager"}
var streetNames = []string{"Main St", "Oak Ave", "Pine Rd", "Maple Dr", "Cedar Ln", "Elm St", "Park Ave", "Lake Dr", "River Rd", "Hill St"}
var cities = []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
var states = []string{"NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"}
var claimTypes = []string{"Auto", "Home", "Life", "Health", "Property"}
var claimStatuses = []string{"Open", "Closed", "Pending", "Denied", "Investigation"}
var vehicleTypes = []string{"Sedan", "SUV", "Truck", "Motorcycle", "Van", "Coupe", "Wagon"}

func randomElement[T any](slice []T) T {
	return slice[rand.Intn(len(slice))]
}

func randomCreditScore() int {
	return 500 + rand.Intn(300)
}

func randomAnnualIncome() float64 {
	return 30000 + rand.Float64()*170000
}

func randomDate(start, end time.Time) time.Time {
	return time.Unix(rand.Int64n(end.Unix()-start.Unix())+start.Unix(), 0)
}

func generateCustomers(db *sql.DB, count int) error {
	log.Printf("Generating %d customers...", count)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO customers 
		(first_name, last_name, email, phone_number, date_of_birth, address, city, state, zip_code, country, credit_score, annual_income, occupation, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now()
	dobStart := now.AddDate(-80, 0, 0)
	dobEnd := now.AddDate(-18, 0, 0)
	createdStart := now.AddDate(-2, 0, 0)

	for i := 0; i < count; i++ {
		firstName := randomElement(firstNames)
		lastName := randomElement(lastNames)
		email := fmt.Sprintf("%s.%s%d@example.com", firstName, lastName, i)
		phoneNumber := fmt.Sprintf("(%d) %d-%d", rand.Intn(900)+100, rand.Intn(900)+100, rand.Intn(10000))
		dateOfBirth := randomDate(dobStart, dobEnd)
		address := fmt.Sprintf("%d %s", rand.Intn(9999)+1, randomElement(streetNames))
		city := randomElement(cities)
		state := randomElement(states)
		zipCode := fmt.Sprintf("%05d", rand.Intn(90000)+10000)
		creditScore := randomCreditScore()
		annualIncome := randomAnnualIncome()
		occupation := randomElement(occupations)
		createdAt := randomDate(createdStart, now)

		_, err := stmt.Exec(
			firstName, lastName, email, phoneNumber, dateOfBirth,
			address, city, state, zipCode, "USA", creditScore, annualIncome,
			occupation, createdAt, createdAt,
		)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Successfully generated %d customers", count)
	return nil
}

func generateClaims(db *sql.DB, customerCount, claimsPerCustomer int) error {
	totalClaims := customerCount * claimsPerCustomer
	log.Printf("Generating %d claims...", totalClaims)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO claims 
		(customer_id, claim_date, claim_type, claim_status, claim_amount, claim_paid_amount, vehicle_type, agent_id, agent_name, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now()
	claimStart := now.AddDate(-5, 0, 0)
	agentNames := []string{"Agent Smith", "Agent Johnson", "Agent Williams", "Agent Brown", "Agent Jones", "Agent Davis", "Agent Miller", "Agent Wilson"}

	for customerID := 1; customerID <= customerCount; customerID++ {
		for j := 0; j < claimsPerCustomer; j++ {
			claimType := randomElement(claimTypes)
			claimStatus := randomElement(claimStatuses)
			claimAmount := 1000 + rand.Float64()*49000
			claimPaidAmount := 0.0

			if claimStatus == "Closed" || claimStatus == "Denied" {
				claimPaidAmount = claimAmount * rand.Float64()
			}

			vehicleType := randomElement(vehicleTypes)
			agentID := rand.Intn(8) + 1
			agentName := agentNames[agentID-1]
			claimDate := randomDate(claimStart, now)
			createdAt := randomDate(claimStart, now)

			_, err := stmt.Exec(
				customerID, claimDate, claimType, claimStatus, claimAmount,
				claimPaidAmount, vehicleType, agentID, agentName,
				createdAt, createdAt,
			)
			if err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Successfully generated %d claims", totalClaims)
	return nil
}

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

	rand.Seed(time.Now().UnixNano())

	customerCount := 1000
	claimsPerCustomer := 5

	if err := generateCustomers(db, customerCount); err != nil {
		log.Fatalf("Failed to generate customers: %v", err)
	}

	if err := generateClaims(db, customerCount, claimsPerCustomer); err != nil {
		log.Fatalf("Failed to generate claims: %v", err)
	}

	log.Println("Data generation completed successfully!")
	log.Printf("Generated %d customers and %d claims", customerCount, customerCount*claimsPerCustomer)
}
