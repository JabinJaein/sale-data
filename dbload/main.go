package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github/jabin/dbload/svc"
)

var (
	db      *sql.DB
	csvFile = "data/sales_data.csv"
)

func main() {
	connStr := "host=localhost port=5432 user=jabin.jaein dbname=sales_data sslmode=disable"

	var err error
	db, err = sql.Open("postgres", connStr) // <--- Use '=' here, NOT ':=' to avoid shadowing
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Initial data load
	if err := svc.RefreshData(db, csvFile); err != nil {
		log.Fatalf("Initial data refresh failed: %v", err)
	}

	// HTTP Handlers
	http.HandleFunc("/refresh", handleRefresh)
	http.HandleFunc("/revenue/product", handleRevenueByProduct)
	http.HandleFunc("/revenue/category", handleRevenueByCategory)
	http.HandleFunc("/revenue/region", handleRevenueByRegion)
	http.HandleFunc("/revenue/total", handleTotalRevenue)

	log.Println("Starting server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// POST /refresh
func handleRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	go func() {
		if err := svc.RefreshData(db, csvFile); err != nil {
			log.Printf("Data refresh failed: %v", err)
		} else {
			log.Println("Data refresh succeeded")
		}
	}()
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Data refresh started\n"))
}

// GET /revenue/product?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
func handleRevenueByProduct(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	startDateStr := r.URL.Query().Get("start_date")
	endDateStr := r.URL.Query().Get("end_date")

	if startDateStr == "" || endDateStr == "" {
		http.Error(w, "Missing start_date or end_date query parameter", http.StatusBadRequest)
		return
	}

	startDate, err := time.Parse("2006-01-02", startDateStr)
	if err != nil {
		http.Error(w, "Invalid start_date format, use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	endDate, err := time.Parse("2006-01-02", endDateStr)
	if err != nil {
		http.Error(w, "Invalid end_date format, use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	results, err := svc.TotalRevenueByProduct(db, startDate, endDate)
	if err != nil {
		http.Error(w, "Error fetching revenue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// GET /revenue/category?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
func handleRevenueByCategory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	startDateStr := r.URL.Query().Get("start_date")
	endDateStr := r.URL.Query().Get("end_date")

	if startDateStr == "" || endDateStr == "" {
		http.Error(w, "Missing start_date or end_date query parameter", http.StatusBadRequest)
		return
	}

	startDate, err := time.Parse("2006-01-02", startDateStr)
	if err != nil {
		http.Error(w, "Invalid start_date format, use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	endDate, err := time.Parse("2006-01-02", endDateStr)
	if err != nil {
		http.Error(w, "Invalid end_date format, use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	results, err := svc.TotalRevenueByCategory(db, startDate, endDate)
	if err != nil {
		http.Error(w, "Error fetching revenue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// GET /revenue/region?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
func handleRevenueByRegion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	startDateStr := r.URL.Query().Get("start_date")
	endDateStr := r.URL.Query().Get("end_date")

	if startDateStr == "" || endDateStr == "" {
		http.Error(w, "Missing start_date or end_date query parameter", http.StatusBadRequest)
		return
	}

	startDate, err := time.Parse("2006-01-02", startDateStr)
	if err != nil {
		http.Error(w, "Invalid start_date format, use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	endDate, err := time.Parse("2006-01-02", endDateStr)
	if err != nil {
		http.Error(w, "Invalid end_date format, use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	results, err := svc.TotalRevenueByRegion(db, startDate, endDate)
	if err != nil {
		http.Error(w, "Error fetching revenue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// GET /revenue/total?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
func handleTotalRevenue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	startDateStr := r.URL.Query().Get("start_date")
	endDateStr := r.URL.Query().Get("end_date")

	if startDateStr == "" || endDateStr == "" {
		http.Error(w, "Missing start_date or end_date query parameter", http.StatusBadRequest)
		return
	}

	startDate, err := time.Parse("2006-01-02", startDateStr)
	if err != nil {
		http.Error(w, "Invalid start_date format, use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	endDate, err := time.Parse("2006-01-02", endDateStr)
	if err != nil {
		http.Error(w, "Invalid end_date format, use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	total, err := svc.TotalRevenue(db, startDate, endDate)
	if err != nil {
		http.Error(w, "Error fetching total revenue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]float64{"total_revenue": total})
}
