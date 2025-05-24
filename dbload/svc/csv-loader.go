package svc

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"github/jabin/dbload/datamodel"
	"log"
	"os"
	"strconv"
	"time"
)

func LoadCSVData(db *sql.DB, csvFile string) error {
	file, err := os.Open(csvFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV: %v", err)
	}

	// Prepare your insert statements with ON CONFLICT DO NOTHING for all unique constraints
	insertCustomer := `INSERT INTO customers (customer_id, customer_name, email, address, region) 
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT (customer_id) DO NOTHING`
	insertProduct := `INSERT INTO products (product_id, product_name, category, unit_price)
		VALUES ($1, $2, $3, $4) ON CONFLICT (product_id) DO NOTHING`
	insertOrder := `INSERT INTO orders (order_id, customer_id, order_date, payment_method, shipping_cost, discount) 
		VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (order_id) DO NOTHING`
	insertOrderItem := `INSERT INTO order_items (order_id, product_id, quantity_sold, unit_price_at_sale, discount_applied)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (order_id, product_id) DO NOTHING`

	for i, row := range records {
		if i == 0 {
			continue // skip header
		}

		r := datamodel.Record{
			OrderID:       row[0],
			ProductID:     row[1],
			CustomerID:    row[2],
			ProductName:   row[3],
			Category:      row[4],
			Region:        row[5],
			DateOfSale:    row[6],
			QuantitySold:  row[7],
			UnitPrice:     row[8],
			Discount:      row[9],
			ShippingCost:  row[10],
			PaymentMethod: row[11],
			CustomerName:  row[12],
			CustomerEmail: row[13],
			CustomerAddr:  row[14],
		}

		// Parse / transform
		orderDate, err := time.Parse("2006-01-02", r.DateOfSale)
		if err != nil {
			log.Printf("Row %d: invalid date %v", i, err)
			continue
		}

		unitPrice, err := strconv.ParseFloat(r.UnitPrice, 64)
		if err != nil {
			log.Printf("Row %d: invalid unit price %v", i, err)
			continue
		}

		shippingCost, err := strconv.ParseFloat(r.ShippingCost, 64)
		if err != nil {
			log.Printf("Row %d: invalid shipping cost %v", i, err)
			continue
		}

		discount, err := strconv.ParseFloat(r.Discount, 64)
		if err != nil {
			log.Printf("Row %d: invalid discount %v", i, err)
			continue
		}

		quantitySold, err := strconv.Atoi(r.QuantitySold)
		if err != nil {
			log.Printf("Row %d: invalid quantity %v", i, err)
			continue
		}

		// Start transaction per row
		tx, err := db.Begin()
		if err != nil {
			log.Printf("Row %d: failed to begin transaction: %v", i, err)
			continue
		}

		// Insert customer
		_, err = tx.Exec(insertCustomer, r.CustomerID, r.CustomerName, r.CustomerEmail, r.CustomerAddr, r.Region)
		if err != nil {
			tx.Rollback()
			log.Printf("Row %d: insert customer failed: %v", i, err)
			continue
		}

		// Insert product
		_, err = tx.Exec(insertProduct, r.ProductID, r.ProductName, r.Category, unitPrice)
		if err != nil {
			tx.Rollback()
			log.Printf("Row %d: insert product failed: %v", i, err)
			continue
		}

		// Insert order
		_, err = tx.Exec(insertOrder, r.OrderID, r.CustomerID, orderDate, r.PaymentMethod, shippingCost, discount)
		if err != nil {
			tx.Rollback()
			log.Printf("Row %d: insert order failed: %v", i, err)
			continue
		}

		// Insert order_item
		_, err = tx.Exec(insertOrderItem, r.OrderID, r.ProductID, quantitySold, unitPrice, discount)
		if err != nil {
			tx.Rollback()
			log.Printf("Row %d: insert order_item failed: %v", i, err)
			continue
		}

		err = tx.Commit()
		if err != nil {
			log.Printf("Row %d: commit failed: %v", i, err)
			continue
		}
	}

	log.Printf("CSV data loading completed.")
	return nil
}

// RefreshData truncates the related tables and reloads data from the CSV file
func RefreshData(db *sql.DB, csvFile string) error {
	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction failed: %v", err)
	}

	// Truncate tables in order to avoid FK conflicts
	tables := []string{"order_items", "orders", "products", "customers"}
	for _, table := range tables {
		_, err := tx.Exec(fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table))
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to truncate table %s: %v", table, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed during truncate: %v", err)
	}

	// Now reload CSV data (LoadCSVData uses its own transaction)
	err = LoadCSVData(db, csvFile)
	if err != nil {
		return fmt.Errorf("failed to load CSV data: %v", err)
	}

	log.Println("Data refresh completed successfully.")
	return nil
}
