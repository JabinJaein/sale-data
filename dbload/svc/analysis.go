package svc

import (
	"database/sql"
	"fmt"
	"time"
)

type RevenueByProduct struct {
	ProductID    string  `json:"product_id"`
	ProductName  string  `json:"product_name"`
	TotalRevenue float64 `json:"total_revenue"`
}

func TotalRevenueByProduct(db *sql.DB, startDate, endDate time.Time) ([]RevenueByProduct, error) {
	query := `
	SELECT 
		p.product_id, p.product_name,
		SUM(oi.quantity_sold * oi.unit_price_at_sale - oi.discount_applied) AS total_revenue
	FROM order_items oi
	JOIN products p ON oi.product_id = p.product_id
	JOIN orders o ON oi.order_id = o.order_id
	WHERE o.order_date BETWEEN $1 AND $2
	GROUP BY p.product_id, p.product_name
	ORDER BY total_revenue DESC
	`
	rows, err := db.Query(query, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	results := []RevenueByProduct{}
	for rows.Next() {
		var r RevenueByProduct
		if err := rows.Scan(&r.ProductID, &r.ProductName, &r.TotalRevenue); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, nil
}

func TotalRevenueByCategory(db *sql.DB, startDate, endDate time.Time) (map[string]float64, error) {
	query := `
		SELECT p.category, 
		       SUM(oi.quantity_sold * oi.unit_price_at_sale - oi.discount_applied + o.shipping_cost) AS total_revenue
		FROM orders o
		JOIN order_items oi ON o.order_id = oi.order_id
		JOIN products p ON oi.product_id = p.product_id
		WHERE o.order_date BETWEEN $1 AND $2
		GROUP BY p.category
		ORDER BY total_revenue DESC
	`

	rows, err := db.Query(query, startDate, endDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]float64)
	for rows.Next() {
		var category string
		var revenue float64
		if err := rows.Scan(&category, &revenue); err != nil {
			return nil, err
		}
		result[category] = revenue
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// TotalRevenueByRegion returns revenue grouped by region
func TotalRevenueByRegion(db *sql.DB, startDate, endDate time.Time) (map[string]float64, error) {
	query := `
		SELECT c.region, 
		       SUM(oi.quantity_sold * oi.unit_price_at_sale - oi.discount_applied + o.shipping_cost) AS total_revenue
		FROM orders o
		JOIN customers c ON o.customer_id = c.customer_id
		JOIN order_items oi ON o.order_id = oi.order_id
		WHERE o.order_date BETWEEN $1 AND $2
		GROUP BY c.region
		ORDER BY total_revenue DESC
	`

	rows, err := db.Query(query, startDate, endDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]float64)
	for rows.Next() {
		var region string
		var revenue float64
		if err := rows.Scan(&region, &revenue); err != nil {
			return nil, err
		}
		result[region] = revenue
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// TotalRevenue returns the overall revenue for the given date range
func TotalRevenue(db *sql.DB, startDate, endDate time.Time) (float64, error) {
	query := `
		SELECT SUM(oi.quantity_sold * oi.unit_price_at_sale - oi.discount_applied + o.shipping_cost) AS total_revenue
		FROM orders o
		JOIN order_items oi ON o.order_id = oi.order_id
		WHERE o.order_date BETWEEN $1 AND $2
	`

	var total float64
	err := db.QueryRow(query, startDate, endDate).Scan(&total)
	if err != nil {
		return 0, err
	}
	return total, nil
}
