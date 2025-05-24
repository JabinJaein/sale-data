-- Table to store product details

CREATE TABLE products (
                          product_id VARCHAR PRIMARY KEY,
                          product_name VARCHAR NOT NULL,
                          category VARCHAR,
                          description TEXT,
                          unit_price NUMERIC(10,2) NOT NULL
);

-- Table to store customer details
CREATE TABLE customers (
                           customer_id VARCHAR PRIMARY KEY,
                           customer_name VARCHAR NOT NULL,
                           email VARCHAR,
                           address TEXT,
                           region VARCHAR
);

-- Table to store order headers
CREATE TABLE orders (
                        order_id VARCHAR PRIMARY KEY,
                        customer_id VARCHAR REFERENCES customers(customer_id),
                        order_date DATE,
                        payment_method VARCHAR,
                        shipping_cost NUMERIC(10,2),
                        discount NUMERIC(5,2)
);

-- Table to store items in each order
CREATE TABLE order_items (
                             order_item_id SERIAL PRIMARY KEY,
                             order_id VARCHAR REFERENCES orders(order_id),
                             product_id VARCHAR REFERENCES products(product_id),
                             quantity_sold INTEGER,
                             unit_price_at_sale NUMERIC(10,2),
                             discount_applied NUMERIC(5,2)
);
