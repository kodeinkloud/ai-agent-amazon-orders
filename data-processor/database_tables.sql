-- Drop all tables (run this before creating new schema if needed)
-- First drop all tables with dependencies
-- DROP TABLE IF EXISTS 
--     cart_items,
--     digital_borrows,
--     digital_order_payments,
--     digital_order_items,
--     digital_orders,
--     refunds,
--     returns,
--     order_items,
--     orders,
--     payment_methods,
--     addresses,
--     products
-- CASCADE;

-- -- Drop triggers
-- DROP TRIGGER IF EXISTS update_products_updated_at ON products;
-- DROP TRIGGER IF EXISTS update_orders_updated_at ON orders;
-- DROP TRIGGER IF EXISTS update_digital_orders_updated_at ON digital_orders;

-- -- Drop trigger function
-- DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;

-- -- Drop custom ENUM types
-- DROP TYPE IF EXISTS order_status_enum CASCADE;
-- DROP TYPE IF EXISTS shipment_status_enum CASCADE;
-- DROP TYPE IF EXISTS return_status_enum CASCADE;

-- -- Commit the transaction
-- COMMIT;

-- Create custom types
CREATE TYPE order_status_enum AS ENUM ('Open', 'Closed', 'Cancelled');
CREATE TYPE shipment_status_enum AS ENUM ('Pending', 'Shipped', 'Delivered');
CREATE TYPE return_status_enum AS ENUM ('Pending', 'Completed', 'Rejected');

-- Products table
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    asin VARCHAR(20) UNIQUE NOT NULL,
    product_name TEXT NOT NULL,
    product_condition VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Addresses table to normalize shipping/billing addresses
CREATE TABLE addresses (
    id SERIAL PRIMARY KEY,
    address_line1 TEXT NOT NULL,
    address_line2 TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add unique constraint to addresses table
ALTER TABLE addresses ADD CONSTRAINT unique_address
    UNIQUE (address_line1, city, state, postal_code);

-- Payment methods table
CREATE TABLE payment_methods (
    id SERIAL PRIMARY KEY,
    payment_type VARCHAR(50) NOT NULL,
    payment_instrument VARCHAR(100),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Main orders table (retail orders)
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    website VARCHAR(50) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    currency VARCHAR(10) NOT NULL,
    order_status order_status_enum,
    shipping_address_id INTEGER REFERENCES addresses(id),
    billing_address_id INTEGER REFERENCES addresses(id),
    payment_method_id INTEGER REFERENCES payment_methods(id),
    total_owed DECIMAL(10,2),
    shipping_charge DECIMAL(10,2),
    total_discounts DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order items linking orders and products
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL,
    unit_price_tax DECIMAL(10,2),
    shipment_status shipment_status_enum,
    ship_date TIMESTAMP,
    shipping_option VARCHAR(50),
    carrier_tracking VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Returns table
CREATE TABLE returns (
    id SERIAL PRIMARY KEY,
    return_authorization_id VARCHAR(50) UNIQUE NOT NULL,
    order_item_id INTEGER REFERENCES order_items(id),
    return_date TIMESTAMP NOT NULL,
    return_status return_status_enum NOT NULL,
    return_reason TEXT,
    tracking_id VARCHAR(50),
    return_ship_option VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Refunds table
CREATE TABLE refunds (
    id SERIAL PRIMARY KEY,
    return_id INTEGER REFERENCES returns(id),
    reversal_id VARCHAR(50) UNIQUE NOT NULL,
    amount_refunded DECIMAL(10,2) NOT NULL,
    refund_date TIMESTAMP NOT NULL,
    status VARCHAR(20),
    currency VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Digital orders table
CREATE TABLE digital_orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    delivery_packet_id VARCHAR(100),
    marketplace VARCHAR(50),
    order_date TIMESTAMP NOT NULL,
    fulfilled_date TIMESTAMP,
    is_fulfilled BOOLEAN DEFAULT false,
    currency VARCHAR(10),
    total_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Digital order items
CREATE TABLE digital_order_items (
    id SERIAL PRIMARY KEY,
    digital_order_id INTEGER REFERENCES digital_orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER DEFAULT 1,
    unit_price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Digital order payments
CREATE TABLE digital_order_payments (
    id SERIAL PRIMARY KEY,
    digital_order_id INTEGER REFERENCES digital_orders(id),
    transaction_amount DECIMAL(10,2),
    currency VARCHAR(10),
    claim_code VARCHAR(100),
    monetary_component_type VARCHAR(50),
    offer_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Digital borrows table
CREATE TABLE digital_borrows (
    id SERIAL PRIMARY KEY,
    asin VARCHAR(20) REFERENCES products(asin),
    loan_creation_date TIMESTAMP NOT NULL,
    loan_acceptance_date TIMESTAMP,
    loan_status VARCHAR(20),
    loan_program VARCHAR(50),
    end_date TIMESTAMP,
    delivery_device_name VARCHAR(100),
    content_type VARCHAR(20),
    is_first_content_loan BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cart items
CREATE TABLE cart_items (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id),
    date_added TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    cart_list VARCHAR(20),
    one_click_buyable BOOLEAN DEFAULT false,
    is_gift_wrapped BOOLEAN DEFAULT false,
    source VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_products_asin ON products(asin);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(order_status);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_returns_date ON returns(return_date);
CREATE INDEX idx_refunds_date ON refunds(refund_date);
CREATE INDEX idx_digital_orders_order_id ON digital_orders(order_id);
CREATE INDEX idx_digital_orders_date ON digital_orders(order_date);
CREATE INDEX idx_digital_borrows_asin ON digital_borrows(asin);
CREATE INDEX idx_digital_borrows_dates ON digital_borrows(loan_creation_date, end_date);
CREATE INDEX idx_cart_items_product ON cart_items(product_id);
CREATE INDEX idx_cart_items_date ON cart_items(date_added);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at columns
CREATE TRIGGER update_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_digital_orders_updated_at
    BEFORE UPDATE ON digital_orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Add comments to tables
COMMENT ON TABLE products IS 'Stores all products from both retail and digital orders';
COMMENT ON TABLE orders IS 'Stores retail order header information';
COMMENT ON TABLE order_items IS 'Stores individual items within retail orders';
COMMENT ON TABLE digital_orders IS 'Stores digital order header information';
COMMENT ON TABLE digital_order_items IS 'Stores individual items within digital orders';
COMMENT ON TABLE returns IS 'Stores return information for retail orders';
COMMENT ON TABLE refunds IS 'Stores refund information for returns';
COMMENT ON TABLE digital_borrows IS 'Stores digital content borrowing information';
COMMENT ON TABLE cart_items IS 'Stores shopping cart items';



-- -- First, disable foreign key checks to avoid dependency issues
-- SET session_replication_role = 'replica';

-- -- Drop all tables with CASCADE to handle dependencies
-- DROP TABLE IF EXISTS 
--     cart_items,
--     digital_borrows,
--     digital_order_payments,
--     digital_order_items,
--     digital_orders,
--     refunds,
--     returns,
--     order_items,
--     orders,
--     payment_methods,
--     addresses,
--     products
-- CASCADE;

-- -- Drop all custom types
-- DROP TYPE IF EXISTS 
--     order_status_enum,
--     shipment_status_enum,
--     return_status_enum
-- CASCADE;

-- -- Drop all functions and triggers
-- DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;

-- -- Reset foreign key checks
-- SET session_replication_role = 'origin';