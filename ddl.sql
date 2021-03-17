-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    address VARCHAR(255),
    phone_number VARCHAR(255)
);

-- Create credit card table
CREATE TABLE IF NOT EXISTS credit_cards (
    id INT AUTO_INCREMENT PRIMARY KEY,
    provider VARCHAR(255),
    number VARCHAR(255),
    security_code VARCHAR(255),
    expiry_date VARCHAR(255),
    customer_id INT,
    FOREIGN KEY(customer_id) REFERENCES customers(id)
);