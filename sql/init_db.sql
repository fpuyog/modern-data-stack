-- PostgreSQL Initialization Script
-- Create tables for local PostgreSQL database

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(200),
    country VARCHAR(100),
    city VARCHAR(100),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO users (username, email, full_name, country, city, registration_date)
VALUES
    ('jdoe', 'john.doe@example.com', 'John Doe', 'USA', 'New York', '2024-01-15'),
    ('asmith', 'alice.smith@example.com', 'Alice Smith', 'UK', 'London', '2024-02-20'),
    ('btanaka', 'bob.tanaka@example.com', 'Bob Tanaka', 'Japan', 'Tokyo', '2024-03-10'),
    ('msilva', 'maria.silva@example.com', 'Maria Silva', 'Brazil', 'São Paulo', '2024-04-05'),
    ('cgonzalez', 'carlos.gonzalez@example.com', 'Carlos González', 'Argentina', 'Buenos Aires', '2024-05-12')
ON CONFLICT (email) DO NOTHING;

-- Create index
CREATE INDEX IF NOT EXISTS idx_users_country ON users(country);
CREATE INDEX IF NOT EXISTS idx_users_registration_date ON users(registration_date);
