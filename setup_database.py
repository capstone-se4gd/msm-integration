import sqlite3
import os
import hashlib
import uuid
import datetime

# Database file path
DATABASE = 'database.db'

def hash_password(password):
    """Create a SHA-256 hash of the password."""
    salt = os.environ.get('PASSWORD_SALT', 'default-salt-for-dev')
    return hashlib.sha256((password + salt).encode()).hexdigest()

def setup_database():
    """Set up the database with all required tables."""
    # Check if database file exists and remove it if it does
    if os.path.exists(DATABASE):
        print(f"Removing existing database: {DATABASE}")
        os.remove(DATABASE)
    
    print(f"Creating new database: {DATABASE}")
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute('''
    CREATE TABLE users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'user',
        created_at TEXT NOT NULL
    )
    ''')
    
    # Create products table
    cursor.execute('''
    CREATE TABLE products (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        user_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )
    ''')
    
    # Create batches table
    cursor.execute('''
    CREATE TABLE batches (
        id TEXT PRIMARY KEY,
        product_id TEXT NOT NULL,
        information_url TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY (product_id) REFERENCES products (id)
    )
    ''')

    # Create invoices table
    cursor.execute('''
    CREATE TABLE invoices (
        id TEXT PRIMARY KEY,
        batch_id TEXT NOT NULL,
        facility TEXT NOT NULL,
        organizational_unit TEXT NOT NULL,
        supplier_url TEXT NOT NULL,
        sub_category TEXT NOT NULL,
        invoice_number TEXT,
        invoice_date TEXT,
        emissions_are_per_unit TEXT,
        quantity_needed_per_unit TEXT,
        units_bought REAL,
        total_amount REAL,
        currency TEXT,
        transaction_start_date TEXT,
        transaction_end_date TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (batch_id) REFERENCES batches (id)
    )
    ''')
    
    # Create transactions table for storing invoice processing results
    cursor.execute('''
    CREATE TABLE transactions (
        id TEXT PRIMARY KEY,
        result TEXT NOT NULL,  -- JSON string or error message
        created_at TEXT NOT NULL,
        deletion_scheduled_at TEXT
    )
    ''')
    
    # Create an admin user
    admin_id = str(uuid.uuid4())
    admin_password = hash_password('admin123')  # Change this in production!
    created_at = datetime.datetime.utcnow().isoformat()
    
    cursor.execute('''
    INSERT INTO users (id, username, email, password, role, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (admin_id, 'admin', 'admin@example.com', admin_password, 'admin', created_at))
    
    conn.commit()
    
    conn.close()
    
    print("Database setup complete!")
    print(f"Admin user created with email: 'admin@example.com' and password: 'admin123'")

if __name__ == "__main__":
    setup_database()