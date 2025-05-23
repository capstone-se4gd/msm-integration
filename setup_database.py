import os
import hashlib
import uuid
import datetime
import pymysql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def hash_password(password):
    """Create a SHA-256 hash of the password."""
    salt = os.environ.get('PASSWORD_SALT', 'default-salt-for-dev')
    return hashlib.sha256((password + salt).encode()).hexdigest()

def setup_database():
    """Set up the MySQL database with all required tables."""
    # Get database configuration from environment variables
    db_host = os.environ.get('DB_HOST')
    db_port = int(os.environ.get('DB_PORT', 3306))
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_name = os.environ.get('DB_NAME')
    
    try:
        # Connect to the MySQL server without specifying database
        conn = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password
        )
        
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.execute(f"USE `{db_name}`")
        
        # Create users table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id VARCHAR(36) PRIMARY KEY,
            username VARCHAR(255) UNIQUE NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            password VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL DEFAULT 'user',
            created_at DATETIME NOT NULL
        ) ENGINE=InnoDB
        ''')
        
        # Create products table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id VARCHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            user_id VARCHAR(36) NOT NULL,
            created_at DATETIME NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        ) ENGINE=InnoDB
        ''')
        
        # Create batches table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS batches (
            id VARCHAR(36) PRIMARY KEY,
            product_id VARCHAR(36) NOT NULL,
            information_url VARCHAR(2048) NOT NULL,
            created_at DATETIME NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products (id)
        ) ENGINE=InnoDB
        ''')

        # Create invoices table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS invoices (
            id VARCHAR(36) PRIMARY KEY,
            batch_id VARCHAR(36) NOT NULL,
            facility VARCHAR(255) NOT NULL,
            organizational_unit VARCHAR(255) NOT NULL,
            supplier_url VARCHAR(2048) NOT NULL,
            sub_category VARCHAR(255) NOT NULL,
            invoice_number VARCHAR(255),
            invoice_date DATE,
            emissions_are_per_unit VARCHAR(255),
            quantity_needed_per_unit VARCHAR(255),
            units_bought FLOAT,
            total_amount FLOAT,
            currency VARCHAR(50),
            transaction_start_date DATE,
            transaction_end_date DATE,
            created_at DATETIME NOT NULL,
            FOREIGN KEY (batch_id) REFERENCES batches (id)
        ) ENGINE=InnoDB
        ''')
        
        # Create transactions table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id VARCHAR(36) PRIMARY KEY,
            result LONGTEXT NOT NULL,  -- JSON string or error message
            created_at DATETIME NOT NULL,
            deletion_scheduled_at DATETIME
        ) ENGINE=InnoDB
        ''')
        
        # Check if admin user already exists
        cursor.execute("SELECT id FROM users WHERE username = 'admin'")
        admin_exists = cursor.fetchone()
        
        if not admin_exists:
            # Create an admin user
            admin_id = str(uuid.uuid4())
            admin_password = hash_password('admin123')  # Change this in production!
            created_at = datetime.datetime.utcnow()
            
            cursor.execute('''
            INSERT INTO users (id, username, email, password, role, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ''', (admin_id, 'admin', 'admin@example.com', admin_password, 'admin', created_at))
            
            print(f"Admin user created with email: 'admin@example.com' and password: 'admin123'")
        else:
            print("Admin user already exists")
        
        conn.commit()
        conn.close()
        
        print("Database setup complete!")
        
    except Exception as e:
        print(f"Database setup failed: {e}")
        
if __name__ == "__main__":
    setup_database()