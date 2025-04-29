# Flask Middleware with Authentication

This project implements a Flask middleware with authentication for processing invoices and managing product batches.

## Features

- User authentication with JWT tokens
- Process invoices from XML files asynchronously
- Create batches with sustainability metrics
- Fetch product information with associated batches
- SQLite3 database for data storage

## Getting Started

### Prerequisites

- Python 3.7+
- pip (Python package installer)

### Installation

1. Clone this repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set up the database:

```bash
python setup_database.py
```

4. Run the application:

```bash
python app.py
```

The server will start on `http://localhost:5000`.

## API Endpoints

### Authentication

- `POST /auth/register`: Register a new user
  - Request body: `{ "username": "user", "email": "user@example.com", "password": "password" }`
  - Response: `{ "message": "User registered successfully!", "success": true, "user_id": "..." }`

- `POST /auth/login`: Log in a user
  - Request body: `{ "username": "user", "password": "password" }`
  - Response: `{ "message": "Login successful", "authenticated": true, "token": "...", "user": {...}, "expires_in": 3600 }`

- `GET /auth/validate-token`: Validate a token
  - Headers: `Authorization: Bearer YOUR_TOKEN`
  - Response: `{ "message": "Token is valid", "authenticated": true, "user": {...} }`

### Invoice Processing

- `POST /process-invoices`: Process XML invoice files
  - Headers: `Authorization: Bearer YOUR_TOKEN`
  - Request: `multipart/form-data` with XML files in the `files` field
  - Response: `{ "message": "Processing started", "transaction_id": "..." }`

- `GET /transaction/<transaction_id>`: Get transaction results
  - Headers: `Authorization: Bearer YOUR_TOKEN`
  - Response: `{ "id": "...", "result": [...], "created_at": "...", "deletion_scheduled_at": "..." }`

### Product Management

- `POST /create-batch`: Create a new batch
  - Headers: `Authorization: Bearer YOUR_TOKEN`
  - Request body: `{ "productId": "..." (optional), "productName": "...", "xmlData": "...", "sustainabilityMetrics": {...} }`
  - Response: `{ "message": "Batch created successfully", "productId": "...", "batchId": "..." }`

- `GET /product/<product_id>`: Get product information with batches
  - Headers: `Authorization: Bearer YOUR_TOKEN`
  - Response: `{ "productId": "...", "productName": "...", "batches": [...] }`

- `GET /products`: Get all products with batches
  - Headers: `Authorization: Bearer YOUR_TOKEN`
  - Response: `[{ "productId": "...", "productName": "...", "batches": [...] }, ...]`

## Database Schema

### Users Table
- `id`: TEXT PRIMARY KEY
- `username`: TEXT UNIQUE NOT NULL
- `email`: TEXT UNIQUE NOT NULL
- `password`: TEXT NOT NULL
- `role`: TEXT NOT NULL DEFAULT 'user'
- `created_at`: TEXT NOT NULL

### Products Table
- `id`: TEXT PRIMARY KEY
- `name`: TEXT NOT NULL
- `created_at`: TEXT NOT NULL

### Batches Table
- `id`: TEXT PRIMARY KEY
- `product_id`: TEXT NOT NULL (Foreign key to products.id)
- `information_url`: TEXT NOT NULL
- `created_at`: TEXT NOT NULL

### Transactions Table
- `id`: TEXT PRIMARY KEY
- `result`: TEXT NOT NULL (JSON string)
- `created_at`: TEXT NOT NULL
- `deletion_scheduled_at`: TEXT

## Security Notes

- Passwords are hashed using SHA-256
- JWT tokens expire after 60 minutes
- Transaction results are automatically deleted after 24 hours
- For production use, change the secret keys and salt

## Testing

The database is set up with a default admin user:
- Username: `admin`
- Password: `admin123`

There's also a sample product and batch for testing purposes.
