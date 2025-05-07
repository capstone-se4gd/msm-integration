import sqlite3
import jwt
import datetime
import os
import uuid
import hashlib
from functools import wraps
from flask import request, jsonify, g
from flask_restx import abort

# Secret key for JWT - in production, use environment variables
SECRET_KEY = os.environ.get('JWT_SECRET_KEY', 'your-secret-key-for-dev')
# Token expiration time (in minutes)
TOKEN_EXPIRATION = 60  # 1 hour
# Database file path
DATABASE = 'database.db'

def get_db():
    """Get database connection for the current request."""
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row  # This enables column access by name: row['column_name']
    return db

def close_db(e=None):
    """Close database connection at the end of request."""
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def query_db(query, args=(), one=False):
    """Query the database and return the results as a list of dictionaries."""
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (dict(rv[0]) if rv else None) if one else [dict(row) for row in rv]

def execute_db(query, args=(), commit=True):
    """Execute a database query and optionally commit changes."""
    db = get_db()
    cur = db.execute(query, args)
    if commit:
        db.commit()
    last_id = cur.lastrowid
    cur.close()
    return last_id

def hash_password(password):
    """Create a SHA-256 hash of the password."""
    salt = os.environ.get('PASSWORD_SALT', 'default-salt-for-dev')
    return hashlib.sha256((password + salt).encode()).hexdigest()

def verify_password(stored_password, provided_password):
    """Verify a stored password against the provided password."""
    return stored_password == hash_password(provided_password)

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        auth_header = request.headers.get('Authorization')
        
        # Extract token from header
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
        
        if not token:
            abort(401, 'Token is missing')
        
        try:
            # Decode the token
            data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            user_id = data['user_id']
            
            # Get the user from database
            current_user = query_db('SELECT * FROM users WHERE id = ?', [user_id], one=True)
            
            if not current_user:
                abort(401, 'User not found')
                
        except jwt.ExpiredSignatureError:
            abort(401, 'Token has expired')
        except jwt.InvalidTokenError:
            abort(401, 'Invalid token')
        except Exception as e:
            abort(500, f'Authentication error: {str(e)}')
        
        # Pass the current user to the route
        return f(*args, **kwargs, current_user=current_user)
    
    return decorated

# Authentication routes
def register_auth_routes(app):
    
    # Register teardown function
    app.teardown_appcontext(close_db)
    
    @app.route('/auth/register', methods=['POST'])
    def register():
        data = request.get_json()
        
        if not data:
            return jsonify({'message': 'No input data provided', 'success': False}), 400
            
        # Basic validation
        required_fields = ['username', 'email', 'password']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'message': f'Missing required field: {field}', 
                    'success': False
                }), 400
        
        # Check if user already exists
        if query_db('SELECT 1 FROM users WHERE username = ?', [data['username']], one=True):
            return jsonify({'message': 'Username already exists!', 'success': False}), 409
            
        if query_db('SELECT 1 FROM users WHERE email = ?', [data['email']], one=True):
            return jsonify({'message': 'Email already registered!', 'success': False}), 409
        
        # Create new user
        user_id = str(uuid.uuid4())
        hashed_password = hash_password(data['password'])
        role = data.get('role', 'user')  # Default role is 'user'
        
        execute_db(
            'INSERT INTO users (id, username, email, password, role, created_at) VALUES (?, ?, ?, ?, ?, ?)',
            [user_id, data['username'], data['email'], hashed_password, role, datetime.datetime.utcnow().isoformat()]
        )
        
        return jsonify({
            'message': 'User registered successfully!',
            'success': True,
            'user_id': user_id
        }), 201

    @app.route('/auth/login', methods=['POST'])
    def login():
        auth = request.get_json()
        
        if not auth or not auth.get('email') or not auth.get('password'):
            return jsonify({'message': 'Missing login credentials!', 'authenticated': False}), 400
        
        username = auth.get('email')
        password = auth.get('password')
        
        # Find user in database
        user = query_db('SELECT * FROM users WHERE email = ?', [username], one=True)
        
        if not user:
            return jsonify({'message': 'User not found!', 'authenticated': False}), 401
            
        if verify_password(user['password'], password):
            # Generate JWT token
            token = jwt.encode({
                'user_id': user['id'],
                'username': user['username'],
                'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=TOKEN_EXPIRATION)
            }, SECRET_KEY, algorithm="HS256")
            
            return jsonify({
                'message': 'Login successful',
                'authenticated': True,
                'token': token,
                'user': {
                    'id': user['id'],
                    'username': user['username'],
                    'email': user['email'],
                    'role': user['role']
                },
                'expires_in': TOKEN_EXPIRATION * 60  # in seconds
            })
        
        return jsonify({'message': 'Invalid credentials!', 'authenticated': False}), 401

    @app.route('/auth/validate-token', methods=['GET'])
    @token_required
    def validate_token(current_user):
        return jsonify({
            'message': 'Token is valid',
            'authenticated': True,
            'user': {
                'id': current_user['id'], 
                'username': current_user['username'],
                'email': current_user['email'],
                'role': current_user['role']
            }
        })

    return app