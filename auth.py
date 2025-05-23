import pymysql
import pymysql.cursors
import jwt
import datetime
import os
import uuid
import hashlib
from functools import wraps
from flask import request, jsonify, g
from flask_restx import abort, Resource, fields
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Secret key for JWT - in production, use environment variables
SECRET_KEY = os.environ.get('JWT_SECRET_KEY', 'your-secret-key-for-dev')
# Token expiration time (in minutes)
TOKEN_EXPIRATION = 60  # 1 hour

def get_db():
    """Get database connection for the current request."""
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = pymysql.connect(
            host=os.environ.get('DB_HOST'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            database=os.environ.get('DB_NAME'),
            port=int(os.environ.get('DB_PORT', 3306)),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor  # This enables column access by name
        )
    return db

def close_db(e=None):
    """Close database connection at the end of request."""
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def query_db(query, args=(), one=False):
    """Query the database and return the results as a list of dictionaries."""
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(query, args)
        rv = cur.fetchall()
    return rv[0] if rv and one else rv

def execute_db(query, args=(), commit=True):
    """Execute a database query and optionally commit changes."""
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(query, args)
        last_id = cur.lastrowid
        if commit:
            conn.commit()
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
        auth_header = request.headers.get('Authorization')

        if not auth_header:
            abort(401, 'Token is missing')

        # Remove 'Bearer ' prefix if it exists
        if auth_header.startswith('Bearer '):
            token = auth_header[7:]  # Skip 'Bearer ' part
        else:
            token = auth_header
            
        if not token:
            abort(401, 'Token is missing')
        
        try:
            # Decode the token
            data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            user_id = data['user_id']
            
            # Get the user from database
            current_user = query_db('SELECT * FROM users WHERE id = %s', [user_id], one=True)
            
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
def register_auth_routes(app, auth_ns):
    # Define models for the API
    login_model = auth_ns.model('Login', {
        'email': fields.String(required=True, description='User email'),
        'password': fields.String(required=True, description='User password')
    })
    
    register_model = auth_ns.model('Register', {
        'username': fields.String(required=True, description='Username'),
        'email': fields.String(required=True, description='User email'),
        'password': fields.String(required=True, description='User password'),
        'role': fields.String(required=False, description='User role', default='user')
    })
    
    user_model = auth_ns.model('User', {
        'id': fields.String(description='User ID'),
        'username': fields.String(description='Username'),
        'email': fields.String(description='User email'),
        'role': fields.String(description='User role')
    })
    
    token_response = auth_ns.model('TokenResponse', {
        'message': fields.String(description='Response message'),
        'authenticated': fields.Boolean(description='Authentication status'),
        'token': fields.String(description='JWT token'),
        'user': fields.Nested(user_model),
        'expires_in': fields.Integer(description='Token expiration time in seconds')
    })

    update_user_model = auth_ns.model('UpdateUser', {
        'username': fields.String(description='Username'),
        'email': fields.String(description='User email'),
        'password': fields.String(description='User password'),
        'role': fields.String(description='User role')
    })
    
    # Register teardown function
    app.teardown_appcontext(close_db)
    
    @auth_ns.route('/register')
    class Register(Resource):
        @auth_ns.expect(register_model)
        @auth_ns.doc(security='authorization')
        @auth_ns.response(201, 'User registered successfully', user_model)
        @auth_ns.response(400, 'Bad request')
        @auth_ns.response(403, 'Forbidden - Only admin can register new users')
        @auth_ns.response(409, 'Conflict - Username or email already exists')
        @auth_ns.response(500, 'Internal server error')
        @token_required
        def post(self, current_user):
            data = request.json

            # Check if user is admin
            if current_user['role'] != 'admin':
                return {'message': 'Only admin can register new users!', 'success': False}, 403

            # Basic validation
            required_fields = ['username', 'email', 'password']
            for field in required_fields:
                if field not in data:
                    return {'message': f'Missing required field: {field}', 'success': False}, 400
            
            # Check if user already exists
            if query_db('SELECT 1 FROM users WHERE username = %s', [data['username']], one=True):
                return {'message': 'Username already exists!', 'success': False}, 409
                
            if query_db('SELECT 1 FROM users WHERE email = %s', [data['email']], one=True):
                return {'message': 'Email already registered!', 'success': False}, 409
            
            # Create new user
            user_id = str(uuid.uuid4())
            hashed_password = hash_password(data['password'])
            role = data.get('role', 'user')  # Default role is 'user'
            
            execute_db(
                'INSERT INTO users (id, username, email, password, role, created_at) VALUES (%s, %s, %s, %s, %s, %s)',
                [user_id, data['username'], data['email'], hashed_password, role, datetime.datetime.utcnow()]
            )
            
            return {
                'message': 'User registered successfully!',
                'success': True,
                'user_id': user_id
            }, 201

    @auth_ns.route('/login')
    class Login(Resource):
        @auth_ns.expect(login_model)
        @auth_ns.response(200, 'Login successful', token_response)
        def post(self):
            auth = request.json
            
            if not auth or not auth.get('email') or not auth.get('password'):
                return {'message': 'Missing login credentials!', 'authenticated': False}, 400
            
            username = auth.get('email')
            password = auth.get('password')
            
            # Find user in database
            user = query_db('SELECT * FROM users WHERE email = %s', [username], one=True)
            
            if not user:
                return {'message': 'User not found!', 'authenticated': False}, 401
                
            if verify_password(user['password'], password):
                # Generate JWT token
                token = jwt.encode({
                    'user_id': user['id'],
                    'username': user['username'],
                    'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=TOKEN_EXPIRATION)
                }, SECRET_KEY, algorithm="HS256")
                
                return {
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
                }
            
            return {'message': 'Invalid credentials!', 'authenticated': False}, 401

    @auth_ns.route('/validate-token')
    class ValidateToken(Resource):
        @auth_ns.doc(security='authorization')
        @token_required
        def get(self, current_user):
            return {
                'message': 'Token is valid',
                'authenticated': True,
                'user': {
                    'id': current_user['id'], 
                    'username': current_user['username'],
                    'email': current_user['email'],
                    'role': current_user['role']
                }
            }
            
    @auth_ns.route('/users/<string:user_id>')
    class UserManagement(Resource):
        @auth_ns.doc(security='authorization')
        @auth_ns.expect(update_user_model)
        @auth_ns.response(200, 'User updated successfully', user_model)
        @auth_ns.response(403, 'Unauthorized to update this user')
        @auth_ns.response(404, 'User not found')
        @auth_ns.response(409, 'Username or email already taken')
        @token_required
        def put(self, user_id, current_user):
            """Update user information"""
            # Check permissions - users can only update their own info, admins can update any user
            if current_user['id'] != user_id and current_user['role'] != 'admin':
                return {'message': 'Unauthorized to update this user', 'success': False}, 403
                
            data = request.json
            if not data:
                return {'message': 'No update data provided', 'success': False}, 400
                
            # Verify user exists
            user = query_db('SELECT * FROM users WHERE id = %s', [user_id], one=True)
            if not user:
                return {'message': 'User not found', 'success': False}, 404
                
            # Prepare update fields
            updates = []
            params = []
            
            if 'username' in data and data['username']:
                # Check if new username already exists (if changing)
                if data['username'] != user['username']:
                    if query_db('SELECT 1 FROM users WHERE username = %s AND id != %s', 
                                [data['username'], user_id], one=True):
                        return {'message': 'Username already taken', 'success': False}, 409
                updates.append('username = %s')
                params.append(data['username'])
                
            if 'email' in data and data['email']:
                # Check if new email already exists (if changing)
                if data['email'] != user['email']:
                    if query_db('SELECT 1 FROM users WHERE email = %s AND id != %s', 
                                [data['email'], user_id], one=True):
                        return {'message': 'Email already registered', 'success': False}, 409
                updates.append('email = %s')
                params.append(data['email'])
                
            if 'password' in data and data['password']:
                updates.append('password = %s')
                params.append(hash_password(data['password']))
                
            if 'role' in data and data['role']:
                # Only admins can change roles
                if current_user['role'] != 'admin':
                    return {'message': 'Only admins can change user roles', 'success': False}, 403
                updates.append('role = %s')
                params.append(data['role'])
                
            if not updates:
                return {'message': 'No valid update fields provided', 'success': False}, 400
                
            # Execute update
            params.append(user_id)  # For the WHERE clause
            query = f"UPDATE users SET {', '.join(updates)} WHERE id = %s"
            execute_db(query, params)
            
            # Get updated user info
            updated_user = query_db('SELECT id, username, email, role FROM users WHERE id = %s', 
                                    [user_id], one=True)
            
            return {
                'message': 'User updated successfully',
                'success': True,
                'user': updated_user
            }, 200
            
        @auth_ns.doc(security='authorization')
        @auth_ns.response(200, 'User deleted successfully')
        @auth_ns.response(403, 'Unauthorized to delete this user')
        @auth_ns.response(404, 'User not found')
        @auth_ns.response(500, 'Internal server error')
        @token_required
        def delete(self, user_id, current_user):
            """Delete a user account"""
            # Check permissions - users can only delete their own account, admins can delete any user
            if current_user['id'] != user_id and current_user['role'] != 'admin':
                return {'message': 'Unauthorized to delete this user', 'success': False}, 403
                
            # Verify user exists
            user = query_db('SELECT * FROM users WHERE id = %s', [user_id], one=True)
            if not user:
                return {'message': 'User not found', 'success': False}, 404
                
            # Prevent deleting the last admin
            if user['role'] == 'admin':
                admin_count = query_db('SELECT COUNT(*) as count FROM users WHERE role = %s', 
                                      ['admin'], one=True)
                if admin_count['count'] <= 1:
                    return {'message': 'Cannot delete the last admin account', 'success': False}, 403
            
            # Delete user
            execute_db('DELETE FROM users WHERE id = %s', [user_id])
            
            return {
                'message': 'User deleted successfully',
                'success': True
            }, 200

    @auth_ns.route('/users')
    class UserList(Resource):
        @auth_ns.doc(security='authorization')
        @auth_ns.response(200, 'List of users', [user_model])
        @auth_ns.response(403, 'Unauthorized to list users')
        @auth_ns.response(500, 'Internal server error')
        @token_required
        def get(self, current_user):
            """List all users (admin only)"""
            # Only admins can list all users
            if current_user['role'] != 'admin':
                return {'message': 'Unauthorized - Admin access required', 'success': False}, 403
                
            users = query_db('''
                SELECT id, username, email, role, created_at 
                FROM users 
                ORDER BY created_at DESC
            ''')
            
            return {'users': users, 'success': True}, 200

    return app