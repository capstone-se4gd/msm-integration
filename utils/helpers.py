from datetime import datetime, timedelta
import aiohttp
# Import the authentication module
from auth import execute_db
import sqlite3
import threading
import json
from flask import current_app, copy_current_request_context

# Database file path
DATABASE = 'database.db'

# Schedule transaction deletion
def schedule_transaction_deletion(transaction_id, hours=24):
    """Schedule transaction to be deleted after specified hours."""
    deletion_time = datetime.utcnow() + timedelta(hours=hours)
    
    # Update transaction with scheduled deletion time
    execute_db(
        'UPDATE transactions SET deletion_scheduled_at = ? WHERE id = ?',
        [deletion_time.isoformat(), transaction_id]
    )
    
    # Create a timer to delete the transaction
    @copy_current_request_context
    def delete_transaction():
        with current_app.app_context():
            try:
                conn = sqlite3.connect(DATABASE)
                cursor = conn.cursor()
                cursor.execute('DELETE FROM transactions WHERE id = ?', [transaction_id])
                conn.commit()
                conn.close()
                print(f"Transaction {transaction_id} deleted as scheduled")
            except Exception as e:
                print(f"Error deleting transaction {transaction_id}: {e}")
    
    # Schedule the deletion task
    timer = threading.Timer(hours * 3600, delete_transaction)
    timer.daemon = True
    timer.start()

# Helper function to fetch data from URL
async def fetch_url_data(url):
    """Fetch data from URL asynchronously."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return {'error': f'HTTP error: {response.status}'}
    except aiohttp.ClientError as e:
        print(f"ClientError while fetching URL {url}: {str(e)}")
        return {'error': f'Connection error: {str(e)}'}
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError while parsing response from {url}: {str(e)}")
        return {'error': f'Invalid JSON response: {str(e)}'}
    except Exception as e:
        print(f"Unexpected error while fetching {url}: {str(e)}")
        return {'error': f'Fetch error: {str(e)}'}
