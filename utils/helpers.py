from datetime import datetime, timedelta
import aiohttp
# Import the authentication module
from auth import execute_db, query_db
import threading
import json
from flask import current_app, copy_current_request_context

# Create a shared session object
_session = None

async def get_session():
    """Get or create a shared aiohttp ClientSession."""
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=20,  # Limit total connections
                limit_per_host=5,  # Limit connections per host
                ttl_dns_cache=300  # Cache DNS results for 5 minutes
            ),
            timeout=aiohttp.ClientTimeout(total=30)  # Set timeout
        )
    return _session

# Schedule transaction deletion
def schedule_transaction_deletion(transaction_id, hours=24):
    """Schedule transaction to be deleted after specified hours."""
    deletion_time = datetime.utcnow() + timedelta(hours=hours)
    
    # Update transaction with scheduled deletion time
    execute_db(
        'UPDATE transactions SET deletion_scheduled_at = %s WHERE id = %s',
        [deletion_time, transaction_id]
    )
    
    # Create a timer to delete the transaction
    @copy_current_request_context
    def delete_transaction():
        with current_app.app_context():
            try:
                # Use execute_db instead of direct SQLite connection
                execute_db('DELETE FROM transactions WHERE id = %s', [transaction_id])
                print(f"Transaction {transaction_id} deleted as scheduled")
            except Exception as e:
                print(f"Error deleting transaction {transaction_id}: {e}")
    
    # Schedule the deletion task
    timer = threading.Timer(hours * 3600, delete_transaction)
    timer.daemon = True
    timer.start()

# Helper function to fetch data from URL
async def fetch_url_data(url):
    """Fetch data from URL using shared session."""
    try:
        session = await get_session()
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                return {'error': f'HTTP error: {response.status}'}
    except Exception as e:
        print(f"Error fetching URL {url}: {str(e)}")
        return {'error': f'Fetch error: {str(e)}'}
