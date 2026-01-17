# app.py
import asyncio
import json
import time
import os
import threading
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from flask import Flask, request, jsonify
from flask_cors import CORS
import websockets
import aiohttp
from frappeclient import FrappeClient

from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

client = FrappeClient(os.getenv("FRAPPE_URL"))
client.authenticate(os.getenv("API_KEY"), os.getenv("API_SECRET"))

# Configure logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_date = datetime.now().strftime("%d.%m.%Y")
log_file = os.path.join(log_dir, f"orderlog_{log_date}.log")

# Create handlers
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING) # Reduce terminal noise

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[file_handler, console_handler]
)
logger = logging.getLogger(__name__)

# Global state for WebSocket connection
class WebSocketManager:
    def __init__(self):
        self.ws = None
        self.is_connected = False
        self.is_connecting = False
        self.heartbeat_count = 0
        self.connection_status = "Disconnected"
        self.reconnect_attempts = 0
        self.webhook_sends = 0
        self.max_reconnect_attempts = 5
        self.should_reconnect = False  # Control flag for reconnection
        self.messages = []
        self.heartbeat_task = None
        self.reconnect_task = None
        self.loop = None
        self.thread = None
        self._loop_initialized = False
        
    def add_message(self, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.messages.append(f"{timestamp}: {msg}")
        # Keep only last 1000 messages to prevent memory issues
        if len(self.messages) > 1000:
            self.messages = self.messages[-1000:]
        logger.info(msg)

    def ensure_event_loop(self):
        """Ensure an event loop exists for async operations"""
        if self.loop is None and not self._loop_initialized:
            try:
                # Create new event loop
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
            except Exception as e:
                logger.error(f"Error creating event loop: {e}")
            self._loop_initialized = True

ws_manager = WebSocketManager()

import queue

# Order processing queue
order_queue = queue.Queue()

def process_order_queue():
    """Worker to process orders from queue and send to Frappe"""
    while True:
        try:
            order_data = order_queue.get()
            order_id = order_data.get('norenordno', 'Unknown')
            
            try:
                # Convert datetime
                norentm = order_data.get('norentm', '')
                formatted_date = None
                if norentm:
                    try:
                        # Input format from user request: "19:28:42 16-01-2026"
                        # Which corresponds to "%H:%M:%S %d-%m-%Y"
                        dt_obj = datetime.strptime(norentm, "%H:%M:%S %d-%m-%Y")
                        # Output format for Frappe/MySQL: "2026-01-16 19:28:42"
                        formatted_date = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        try:
                             # Fallback for potential alternative format or if already correct
                             # trying to parse potential "DD-MM-YYYY HH:MM:SS" or similar
                             # But logs show: "Incorrect datetime value: '19:59:09 16-01-2026'
                             # This confirms the DB received the raw string instead of the formatted one
                             # Wait, the logs say: Incorrect datetime value: '19:59:09 16-01-2026'
                             # This means it WAS trying to insert the original string '19:59:09 16-01-2026'
                             # So formatted_date mapping might have failed or not been used?
                             # Ah, if formatted_date is None (exception raised), it falls back to now()
                             # BUT, in the dict below: "norentm": formatted_date or ...
                             # If my parsing works, it should be fine.
                             # Let's add extra logging to debug exactly what we parsed.
                             logger.warning(f"Could not parse date: {norentm}")
                             formatted_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                             formatted_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Prepare document
                doc = {
                    "doctype": "Sky Order Feed",
                    **order_data,  # Unpack original data first
                    "norentm": formatted_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Overwrite with formatted date
                }
                
                # Cleanup/Exclude fields not needed or that clash?
                # User said "all the fields created everything is data field", so we can probably dump everything.
                # However, 'doctype' is set manually.
                
                logger.info(f"📤 Inserting order {order_id} to Frappe...")
                
                # Insert into Frappe
                try:
                    new_doc = client.insert(doc)
                    logger.info(f"✅ Order {order_id} created in Frappe: {new_doc.get('name')}")
                except Exception as e:
                    # Check if it's a duplicate entry error
                    error_str = str(e)
                    if "Duplicate entry" in error_str or "IntegrityError" in error_str:
                        logger.info(f"ℹ️ Order {order_id} exists. Updating status...")
                        # Update the existing record
                        # We need to ensure 'doctype' and 'name' (or key field) are in doc
                        # 'norenordno' seems to be mapped to 'name' automatically by Frappe or your setup
                        # If 'name' is not in doc, client.update might fail if it relies on it.
                        # Assuming 'norenordno' is the name, let's make sure valid doc is passed.
                        # Usually update requires {"doctype": "...", "name": "...", ...fields...}
                        doc['name'] = order_id # Ensure name is set for update
                        updated_doc = client.update(doc)
                        logger.info(f"🔄 Order {order_id} updated: {updated_doc.get('status')}")
                    else:
                        raise e
                
            except Exception as e:
                # Don't log full stack trace for duplicates if we missed the catch above
                if "Duplicate entry" in str(e):
                     logger.info(f"ℹ️ Order {order_id} already exists (caught in outer block).")
                else:
                     logger.error(f"❌ Failed to insert order {order_id}: {e}")
            finally:
                order_queue.task_done()
                
        except Exception as e:
            logger.error(f"Error in order worker: {e}")
            time.sleep(1)

# Start worker thread
worker = threading.Thread(target=process_order_queue, daemon=True)
worker.start()

# Webhook configuration (Deprecated)
# WEBHOOK_URL = "https://n8n.gopocket.in/webhook/orderfeed"

async def handle_websocket_message(data: Dict[str, Any]):
    """Handle incoming WebSocket messages"""
    # Connection acknowledgement
    if data.get('t') == "ck":
        if data.get('s') == "OK":
            ws_manager.connection_status = "Connected - Authenticated"
            ws_manager.add_message('✓ Authentication successful')
            ws_manager.reconnect_attempts = 0  # Reset reconnection attempts
            
            # Start heartbeat after successful authentication
            await start_heartbeat()
        else:
            ws_manager.connection_status = "Connected - Auth Failed"
            ws_manager.add_message('✗ Authentication failed')
            # If authentication fails, disconnect and retry if reconnection is enabled
            await disconnect_websocket()
            if ws_manager.should_reconnect:
                await attempt_reconnect()
    
    # Handle heartbeat response
    elif data.get('t') == "hk":
        ws_manager.add_message(f"✓ Heartbeat acknowledged (ft: {data.get('ft', 'N/A')})")
    
    # Handle tick data
    elif data.get('t') in ["tf", "tk"]:
        ws_manager.add_message("📈 Tick data received")
    
    # Handle order feed data
    elif data.get('t') in ["om", "ok"]:
        order_id = data.get('norenordno', 'Unknown')
        symbol = data.get('tsym', 'Unknown')
        status = data.get('status', 'Unknown')
        report_type = data.get('reporttype', 'Unknown')
        
        ws_manager.add_message(
            f"📊 Order {order_id} - {symbol} - Status: {status} - Report: {report_type}"
        )
        
        # Send order messages to Frappe queue
        if data.get('t') == "om":
            # Add to queue for background processing
            order_queue.put(data)
            ws_manager.add_message(f"Order {order_id} queued for processing")

def get_susertoken():
    """Fetch susertoken from Gopocket Settings"""
    try:
        # For Single DocTypes, name is usually the same as doctype
        settings = client.get_doc("Gopocket Settings", "Gopocket Settings")
        token = settings.get("susertoken")
        if token:
            logger.info("Successfully fetched susertoken from Gopocket Settings")
            return token
        else:
            logger.warning("susertoken not found in Gopocket Settings")
            return None
    except Exception as e:
        logger.error(f"Error fetching susertoken: {e}")
        return None

async def authenticate_websocket():
    """Send authentication message to WebSocket"""
    if ws_manager.ws and ws_manager.ws.open:
        
        # Get dynamic token
        token = get_susertoken()
        if not token:
             # Fallback or fail? User implies we MUST get it.
             # If we fail, we probably shouldn't auth, but let's try with the old hardcoded one as backup 
             # OR just fail. I will log and fail the auth if no token.
             logger.error("Cannot authenticate: missing susertoken")
             return

        auth_message = {
            "susertoken": token,
            "t": "c",
            "uid": "SKYAPI",
            "source": "FU"
        }
        
        await ws_manager.ws.send(json.dumps(auth_message))
        # Mask token in logs for security
        safe_msg = auth_message.copy()
        safe_msg['susertoken'] = '***'
        ws_manager.add_message(f"Sent authentication: {json.dumps(safe_msg)}")
        ws_manager.connection_status = 'Connected - Authenticating'

async def start_heartbeat():
    """Start sending heartbeat every 10 seconds"""
    if ws_manager.heartbeat_task:
        ws_manager.heartbeat_task.cancel()
    
    async def heartbeat_loop():
        while True:
            try:
                if ws_manager.ws and ws_manager.ws.open:
                    heartbeat_message = {"k": "", "t": "h"}
                    await ws_manager.ws.send(json.dumps(heartbeat_message))
                    ws_manager.add_message(f"Sent heartbeat: {json.dumps(heartbeat_message)}")
                    ws_manager.heartbeat_count += 1
                await asyncio.sleep(10)  # 10 seconds interval
            except asyncio.CancelledError:
                break
            except Exception as e:
                ws_manager.add_message(f"Heartbeat error: {e}")
                await asyncio.sleep(10)
    
    ws_manager.heartbeat_task = asyncio.create_task(heartbeat_loop())

async def stop_heartbeat():
    """Stop heartbeat sending"""
    if ws_manager.heartbeat_task:
        ws_manager.heartbeat_task.cancel()
        ws_manager.heartbeat_task = None

async def attempt_reconnect():
    """Attempt to reconnect immediately without delay"""
    if not ws_manager.should_reconnect:
        return
    
    # Check if we've exceeded max reconnection attempts
    if ws_manager.reconnect_attempts >= ws_manager.max_reconnect_attempts:
        ws_manager.add_message(
            f"Max reconnection attempts ({ws_manager.max_reconnect_attempts}) reached. "
            "Please connect manually."
        )
        ws_manager.connection_status = 'Max Reconnect Attempts Reached'
        ws_manager.reconnect_attempts = 0
        return
    
    attempt_number = ws_manager.reconnect_attempts + 1
    
    ws_manager.add_message(
        f"Reconnection attempt {attempt_number}/{ws_manager.max_reconnect_attempts} immediately..."
    )
    ws_manager.connection_status = f"Reconnecting... (Attempt {attempt_number}/{ws_manager.max_reconnect_attempts})"
    
    ws_manager.reconnect_attempts += 1
    await connect_websocket()

async def connect_websocket():
    """Connect to WebSocket server"""
    # Clear any existing reconnection
    if ws_manager.reconnect_task:
        ws_manager.reconnect_task.cancel()
        ws_manager.reconnect_task = None
    
    if ws_manager.is_connecting or ws_manager.is_connected:
        ws_manager.add_message('WebSocket is already connecting or connected')
        return
    
    ws_manager.is_connecting = True
    ws_manager.connection_status = 'Connecting...'
    ws_manager.add_message('Connecting to WebSocket...')
    
    try:
        # Connect to WebSocket
        ws_manager.ws = await websockets.connect(
            'wss://skypro.skybroking.com/NorenWSAdmin/',
            ping_interval=None
        )
        
        ws_manager.is_connected = True
        ws_manager.is_connecting = False
        ws_manager.connection_status = 'Connected - Authenticating'
        ws_manager.add_message('WebSocket connected successfully')
        
        # Send authentication message
        await authenticate_websocket()
        
        # Start listening for messages
        async def listen_for_messages():
            try:
                async for message in ws_manager.ws:
                    try:
                        data = json.loads(message)
                        ws_manager.add_message(f"Received: {json.dumps(data)}")
                        await handle_websocket_message(data)
                    except json.JSONDecodeError:
                        ws_manager.add_message(f"Received (non-JSON): {message}")
            except websockets.exceptions.ConnectionClosed as e:
                ws_manager.is_connected = False
                ws_manager.is_connecting = False
                await stop_heartbeat()
                
                status = 'Disconnected'
                should_reconnect = ws_manager.should_reconnect
                
                if e.code == 1000:
                    status = 'Normal Closure'
                    ws_manager.add_message('Normal closure detected - will attempt to reconnect')
                elif e.code == 1006:
                    status = 'Abnormal Closure'
                    ws_manager.add_message('Abnormal closure detected - will attempt to reconnect')
                else:
                    status = f'Closed (Code: {e.code})'
                
                ws_manager.connection_status = status
                ws_manager.add_message(
                    f"WebSocket disconnected. Code: {e.code}, "
                    f"Status: {status}, Reason: {e.reason or 'No reason provided'}"
                )
                
                ws_manager.ws = None
                
                # Attempt to reconnect if reconnection is enabled
                if should_reconnect:
                    await attempt_reconnect()
            except Exception as e:
                ws_manager.is_connected = False
                ws_manager.is_connecting = False
                ws_manager.connection_status = 'Connection Error'
                ws_manager.add_message(f'WebSocket error: {e}')
                logger.error(f'WebSocket error: {e}')
                
                if ws_manager.should_reconnect:
                    await attempt_reconnect()
        
        # Start listening in background
        await listen_for_messages()
        
    except Exception as error:
        ws_manager.is_connecting = False
        ws_manager.connection_status = 'Connection Failed'
        ws_manager.add_message(f'Failed to create WebSocket: {error}')
        logger.error(f'WebSocket connection error: {error}')
        
        # Attempt reconnect on connection failure if reconnection is enabled
        if ws_manager.should_reconnect:
            await attempt_reconnect()

async def disconnect_websocket():
    """Disconnect WebSocket and stop all reconnection attempts"""
    # Disable reconnection
    ws_manager.should_reconnect = False
    
    # Clear reconnection attempts counter
    ws_manager.reconnect_attempts = 0
    
    # Stop heartbeat
    await stop_heartbeat()
    
    # Clear reconnect task
    if ws_manager.reconnect_task:
        ws_manager.reconnect_task.cancel()
        ws_manager.reconnect_task = None
    
    # Close WebSocket connection
    if ws_manager.ws:
        await ws_manager.ws.close(1000, 'User initiated disconnect')
        ws_manager.ws = None
    
    ws_manager.is_connected = False
    ws_manager.is_connecting = False
    ws_manager.connection_status = 'Disconnected (Manual)'
    ws_manager.add_message('Manually disconnected - auto-reconnect disabled')

def run_async_in_thread(coro):
    """Run an async coroutine in the event loop properly"""
    ws_manager.ensure_event_loop()
    
    # If the loop is already running (in another thread), schedule it thread-safely
    if ws_manager.loop and ws_manager.loop.is_running():
        return asyncio.run_coroutine_threadsafe(coro, ws_manager.loop)
    
    # Otherwise run in a separate thread to avoid blocking Flask
    def run():
        try:
            ws_manager.loop.run_until_complete(coro)
        except Exception as e:
            logger.error(f"Error in async thread: {e}")
    
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread

# Initialize event loop on app startup
with app.app_context():
    ws_manager.ensure_event_loop()

# API Endpoints
@app.route('/api/start', methods=['POST'])
def start_connection():
    """Start WebSocket connection"""
    if ws_manager.is_connecting or ws_manager.is_connected:
        return jsonify({
            'status': 'error',
            'message': 'WebSocket is already connecting or connected'
        }), 400
    
    # Enable reconnection
    ws_manager.should_reconnect = True
    
    # Start connection in background thread
    ws_manager.thread = run_async_in_thread(connect_websocket())
    
    return jsonify({
        'status': 'success',
        'message': 'WebSocket connection started',
        'connection_status': ws_manager.connection_status
    })

@app.route('/api/stop', methods=['POST'])
def stop_connection():
    """Stop WebSocket connection (no reconnection)"""
    # Run disconnect in background thread
    run_async_in_thread(disconnect_websocket())
    
    return jsonify({
        'status': 'success',
        'message': 'WebSocket connection stopped',
        'connection_status': ws_manager.connection_status
    })

@app.route('/api/status', methods=['GET'])
def get_status():
    """Get current connection status"""
    return jsonify({
        'is_connected': ws_manager.is_connected,
        'is_connecting': ws_manager.is_connecting,
        'heartbeat_count': ws_manager.heartbeat_count,
        'connection_status': ws_manager.connection_status,
        'reconnect_attempts': ws_manager.reconnect_attempts,
        'webhook_sends': ws_manager.webhook_sends,
        'max_reconnect_attempts': ws_manager.max_reconnect_attempts,
        'should_reconnect': ws_manager.should_reconnect,
        'total_messages': len(ws_manager.messages)
    })

@app.route('/api/messages', methods=['GET'])
def get_messages():
    """Get recent WebSocket messages"""
    limit = request.args.get('limit', default=100, type=int)
    messages = ws_manager.messages[-limit:] if ws_manager.messages else []
    
    return jsonify({
        'messages': messages,
        'total': len(ws_manager.messages)
    })

@app.route('/api/clear-messages', methods=['POST'])
def clear_messages():
    """Clear all messages"""
    ws_manager.messages = []
    return jsonify({
        'status': 'success',
        'message': 'Messages cleared'
    })

@app.route('/api/send-heartbeat', methods=['POST'])
def send_heartbeat():
    """Send manual heartbeat"""
    if not ws_manager.is_connected or not ws_manager.ws:
        return jsonify({
            'status': 'error',
            'message': 'Cannot send heartbeat - WebSocket not connected'
        }), 400
    
    async def send_heartbeat_async():
        if ws_manager.ws and ws_manager.ws.open:
            heartbeat_message = {"k": "", "t": "h"}
            await ws_manager.ws.send(json.dumps(heartbeat_message))
            ws_manager.add_message(f"Manual heartbeat sent: {json.dumps(heartbeat_message)}")
            ws_manager.heartbeat_count += 1
    
    run_async_in_thread(send_heartbeat_async())
    
    return jsonify({
        'status': 'success',
        'message': 'Heartbeat sent'
    })

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'websocket-manager',
        'timestamp': datetime.now().isoformat(),
        'connection_status': ws_manager.connection_status
    })

@app.route('/')
def index():
    """Simple homepage"""
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>WebSocket Manager API</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .endpoint { background: #f5f5f5; padding: 15px; margin: 10px 0; border-radius: 5px; }
            .method { font-weight: bold; color: #007bff; }
            .url { font-family: monospace; background: #e9ecef; padding: 2px 5px; }
        </style>
    </head>
    <body>
        <h1>WebSocket Manager API</h1>
        <p>Use the following endpoints to control the WebSocket connection:</p>
        
        <div class="endpoint">
            <span class="method">POST</span> <span class="url">/api/start</span>
            <p>Start WebSocket connection with auto-reconnection enabled</p>
        </div>
        
        <div class="endpoint">
            <span class="method">POST</span> <span class="url">/api/stop</span>
            <p>Stop WebSocket connection (disables all reconnection)</p>
        </div>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="url">/api/status</span>
            <p>Get current connection status</p>
        </div>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="url">/api/messages?limit=100</span>
            <p>Get recent WebSocket messages</p>
        </div>
        
        <div class="endpoint">
            <span class="method">POST</span> <span class="url">/api/send-heartbeat</span>
            <p>Send manual heartbeat</p>
        </div>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="url">/health</span>
            <p>Health check endpoint</p>
        </div>
        
        <p>Current Status: <strong id="status">Loading...</strong></p>
        
        <script>
            // Fetch current status
            fetch('/api/status')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('status').textContent = data.connection_status;
                    document.getElementById('status').style.color = 
                        data.connection_status.includes('Connected') ? 'green' : 
                        data.connection_status.includes('Connecting') ? 'orange' : 'red';
                })
                .catch(error => {
                    document.getElementById('status').textContent = 'Error fetching status';
                });
        </script>
    </body>
    </html>
    '''

if __name__ == '__main__':
    # Initialize event loop
    ws_manager.ensure_event_loop()
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=5000, debug=True)