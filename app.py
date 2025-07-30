from flask import Flask, jsonify
from collections import deque
import threading
import json
import time
from datetime import datetime, timezone
import websocket  # from websocket-client package

app = Flask(__name__)

# -------------------------
# Global shared state
# -------------------------
trade_volumes = deque(maxlen=200)  # store the last 200 trade volumes
volume_sum = 0.0  # running sum for efficient moving-average calculation
last_signal = None  # stores the most recent Volume Spike detected
state_lock = threading.Lock()  # guard shared state across threads

# -------------------------
# WebSocket handling
# -------------------------

def on_message(ws, message):
    """Callback executed on every incoming trade message from Binance."""
    global volume_sum, last_signal

    try:
        data = json.loads(message)
        # Binance trade stream fields we care about
        price = float(data.get("p", 0))
        volume = float(data.get("q", 0))
        timestamp_ms = data.get("T", int(time.time() * 1000))
    except (ValueError, TypeError):
        # Ignore malformed messages
        return

    with state_lock:
        # Maintain running window statistics
        if len(trade_volumes) == trade_volumes.maxlen:
            # Evict oldest volume from running sum when the window is full
            volume_sum -= trade_volumes[0]
        trade_volumes.append(volume)
        volume_sum += volume

        # Ensure we have enough data to compute a meaningful average
        if len(trade_volumes) == 0:
            return

        moving_avg = volume_sum / len(trade_volumes)

        # Detect Volume Spike as defined in the requirements
        if moving_avg > 0 and volume >= 10 * moving_avg:
            last_signal = {
                "signal_type": "VolumeSpike",
                "price": price,
                "volume": volume,
                "timestamp": datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()
            }


def on_error(ws, error):
    print(f"[WebSocket] Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("[WebSocket] Connection closed. Reconnecting soon…")


def start_websocket_stream():
    """Runs inside a dedicated thread to maintain the Binance WebSocket connection."""
    ws_url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever()
        except Exception as exc:
            print(f"[WebSocket] Exception occurred: {exc}. Retrying in 5 seconds…")
            time.sleep(5)  # simple back-off before attempting to reconnect


# Start the WebSocket listener in the background before the first request
ws_thread = threading.Thread(target=start_websocket_stream, daemon=True)
ws_thread.start()

# -------------------------
# Flask API Endpoints
# -------------------------

@app.route("/api/get_signal", methods=["GET"])
def get_signal():
    with state_lock:
        if last_signal:
            return jsonify(last_signal)
        else:
            return jsonify({"status": "Awaiting first signal..."})


if __name__ == "__main__":
    # Run the Flask development server
    app.run(host="0.0.0.0", port=5000, debug=False)