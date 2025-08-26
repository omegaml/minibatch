import gevent
from gevent import monkey
monkey.patch_all()

from flask import Flask, jsonify
import random
import time

app = Flask(__name__)

# List to store the greenlets
greenlets = []

# Function to simulate an I/O-bound task
def io_task():
    while True:
        # Simulate I/O wait
        time.sleep(random.uniform(0.5, 2))
        
        # Perform some small computation
        result = random.randint(1, 100)
        print(f"Task result: {result}")

        # Wait again
        time.sleep(random.uniform(0.5, 2))

# Function to start the worker
def start_worker():
    # Start the greenlets
    for _ in range(100):
        greenlet = gevent.spawn(io_task)
        greenlets.append(greenlet)

    # Wait for the greenlets to finish
    gevent.joinall(greenlets)

# Flask route to get the status of the worker
@app.route('/worker_status', methods=['GET'])
def get_worker_status():
    # Get the status of the greenlets
    status = {
        'num_greenlets': len(greenlets),
        'active_greenlets': sum(1 for g in greenlets if not g.dead)
    }
    return jsonify(status)

gevent.spawn(start_worker)

if __name__ == '__main__':
    # Start the worker in a separate greenlet
    gevent.spawn(start_worker)    
    # Start the Flask app
    app.run(host='0.0.0.0', port=5001)
