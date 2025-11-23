"""Python clickstream data generation python script. This script generates clickstream data by simulating 5 different users surfing the website at the same time using 5 different threads simulating each user"""
"""Each event (click, page view, purchase) should be a JSON object with fields like: user_id, timestamp, page_url, action (e.g., 'view', 'add_to_cart', 'purchase') and session_id."""
import json 
import random
import uuid
import time
from datetime import datetime, date, timezone, timedelta
import threading

IP_ADDRESSES = [
    "203.0.113.45", "192.0.2.1", "10.0.0.5", 
    "172.16.0.10", "198.51.100.22", "192.168.1.100"
]

NUM_SIMULATED_USER = 5
MAX_SESSION_TIME = 300

def generate_session(user_id:str)->str:
    session_uid = str(hash(user_id)) + "-" + str(uuid.uuid4())
    return session_uid

def generate_clickstream_event(user_id:str, ip_address:str, current_session_id:str, random_num:int)->dict:
    actions=["view", "add_to_cart", "purchase", "support"]
    curr_timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    page_urls=[
        "https://www.amazon.in/home",
        "https://www.amazon.in/cart",
        "https://www.amazon.in/payment",
        "https://www.amazon.in/support"
    ]

    event = {
        "event_id": str(uuid.uuid4()),
        "ip_address":ip_address,
        "user_id":user_id,
        "action":actions[random_num],
        "timestamp":curr_timestamp,
        "page_url":page_urls[random_num],
        "session_id":current_session_id
    }

    return event

def send_to_kafka(event:dict):
    """
    *** Placeholder function for sending data to Kafka ***
    You will replace this with the actual 'kafka-python' producer logic.
    """
    print(json.dumps(event))


def simulate_user_session():
    user_id = str(uuid.uuid4())
    ip_address = random.choice(IP_ADDRESSES)
    current_session_id = generate_session(user_id)

    while True:
        session_start_time = time.time()
        print(f"starting new session: {current_session_id[:8]} || for user: {user_id[:8]}")

        while(time.time()-session_start_time)<MAX_SESSION_TIME:
            action = random.randint(0,3)

            event = generate_clickstream_event(
                user_id=user_id,
                ip_address=ip_address,
                current_session_id=current_session_id,
                random_num=action
            )
            
            send_to_kafka(event)

            time.sleep(random.uniform(0.1, 2.0)) # to simulate random time between user clicks

        print(f"THREAD {threading.get_ident()} | Session {current_session_id[:8]} ended. Simulating inactivity...")
        
        # New session starts after a period of inactivity (simulated by a longer sleep)
        time.sleep(random.uniform(5, 15)) 
        
        # 4. Start a New Session for the SAME user
        current_session_id = generate_session(user_id)


def main():
    print(f"Starting data generation with {NUM_SIMULATED_USER} concurrent users...")
    print("data generation starting...")

    threads = []
    for _ in range(NUM_SIMULATED_USER):
        thread = threading.Thread(target=simulate_user_session)
        thread.daemon=True
        threads.append(thread)
        thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping data generation by keyboard interrupt...")
    
    print("data generation stopping...")


if __name__ == "__main__":
    main()

