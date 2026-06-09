from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

USERS = [f"U-{i:03d}" for i in range(50)]
CATEGORIES = ['grocery', 'electronics', 'fuel', 'restaurant', 'online_shop', 'atm']

def make_transaction():
    user_id = random.choice(USERS)
    timestamp = datetime.now(timezone.utc)
    
    # 5% szans na fraud
    is_fraud = random.random() < 0.05 
    
    # Baza dla normalnej transakcji - dodajemy szum, zwykli ludzie też podróżują
    city = random.choices(
        ['Warsaw', 'Krakow', 'Gdansk', 'Berlin', 'London'], 
        weights=[70, 15, 10, 3, 2]
    )[0]
    category = random.choice(CATEGORIES)
    amount = round(random.uniform(10, 500), 2)

    if is_fraud:
        fraud_type = random.choice(['card_testing', 'high_value', 'foreign_atm'])
        
        if fraud_type == 'card_testing':
            amount = round(random.uniform(1.0, 5.0), 2)
            category = 'online_shop'
            
        elif fraud_type == 'high_value':
            amount = round(random.uniform(3000, 8000), 2)
            category = random.choice(['electronics', 'fuel'])
            
        elif fraud_type == 'foreign_atm':
            amount = round(random.uniform(1000, 4000), 2)
            city = random.choice(['Lagos', 'Berlin', 'London'])
            category = 'atm'

    return {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:8]}",
        "timestamp": timestamp.isoformat(),
        "user_id": user_id,
        "amount": amount,
        "currency": "PLN",
        "merchant_category": category,
        "city": city,
        "is_fraud": is_fraud
    }

print("Producer wystartowal...")
while True:
    txn = make_transaction()
    producer.send('transactions', key=txn['user_id'], value=txn)
    print(txn)
    time.sleep(0.5)