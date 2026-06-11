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
CITIES = ['Warsaw', 'Krakow', 'Gdansk', 'Wroclaw', 'Berlin', 'London']
NORMAL_WEIGHTS = [45, 16, 14, 10, 7, 6]

def pick_city(is_fraud):
    if is_fraud:
        return random.choices(CITIES, weights=[55, 10, 8, 7, 10, 10])[0]
    return random.choices(CITIES, weights=[42, 18, 14, 10, 10, 6])[0]

def pick_category(is_fraud, city):
    if is_fraud:
        return random.choices(CATEGORIES, weights=[10, 18, 16, 10, 38, 8])[0]
    if city == 'London':
        return random.choices(CATEGORIES, weights=[18, 16, 16, 22, 20, 8])[0]
    if city == 'Berlin':
        return random.choices(CATEGORIES, weights=[22, 18, 18, 18, 16, 8])[0]
    return random.choices(CATEGORIES, weights=NORMAL_WEIGHTS)[0]

def pick_amount(is_fraud, category):
    if is_fraud:
        if category == 'online_shop':
            return round(random.uniform(120, 2500) + random.gauss(0, 180), 2)
        if category == 'atm':
            return round(random.uniform(250, 4500) + random.gauss(0, 260), 2)
        if category == 'fuel':
            return round(random.uniform(80, 1800) + random.gauss(0, 120), 2)
        return round(random.uniform(50, 3200) + random.gauss(0, 220), 2)

    base = {
        'grocery': (15, 450),
        'electronics': (50, 2200),
        'fuel': (30, 260),
        'restaurant': (20, 500),
        'online_shop': (15, 1800),
        'atm': (40, 800),
    }[category]
    amount = random.uniform(*base) + random.gauss(0, (base[1] - base[0]) * 0.08)
    return round(max(1.0, amount), 2)

def make_transaction():
    user_id = random.choice(USERS)
    timestamp = datetime.now(timezone.utc)
    hour = timestamp.hour
    is_night = hour >= 23 or hour <= 5

    base_fraud = 0.03
    if is_night:
        base_fraud += 0.03
    if random.random() < 0.12:
        base_fraud += 0.02

    is_fraud = random.random() < base_fraud

    city = pick_city(is_fraud)
    category = pick_category(is_fraud, city)
    amount = pick_amount(is_fraud, category)

    if not is_fraud and random.random() < 0.08:
        amount = round(amount * random.uniform(0.75, 1.25), 2)
    if is_fraud and random.random() < 0.25:
        amount = round(amount * random.uniform(0.55, 1.15), 2)

    return {
        'transaction_id': f"TXN-{uuid.uuid4().hex[:8]}",
        'timestamp': timestamp.isoformat(),
        'user_id': user_id,
        'amount': amount,
        'currency': 'PLN',
        'merchant_category': category,
        'city': city,
        'is_fraud': is_fraud,
    }

print('Producer wystartowal...')
while True:
    txn = make_transaction()
    producer.send('transactions', key=txn['user_id'], value=txn)
    print(txn)
    time.sleep(random.uniform(0.25, 0.9))