import csv
import random
from datetime import datetime, timedelta
import uuid

NUM_RECORDS = 450_000
OUTPUT_FILE = "transactions_v2.csv"

regions = ["DE-HE", "DE-BW", "DE-BY", "DE-NRW", "DE-SN"]
campaigns = ["credit_card_offer", "loan_offer", "insurance_offer", "investment_offer"]
statuses = ["answered", "no_answer", "busy", "declined", "voicemail"]
responses = ["interested", "not_interested", "callback_requested", "rejected", "pending", None]

print(f"Генерация {NUM_RECORDS} записей...")

with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["call_id", "call_time", "client_id", "region_code", "campaign_type", 
                     "call_status", "client_response", "duration_sec", "follow_up_required"])
    
    for i in range(NUM_RECORDS):
        if i % 50000 == 0:
            print(f"Сгенерировано {i} записей...")
        
        call_id = f"call_{datetime.now().strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}"
        call_time = (datetime.now() - timedelta(days=random.randint(0, 30))).replace(
            hour=random.randint(8, 20),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
            microsecond=0
        ).strftime("%Y-%m-%d %H:%M:%S")
        
        client_id = f"client_{random.randint(1000, 99999)}"
        region = random.choice(regions)
        campaign = random.choice(campaigns)
        status = random.choice(statuses)
        response = random.choice(responses) if status == "answered" else None
        duration = random.randint(10, 600) if status == "answered" else 0
        follow_up = random.choice([True, False]) if status == "answered" and duration > 60 else False
        
        writer.writerow([call_id, call_time, client_id, region, campaign, status, response, duration, follow_up])

print(f"Готово! Файл {OUTPUT_FILE} создан.")