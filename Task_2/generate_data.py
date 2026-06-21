import csv
import random
from datetime import datetime, timedelta
import uuid

# Настройки генерации
NUM_RECORDS = 500_000  
OUTPUT_FILE = "loan_applications.csv"

# Константы для генерации
REGIONS = ["DE-HE", "DE-BW", "DE-BY", "DE-NRW", "DE-SN", "DE-RP", "DE-NDS", "DE-OST"]
PRODUCT_TYPES = ["cash_loan", "mortgage", "car_loan", "education_loan", "business_loan", "refinance"]
CHANNELS = ["mobile", "web", "branch", "call_center", "partner"]
RISK_LEVELS = ["low", "medium", "high", "very_high"]
DECISION_STATUSES = ["approved", "rejected", "pending", "manual_review"]

# Вероятности 
PROB_APPROVED = 0.65
PROB_MANUAL_REVIEW = 0.08
PROB_REJECTED = 0.22
PROB_PENDING = 0.05

print(f"Генерация {NUM_RECORDS} записей для файла {OUTPUT_FILE}...")
print("Это займёт около 1-2 минут...")

with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    
    # Заголовки 
    writer.writerow([
        "application_id",
        "event_time",
        "customer_id",
        "region_code",
        "product_type",
        "requested_amount",
        "term_months",
        "credit_score",
        "risk_level",
        "decision_status",
        "approved_amount",
        "channel",
        "employee_review_flag",
        "processing_time_sec"
    ])
    
    # Даты для генерации
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    for i in range(NUM_RECORDS):
        # Прогресс-бар
        if i % 50000 == 0:
            print(f"Сгенерировано {i:,} из {NUM_RECORDS:,} записей ({i/NUM_RECORDS*100:.1f}%)")
        
        # Генерация application_id: app_ГГГГММДД_порядковый_номер
        event_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        date_str = event_date.strftime("%Y%m%d")
        seq_num = f"{i+1:06d}"  # 6-значный номер
        application_id = f"app_{date_str}_{seq_num}"
        
        # event_time
        event_time = event_date.replace(
            hour=random.randint(0, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
            microsecond=0
        ).strftime("%Y-%m-%d %H:%M:%S")
        
        # customer_id
        customer_id = f"cust_{random.randint(1000, 99999)}"
        
        # region_code
        region_code = random.choice(REGIONS)
        
        # product_type
        product_type = random.choice(PRODUCT_TYPES)
        
        # requested_amount 
        if product_type == "cash_loan":
            requested_amount = random.randint(1000, 50000)
        elif product_type == "mortgage":
            requested_amount = random.randint(50000, 500000)
        elif product_type == "car_loan":
            requested_amount = random.randint(5000, 60000)
        elif product_type == "education_loan":
            requested_amount = random.randint(3000, 30000)
        elif product_type == "business_loan":
            requested_amount = random.randint(10000, 200000)
        else:  # refinance
            requested_amount = random.randint(10000, 100000)
        
        # Округлим до сотен 
        requested_amount = round(requested_amount / 100) * 100
        
        # term_months (срок кредита)
        if requested_amount < 5000:
            term_months = random.choice([6, 12, 18, 24])
        elif requested_amount < 20000:
            term_months = random.choice([12, 18, 24, 36, 48])
        elif requested_amount < 50000:
            term_months = random.choice([24, 36, 48, 60])
        else:
            term_months = random.choice([36, 48, 60, 84, 120, 180, 240, 300])
            if term_months > 120 and requested_amount > 200000:
                term_months = random.choice([120, 180, 240, 300])
        
        # credit_score (от 300 до 850, нормальное распределение)
        credit_score = int(random.gauss(650, 100))
        credit_score = max(300, min(850, credit_score))
        
        # risk_level (зависит от credit_score)
        if credit_score >= 750:
            risk_level = "low"
        elif credit_score >= 650:
            risk_level = random.choices(
                ["low", "medium"],
                weights=[0.4, 0.6]
            )[0]
        elif credit_score >= 550:
            risk_level = random.choices(
                ["medium", "high"],
                weights=[0.5, 0.5]
            )[0]
        else:
            risk_level = random.choices(
                ["high", "very_high"],
                weights=[0.6, 0.4]
            )[0]
        
        # decision_status с вероятностями
        decision_status = random.choices(
            ["approved", "rejected", "pending", "manual_review"],
            weights=[PROB_APPROVED, PROB_REJECTED, PROB_PENDING, PROB_MANUAL_REVIEW]
        )[0]
        
        # approved_amount (зависит от решения)
        if decision_status == "approved":
            # Одобряют 60-100% от запрошенной суммы
            approved_percent = random.uniform(0.6, 1.0)
            approved_amount = int(requested_amount * approved_percent)
            approved_amount = round(approved_amount / 100) * 100
        elif decision_status == "manual_review":
            # Иногда одобряют после проверки
            approved_amount = random.choice([0, requested_amount, int(requested_amount * 0.8)])
            approved_amount = round(approved_amount / 100) * 100
        else:
            approved_amount = 0
        
        # channel
        channel = random.choice(CHANNELS)
        
        # employee_review_flag (зависит от суммы и решения)
        if requested_amount > 50000 or decision_status == "manual_review":
            employee_review_flag = random.choices([True, False], weights=[0.7, 0.3])[0]
        elif decision_status in ["approved", "rejected"] and risk_level in ["high", "very_high"]:
            employee_review_flag = random.choices([True, False], weights=[0.5, 0.5])[0]
        else:
            employee_review_flag = random.choices([True, False], weights=[0.05, 0.95])[0]
        
        # processing_time_sec (зависит от суммы и проверок)
        base_time = random.randint(5, 60)
        if employee_review_flag:
            base_time += random.randint(60, 300)
        if requested_amount > 100000:
            base_time += random.randint(30, 120)
        if risk_level in ["high", "very_high"]:
            base_time += random.randint(10, 60)
        
        processing_time_sec = base_time
        
        # Запись строки
        writer.writerow([
            application_id,
            event_time,
            customer_id,
            region_code,
            product_type,
            requested_amount,
            term_months,
            credit_score,
            risk_level,
            decision_status,
            approved_amount,
            channel,
            str(employee_review_flag).lower(),
            processing_time_sec
        ])

print(f"\n✅ Готово! Файл {OUTPUT_FILE} создан.")
print(f"📊 Сгенерировано {NUM_RECORDS:,} записей.")

# Проверка размера файла
import os
file_size = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
print(f"📦 Размер файла: {file_size:.2f} Мб")
