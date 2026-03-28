from pymongo import MongoClient
from faker import Faker
import random
from datetime import datetime, timedelta

# Фиксируем seed для воспроизводимости
random.seed(42)
Faker.seed(42)

# Подключение к MongoDB
client = MongoClient('mongodb://admin:admin123@localhost:27017/')
db = client['ecommerce']

# Очищаем старые коллекции
db.users.drop()
db.products.drop()
db.orders.drop()
db.reviews.drop()

fake = Faker('ru_RU')

print("=" * 50)
print("Генерация данных для MongoDB (seed=42)")
print("=" * 50)

# ========== 1. Генерируем продукты (40 штук) ==========
print("\n1. Генерация продуктов...")

categories = ['Электроника', 'Одежда', 'Книги', 'Дом и сад', 'Спорт', 'Игрушки', 'Косметика']

# Реальные названия товаров
product_names = {
    'Электроника': ['Смартфон Galaxy', 'Ноутбук Air', 'Планшет Tab', 'Наушники Pro', 'Умные часы', 'Телевизор 4K', 'Колонка Smart', 'Фотоаппарат', 'Дрон'],
    'Одежда': ['Футболка хлопок', 'Джинсы классические', 'Куртка зимняя', 'Свитер уютный', 'Кроссовки беговые', 'Шапка теплая', 'Пальто осеннее', 'Платье вечернее', 'Шарф шерстяной'],
    'Книги': ['Мастер и Маргарита', 'Преступление и наказание', 'Война и мир', '1984', 'Маленький принц', 'Три товарища', 'Атлант расправил плечи', 'Сто лет одиночества', 'Дюна'],
    'Дом и сад': ['Набор кастрюль', 'Сковорода антипригарная', 'Ковер с узорами', 'Лопата снеговая', 'Горшок для цветов', 'Стол складной', 'Ваза фарфоровая', 'Полка настенная'],
    'Спорт': ['Гантели 5кг', 'Гиря 16кг', 'Фитнес-браслет', 'Скакалка', 'Мяч футбольный', 'Бутылка для воды', 'Велотренажер', 'Эспандер'],
    'Игрушки': ['Конструктор Lego', 'Мягкая игрушка Мишка', 'Робот трансформер', 'Набор для творчества', 'Машинка радиоуправляемая', 'Кукла', 'Пазл 1000 деталей'],
    'Косметика': ['Крем увлажняющий', 'Шампунь восстанавливающий', 'Тушь для ресниц', 'Помада матовая', 'Маска для лица', 'Скраб для тела', 'Тоник очищающий']
}

# Ценовые диапазоны для каждой категории (min, max)
price_ranges = {
    'Электроника': (8000, 50000),
    'Одежда': (2000, 10000),
    'Книги': (300, 1200),
    'Дом и сад': (1500, 4000),
    'Спорт': (2000, 8000),
    'Игрушки': (500, 5000),
    'Косметика': (300, 4000)
}

# Собираем все возможные товары
all_possible_products = []
for category, names in product_names.items():
    for name in names:
        all_possible_products.append((category, name))

# Берем первые 40 уникальных
products = []
for i in range(40):
    category, name = all_possible_products[i]
    min_price, max_price = price_ranges[category]
    product = {
        'product_id': i + 1,
        'name': name,
        'category': category,
        'price': round(random.uniform(min_price, max_price), 2)
    }
    products.append(product)

db.products.insert_many(products)
print(f"   Добавлено {len(products)} продуктов")
print(f"\n   Примеры цен:")
for p in products[:5]:
    print(f"      {p['name']} ({p['category']}): {p['price']} руб.")

# ========== 2. Генерируем пользователей (150) ==========
print("\n2. Генерация пользователей...")
users = []
for i in range(1, 151):
    user = {
        'user_id': i,
        'name': fake.name(),
        'email': fake.email() if random.random() > 0.05 else '',  # 5% пустых email
        'city': fake.city(),
        'registration_date': fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
    }
    users.append(user)

db.users.insert_many(users)
print(f"   Добавлено {len(users)} пользователей")

# ========== 3. Генерируем заказы (800) ==========
print("\n3. Генерация заказов...")
orders = []
valid_users = [u['user_id'] for u in users if u['email']]  # только с email

start_date = datetime(2024, 1, 1)
end_date = datetime.now()

for i in range(1, 801):
    user_id = random.choice(valid_users)
    order_date = fake.date_between(start_date=start_date, end_date=end_date)
    
    # Генерируем от 1 до 5 товаров в заказе
    num_items = random.randint(1, 5)
    items = []
    used_products = set()
    
    for _ in range(num_items):
        product = random.choice(products)
        if product['product_id'] not in used_products:
            items.append({
                'product_id': product['product_id'],
                'quantity': random.randint(1, 3)
            })
            used_products.add(product['product_id'])
    
    # Статус: 80% completed, 20% cancelled
    status = 'completed' if random.random() < 0.8 else 'cancelled'
    
    order = {
        'order_id': i,
        'user_id': user_id,
        'order_date': order_date.strftime('%Y-%m-%d'),
        'status': status,
        'items': items
    }
    orders.append(order)

db.orders.insert_many(orders)
print(f"   Добавлено {len(orders)} заказов")
print(f"   Из них completed: {len([o for o in orders if o['status'] == 'completed'])}")

# ========== 4. Генерируем отзывы (250) ==========
print("\n4. Генерация отзывов...")
reviews = []
# Только для завершенных заказов
completed_orders = [o for o in orders if o['status'] == 'completed']

review_id = 1
for order in random.sample(completed_orders, min(250, len(completed_orders))):
    user_id = order['user_id']
    
    for item in order['items']:
        product_id = item['product_id']
        # Не каждый товар получает отзыв (60% шанс)
        if random.random() < 0.6:
            review = {
                'review_id': review_id,
                'product_id': product_id,
                'user_id': user_id,
                'rating': random.randint(1, 5),
                'comment': fake.sentence() if random.random() > 0.3 else '',
                'created_date': fake.date_between(
                    start_date=datetime.strptime(order['order_date'], '%Y-%m-%d'),
                    end_date=datetime.now()
                ).strftime('%Y-%m-%d')
            }
            reviews.append(review)
            review_id += 1

db.reviews.insert_many(reviews)
print(f"   Добавлено {len(reviews)} отзывов")

print("\n" + "=" * 50)
print("✅ Генерация данных завершена!")
print("=" * 50)
print(f"\n📊 Статистика:")
print(f"   Продукты: {db.products.count_documents({})}")
print(f"   Пользователи: {db.users.count_documents({})}")
print(f"   Пользователи с email: {len([u for u in users if u['email']])}")
print(f"   Заказы: {db.orders.count_documents({})}")
print(f"   Заказы completed: {len([o for o in orders if o['status'] == 'completed'])}")
print(f"   Отзывы: {db.reviews.count_documents({})}")
