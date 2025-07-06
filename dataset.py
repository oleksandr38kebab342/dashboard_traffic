import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import ipaddress

# Встановлюємо seed для відтворюваності результатів
np.random.seed(42)
random.seed(82)

# Параметри аномалій
ANOMALY_CONFIG = {
    'enable_anomalies': True,     # Увімкнути/вимкнути аномалії
    'anomaly_ratio': 0.15,        # Відсоток аномальних записів у датасеті
    
    # Конфігурація типів аномалій
    'ddos_ratio': 0.4,            # Відсоток DDoS атак від усіх аномалій
    'misconfig_ratio': 0.2,       # Відсоток помилкових конфігурацій
    'nonstandard_port_ratio': 0.2, # Відсоток нестандартних портів
    'repeated_conn_ratio': 0.2,    # Відсоток надмірних повторюваних з'єднань
    
    # Параметри DDoS атаки
    'ddos_target_count': 5,       # Кількість цільових IP для DDoS
    'ddos_duration_minutes': 30,   # Тривалість однієї DDoS атаки в хвилинах
    'ddos_intensity': 'high',      # Інтенсивність: 'low', 'medium', 'high'
    
    # Параметри повторюваних з'єднань
    'repeat_conn_count': 50,       # Кількість повторень одного з'єднання
    'repeat_conn_interval_sec': 5, # Інтервал між повтореннями в секундах
}

# Кількість рядків у датасеті
n_rows = 23000

# Визначення категоріальних значень
protocols = ['TCP', 'UDP', 'ICMP', 'HTTP', 'HTTPS', 'DNS', 'FTP', 'SMTP', 'SSH']
services = ['http', 'https', 'dns', 'ftp', 'ssh', 'smtp', 'pop3', 'imap', 'telnet', 
            'ntp', 'dhcp', 'rdp', 'vnc', '-', 'other']
states = ['FIN', 'CON', 'INT', 'RST', 'ACC', 'REQ', 'CLO', 'EST', '-']

# Стандартні порти для сервісів
standard_ports = {
    'http': 80, 'https': 443, 'dns': 53, 'ftp': 21, 'ssh': 22, 'smtp': 25, 
    'pop3': 110, 'imap': 143, 'telnet': 23, 'ntp': 123, 'dhcp': 67, 'rdp': 3389, 'vnc': 5900
}

# Нестандартні порти для сервісів (для аномалій)
nonstandard_ports = {
    'http': [8080, 8888, 8008, 8081, 8000], 
    'https': [8443, 9443, 4443, 8444, 9444], 
    'dns': [5353, 9053, 8053], 
    'ftp': [2121, 3721, 4559], 
    'ssh': [2222, 2022, 922], 
    'smtp': [2525, 1025, 26, 366], 
    'pop3': [1110, 2110, 1109], 
    'imap': [1143, 2143, 993], 
    'telnet': [2323, 992], 
    'ntp': [1123, 1337], 
    'rdp': [3388, 13389], 
    'vnc': [5901, 5902, 5800]
}

# Генерація часу початку (випадково за останній місяць)
base_time = datetime.now() - timedelta(days=30)
start_times = [base_time + timedelta(seconds=random.randint(0, 30*24*60*60)) for _ in range(n_rows)]

# Створення базового DataFrame
df = pd.DataFrame({
    'id': range(1, n_rows + 1),
    'proto': np.random.choice(protocols, size=n_rows, p=[0.4, 0.3, 0.1, 0.05, 0.05, 0.04, 0.02, 0.02, 0.02]),
    'service': np.random.choice(services, size=n_rows, p=[0.25, 0.2, 0.15, 0.1, 0.1, 0.05, 0.05, 0.03, 0.02, 0.01, 0.01, 0.01, 0.01, 0.005, 0.005]),
    'state': np.random.choice(states, size=n_rows),
    'dur': np.random.exponential(10, n_rows),  # Тривалість у секундах (експоненційний розподіл)
    'spkts': np.random.randint(1, 1000, n_rows),  # Відправлені пакети
    'dpkts': np.random.randint(1, 1000, n_rows),  # Отримані пакети
    'sbytes': np.random.randint(100, 10000000, n_rows),  # Відправлені байти
    'dbytes': np.random.randint(100, 10000000, n_rows),  # Отримані байти
    'rate': np.random.exponential(1000, n_rows),  # Швидкість передачі даних (біти/с)
    'sttl': np.random.randint(30, 255, n_rows),  # Source TTL
    'dttl': np.random.randint(30, 255, n_rows),  # Destination TTL
    'sload': np.random.exponential(5, n_rows),  # Source load
    'dload': np.random.exponential(5, n_rows),  # Destination load
    'sloss': np.random.randint(0, 100, n_rows),  # Source loss (packets)
    'dloss': np.random.randint(0, 100, n_rows),  # Destination loss (packets)
    'sinpkt': np.random.exponential(0.01, n_rows),  # Source inter-packet arrival time
    'dinpkt': np.random.exponential(0.01, n_rows),  # Destination inter-packet arrival time
    'sjit': np.random.exponential(0.005, n_rows),  # Source jitter
    'djit': np.random.exponential(0.005, n_rows),  # Destination jitter
    'swin': np.random.randint(1000, 65535, n_rows),  # Source window size
    'dwin': np.random.randint(1000, 65535, n_rows),  # Destination window size
    'stcpb': np.random.randint(100000, 1000000000, n_rows),  # Source TCP base sequence number
    'dtcpb': np.random.randint(100000, 1000000000, n_rows),  # Destination TCP base sequence number
    'tcprtt': np.random.exponential(0.1, n_rows),  # TCP connection round trip time
    'synack': np.random.exponential(0.05, n_rows),  # TCP connection setup time
    'ackdat': np.random.exponential(0.05, n_rows),  # TCP connection setup time
    'is_attack': np.random.choice([0, 1], size=n_rows, p=[0.9, 0.1]),  # Флаг атаки (0-нормальний трафік, 1-атака)
})

# Генерація IP-адрес
def generate_random_ip():
    return f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

# Розширений список країн для IP-геолокації
countries = [
    'Україна', 'США', 'Німеччина', 'Польща', 'Франція', 'Китай', 'Велика Британія', 'Канада',
    'Індія', 'Японія', 'Австралія', 'Бразилія', 'Південна Корея', 'Італія', 'Іспанія', 
    'Нідерланди', 'Швеція', 'Сінгапур', 'Ізраїль', 'ОАЕ'
]

# Різні ваги для розподілу трафіку за країнами (додаємо високий трафік з Індії)
country_weights = [
    0.25, 0.12, 0.08, 0.06, 0.05, 0.05, 0.05, 0.04,  # Original countries with adjusted weights
    0.10, 0.03, 0.02, 0.03, 0.02, 0.02, 0.02,  # Added countries with various weights (Індія має високий трафік)
    0.02, 0.01, 0.01, 0.01, 0.01
]

# Make sure weights sum to 1.0
country_weights = [w/sum(country_weights) for w in country_weights]

# Додамо IP-адреси і країни до датафрейму
df['src_ip'] = [generate_random_ip() for _ in range(n_rows)]
df['dst_ip'] = [generate_random_ip() for _ in range(n_rows)]
df['src_country'] = np.random.choice(countries, size=n_rows, p=country_weights)
df['dst_country'] = np.random.choice(countries, size=n_rows, p=country_weights)

# Додаємо порти (стандартні для початку)
df['src_port'] = np.random.randint(1024, 65535, n_rows)  # Динамічні порти джерела
df['dst_port'] = df['service'].apply(lambda s: standard_ports.get(s, random.randint(1, 1023)))

# Define different weights for source and destination countries
src_country_weights = [
    0.3, 0.1, 0.05, 0.08, 0.05, 0.05, 0.03, 0.02,  # First 8 countries
    0.04, 0.03, 0.03, 0.03, 0.03, 0.03, 0.03,      # Next 7 countries  
    0.02, 0.02, 0.02, 0.02, 0.02                   # Last 5 countries
]

dst_country_weights = [
    0.15, 0.15, 0.1, 0.08, 0.08, 0.05, 0.04, 0.05, # First 8 countries
    0.04, 0.03, 0.03, 0.03, 0.03, 0.03, 0.03,      # Next 7 countries
    0.02, 0.02, 0.02, 0.01, 0.01                   # Last 5 countries
]

# Ensure weights sum to 1.0
src_country_weights = [w/sum(src_country_weights) for w in src_country_weights]
dst_country_weights = [w/sum(dst_country_weights) for w in dst_country_weights]

# Apply different distributions
df['src_country'] = np.random.choice(countries, size=n_rows, p=src_country_weights)
df['dst_country'] = np.random.choice(countries, size=n_rows, p=dst_country_weights)

# Create correlation between countries (e.g., Ukraine often communicates with Poland)
mask_ukraine = df['src_country'] == 'Україна'
poland_index = np.random.choice(np.where(mask_ukraine)[0], size=int(sum(mask_ukraine)*0.4))
df.loc[poland_index, 'dst_country'] = 'Польща'

# Add byte volume differences (more bytes sent from certain countries)
multiplier = df['src_country'].map({
    'Україна': 1.0, 'США': 1.5, 'Німеччина': 1.2, 'Польща': 0.8, 
    'Франція': 0.9, 'Китай': 1.7, 'Велика Британія': 1.3, 'Канада': 0.7
})
df['sbytes'] = df['sbytes'] * multiplier

# Додавання часових міток
df['start_time'] = start_times
df['end_time'] = [st + timedelta(seconds=dur) for st, dur in zip(start_times, df['dur'])]

# Замість генерації міток аномалій, створюємо записи з нетиповими значеннями
def generate_anomalies_without_labeling(df):
    # Кількість аномальних записів
    anomaly_count = int(len(df) * ANOMALY_CONFIG['anomaly_ratio'])
    
    # Вибираємо випадкові індекси для модифікації
    anomaly_indices = np.random.choice(df.index, size=anomaly_count, replace=False)
    
    # Розділяємо аномальні записи на категорії
    ddos_count = int(anomaly_count * ANOMALY_CONFIG['ddos_ratio'])
    misconfig_count = int(anomaly_count * ANOMALY_CONFIG['misconfig_ratio'])
    nonstandard_port_count = int(anomaly_count * ANOMALY_CONFIG['nonstandard_port_ratio'])
    
    # Індекси для різних типів аномалій
    ddos_indices = anomaly_indices[:ddos_count]
    misconfig_indices = anomaly_indices[ddos_count:ddos_count+misconfig_count]
    nonstandard_port_indices = anomaly_indices[ddos_count+misconfig_count:ddos_count+misconfig_count+nonstandard_port_count]
    
    # 1. DDoS-подібні записи - завищені показники пакетів та швидкості
    for idx in ddos_indices:
        # Висока швидкість пакетів
        df.at[idx, 'spkts'] = random.randint(3000, 10000)
        # Короткі з'єднання
        df.at[idx, 'dur'] = random.uniform(0.0001, 0.01)
        # Малі розміри пакетів
        df.at[idx, 'sbytes'] = df.at[idx, 'spkts'] * random.randint(1, 5)
        # Висока швидкість
        df.at[idx, 'rate'] = df.at[idx, 'spkts'] / df.at[idx, 'dur'] if df.at[idx, 'dur'] > 0 else 9999999

    # 2. Неправильні конфігурації
    for idx in misconfig_indices:
        misconfig_type = random.choice(['wrong_ttl', 'window_size', 'packet_size'])
        
        if misconfig_type == 'wrong_ttl':
            # Нетипові TTL значення
            df.at[idx, 'sttl'] = random.choice([1, 2, 255, 254])
            df.at[idx, 'dttl'] = random.choice([1, 2, 255, 254])
        
        elif misconfig_type == 'window_size':
            # Нетипові розміри вікна TCP
            df.at[idx, 'swin'] = random.choice([1, 2, 3, 65535, 65534])
            df.at[idx, 'dwin'] = random.choice([1, 2, 3, 65535, 65534])
            
        elif misconfig_type == 'packet_size':
            # Невідповідність пакетів і байтів
            df.at[idx, 'spkts'] = random.randint(500, 1000)
            df.at[idx, 'sbytes'] = random.randint(500, 1000)  # ~1 байт на пакет

    # 3. Нестандартні порти
    for idx in nonstandard_port_indices:
        service = df.at[idx, 'service']
        if service in nonstandard_ports and nonstandard_ports[service]:
            df.at[idx, 'dst_port'] = random.choice(nonstandard_ports[service])
        else:
            df.at[idx, 'dst_port'] = random.randint(10000, 65535)
    
    return df

# Використовуємо нову функцію замість старої
# df = generate_anomalies(df)  # Коментуємо стару функцію
df = generate_anomalies_without_labeling(df)  # Використовуємо нову функцію

# Видаляємо колонки anomaly та anomaly_type
if 'anomaly' in df.columns:
    df = df.drop('anomaly', axis=1)
if 'anomaly_type' in df.columns:
    df = df.drop('anomaly_type', axis=1)

# Генеруємо аномалії
df = generate_anomalies_without_labeling(df)

# Перетворення даних та обмеження для підвищення реалізму
# Округлення значень з плаваючою крапкою до 6 знаків після коми
for col in ['dur', 'rate', 'sload', 'dload', 'sinpkt', 'dinpkt', 'sjit', 'djit', 'tcprtt', 'synack', 'ackdat']:
    df[col] = df[col].round(6)

# Збереження даних в CSV
output_path = "data/dataset1.csv"
df.to_csv(output_path, index=False)

print(f"Створено датасет з {len(df)} рядками у файлі {output_path}")
print(f"Нормальні з'єднання: {(df['anomaly'] == 0).sum()}")
print(f"Аномальні з'єднання: {(df['anomaly'] == 1).sum()}")
print(f"Типи аномалій: {df[df['anomaly'] == 1]['anomaly_type'].value_counts().to_dict()}")
print(f"Колонки: {', '.join(df.columns)}")