from faker import Faker
import pandas as pd
import random
from datetime import date
import pyarrow

fake = Faker()

num_companies = 100  # You can adjust this
start_date = date(2024, 1, 1)
end_date = date(2024, 12, 31)
num_dates = (end_date - start_date).days + 1 # Calculate total number of days


rows_per_chunk = 1000000  # Adjust this based on your memory capacity
total_rows_needed = 69591100 # Approximate number of rows for 100GB (from previous estimate)
num_chunks = (total_rows_needed + rows_per_chunk - 1) // rows_per_chunk

output_file = "stock_market_data.parquet"
file_exists = False # Flag to handle writing header only once

for chunk in range(num_chunks):
    print(f"Generating chunk {chunk + 1} of {num_chunks}...")
    data = []
    companies = [fake.company() for _ in range(num_companies)]
    for i in range(rows_per_chunk):
        company = random.choice(companies)
        current_date = fake.date_between(start_date=start_date, end_date=end_date)
        open_price = round(random.uniform(50, 2000), 2)
        price_change = round(random.uniform(-50, 50), 2)
        close_price = round(max(0.01, open_price + price_change), 2)
        high_price = round(max(open_price, close_price, open_price + random.uniform(0, 20)), 2)
        low_price = round(min(open_price, close_price, open_price - random.uniform(0, 20)), 2)
        volume = random.randint(1000, 1000000)
        data.append([company, current_date, open_price, close_price, high_price, low_price, volume])

    df_chunk = pd.DataFrame(data, columns=['Company Name', 'Date', 'Open', 'Close', 'High', 'Low', 'Volume'])

print(f"Finished generating approximately 100GB of stock market data in '{output_file}'.")