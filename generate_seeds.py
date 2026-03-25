import os
import pandas as pd
from faker import Faker

print("Initializing seed generation...")
os.makedirs('seeds', exist_ok=True)
fake = Faker('en_US')
Faker.seed(42)

print("Generating seeds/names.csv (10,000 rows)...")
names_data = []
for _ in range(10000):
    names_data.append({
        'first_name': fake.first_name(),
        'last_name': fake.last_name()
    })
pd.DataFrame(names_data).drop_duplicates().to_csv('seeds/names.csv', index=False)

print("Generating seeds/world_cities.csv (10,000 rows)...")
cities_data = []
for _ in range(10000):
    cities_data.append({
        'city': fake.city(),
        'state': fake.state_abbr(),
        'country': 'United States',
        'zip_code': fake.zipcode()
    })
pd.DataFrame(cities_data).drop_duplicates().to_csv('seeds/world_cities.csv', index=False)

print("Seed generation complete.")
