import pandas as pd
import ipaddress
from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

IP_DB_CSV = "IP2LOCATION-LITE-DB5.CSV"
INPUT_FILE = "ips.txt"
OUTPUT_FILE = "ip_location_results.csv"
BATCH_SIZE = 100_000 
MAX_WORKERS = 10  

print("🔹 Loading IP2Location database...")
cols = [
    "ip_from", "ip_to", "country_code", "country_name",
    "region_name", "city_name", "latitude", "longitude"
]
df = pd.read_csv(IP_DB_CSV, names=cols)
df.sort_values("ip_from", inplace=True)
df.reset_index(drop=True, inplace=True)
ip_from_list = df["ip_from"].tolist()
print(f"Loaded {len(df):,} IP ranges")

def ip_to_int(ip):
    try:
        return int(ipaddress.ip_address(ip))
    except Exception:
        return None

def lookup_ip(ip):
    ip_val = ip_to_int(ip)
    if ip_val is None:
        return {"ip": ip, "country": None, "region": None, "city": None, "latitude": None, "longitude": None}

    idx = bisect_left(ip_from_list, ip_val)
    if idx == 0:
        return {"ip": ip, "country": None, "region": None, "city": None, "latitude": None, "longitude": None}

    row = df.iloc[idx - 1]
    if row.ip_from <= ip_val <= row.ip_to:
        return {
            "ip": ip,
            "country": row.country_name,
            "region": row.region_name,
            "city": row.city_name,
            "latitude": row.latitude,
            "longitude": row.longitude
        }
    else:
        return {"ip": ip, "country": None, "region": None, "city": None, "latitude": None, "longitude": None}

def process_batch(ip_batch):
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(lookup_ip, ip) for ip in ip_batch]
        for future in as_completed(futures):
            results.append(future.result())
    return results

print("🔹 Reading IP list...")
with open(INPUT_FILE, "r") as f:
    ips = [line.strip() for line in f if line.strip()]

print(f"🔹 Total IPs: {len(ips):,}")
all_results = []

for i in range(0, len(ips), BATCH_SIZE):
    batch = ips[i:i + BATCH_SIZE]
    print(f"\nProcessing batch {i//BATCH_SIZE + 1} ({len(batch):,} IPs)...")
    batch_results = process_batch(batch)
    all_results.extend(batch_results)

    pd.DataFrame(batch_results).to_csv(
        OUTPUT_FILE, mode='a', index=False,
        header=(i == 0) 
    )

print(f"\nDone! All results saved to {OUTPUT_FILE}")
