import redis
import time
import csv
import os

r = redis.RedisCluster(host="localhost", port=7000, decode_responses=False)
url_counter = 'urls_processed'
url_queue = '{crawler}url_queue'

csv_file = 'metrics/urls_per_sec.csv'

total_time = 120

# Create CSV file with header if it doesn't exist
if not os.path.exists(csv_file):
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'urls_processed_last_5_sec', 'queue_length'])

code_start = time.time()

while True:
    if total_time > 0:
        if time.time() - code_start > total_time:
            break

    end_time = time.time()
    local = time.localtime()
    start_time = end_time - 5

    # Get the number of URLs processed in the last 5 seconds
    urls_processed = r.get(url_counter)
    r.set(url_counter, 0)  # Reset the counter
    urls_processed = int(urls_processed) if urls_processed else 0
    queue_len = r.llen(url_queue)

    # write to csv
    time_stamp = time.strftime("%Y-%m-%d %H:%M:%S", local)
    with open(csv_file, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([time_stamp, urls_processed, queue_len])
    
    # Sleep for 5 seconds
    time.sleep(5)
