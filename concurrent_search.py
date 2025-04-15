from concurrent.futures import ThreadPoolExecutor, as_completed
import ast
from collections import defaultdict
import time
from datetime import datetime
import os
import csv
from redis.cluster import RedisCluster
from pyspark.sql import SparkSession

num_concurrent = 100

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DistributedWebCrawler") \
    .getOrCreate()

def run_search(term):
    from search import ranked_search
    start = time.time()
    try:
        ranked_search(term, "inverted_index")
        end = time.time()
        return term, end - start, None
    except Exception as e:
        end = time.time()
        return term, end - start, e

# link of terms for concurrent search
search_terms = []
with open("search_terms.txt", "r") as file:
    for i in range(num_concurrent):
        line = file.readline()
        if not line:
            break
        search_terms.append(line.strip())

overall_start_time = time.perf_counter()

# make separate threads for each search term
results_list = []
with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
    future_to_term = {executor.submit(run_search, term): term for term in search_terms}
    
    # Wait for all futures to complete
    for i, future in enumerate(as_completed(future_to_term)):
        term = future_to_term[future]
        try:
            # Get the result tuple: (term, time_taken, error_msg)
            term_result, time_taken, error_msg = future.result()
            results_list.append({
                'term': term_result,
                'time_taken_s': time_taken,
                'error': error_msg
            })
            status = "FAILED" if error_msg else "OK"
            print(f"Completed task {i+1}/{len(search_terms)}: Term='{term_result}', Time={time_taken:.4f}s, Status={status}")
        except Exception as exc:
            # Catch errors *getting* the result (should be rare if run_search_task catches internal errors)
            print(f"FATAL ERROR processing result for term '{term}': {exc}")
            results_list.append({
                'term': term,
                'time_taken_s': time.perf_counter() - overall_start_time, # Approximate time until failure
                'error': f"Executor error: {type(exc).__name__}: {exc}"
            })


# write metrics to file
file_name = f"metrics/concurrent_search_{num_concurrent}.csv"
if not os.path.exists('metrics'):
    os.mkdir('metrics')
# Create CSV file with header if it doesn't exist
file_exists = os.path.isfile(file_name)
if not file_exists:
    with open(file_name, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['term', 'time_taken_s'])

# write the time taken for each term
with open(file_name, 'a', newline='') as f:
    writer = csv.writer(f)
    for result in results_list:
        writer.writerow([result['term'], result['time_taken_s']])
        