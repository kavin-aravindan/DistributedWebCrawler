from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from redis_common import add_url, get_url
import re
import ast  
from collections import defaultdict
import time

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DistributedWebCrawler") \
    .getOrCreate()
sc = spark.sparkContext

def preprocess_and_tokenize(content):
    # Tokenization and preprocessing logic
    if not content:
        return []
    
    # take to lower case
    content = content.lower()
    
    # use regex to clean 
    content = re.sub("' "," ",content)
    content = re.sub('"',"",content)
    content = re.sub("^'"," ",content)
    content = re.sub('^"',"",content)
    content = re.sub("[“.?\n!@#$%&()*:;/,<>-_+=”]"," ",content)
    content = re.sub(" +"," ",content)
    
    tokens = re.findall(r'\b\w+\b', content)
    return tokens

def search(search_term, inverted_index_file):
    # Load index
    index = spark.sparkContext.textFile(inverted_index_file)
    matches = index.filter(lambda x: search_term in x).collect()

    for line in matches:
        print(line)

def ranked_search(search_term, inverted_index_file):
    # preprocess the search term
    search_tokens = preprocess_and_tokenize(search_term)
    search_tokens = list(set(search_tokens))
    print(f"Search tokens: {search_tokens}")
    
    ii_rdd = spark.sparkContext.textFile(inverted_index_file)
    parsed_rdd = ii_rdd.map(ast.literal_eval).filter(lambda x: x is not None)
    
    # pick only matches
    relevant_entries_rdd = parsed_rdd.filter(lambda x: x[0] in search_tokens)
    
    # collect in distributed fashion
    collected_entries = relevant_entries_rdd.collect()
    
    # now generate scores for each url (ie we need to add the score for each token, url pair)
    url_scores = defaultdict(lambda : {'score': 0, 'tokens': set()})
    
    for term, url_score_list in collected_entries:
        if term in search_tokens:
            for url, score in url_score_list:
                url_scores[url]['score'] += score
                url_scores[url]['tokens'].add(term)
            
    final_results_exact = []
    final_results_partial = []
    
    # first add results with all search tokens
    for url, data in url_scores.items():
        if len(data['tokens']) == len(search_tokens):
            final_results_exact.append((url, data['score'], data['tokens']))
    # sort by score
    final_results_exact.sort(key=lambda x: x[1], reverse=True)
    
    # do the same with partial matches
    for url, data in url_scores.items():
        if len(data['tokens']) < len(search_tokens):
            final_results_partial.append((url, data['score'], data['tokens']))
    # sort by score
    final_results_partial.sort(key=lambda x: x[1], reverse=True)
   
    return final_results_exact, final_results_partial


if __name__ == "__main__":
    while True:
        search_term = input("Enter a search term (or 'exit' to quit): ")
        if search_term.lower() == "exit":
            break
        start_time = time.time()
        final_results_exact, final_results_partial = ranked_search(search_term, "inverted_index")
        print(f"Search completed in {time.time() - start_time} seconds")
        # print results
        i = 0
        print("Exact matches:")
        for url, score, tokens in final_results_exact:
            i += 1
            print(f"URL: {url}, Score: {score}, Tokens: {tokens}")
            if i > 10:
                break
        i = 0
        print("\nPartial matches:")
        for url, score, tokens in final_results_partial:
            i += 1
            print(f"URL: {url}, Score: {score}, Tokens: {tokens}")
            if i > 10:
                break
        print("\n")
    