from flask import Flask, request, render_template, jsonify
from search import ranked_search
from functools import lru_cache
import json

import socket

def convert_sets_to_lists(obj):
    """Recursively converts sets to lists within an object."""
    if isinstance(obj, set):
        return sorted(list(obj)) 
    elif isinstance(obj, list):
        return [convert_sets_to_lists(item) for item in obj]
    elif isinstance(obj, tuple):
        return [convert_sets_to_lists(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_sets_to_lists(value) for key, value in obj.items()}
    else:
        return obj

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

app = Flask(__name__)

# LRU cache for recent searches (can hold 20 items)
@lru_cache(maxsize=20)
def cached_search(query):
    print(f"Cache miss for: {query}")
    exact, partial = ranked_search(query, "inverted_index")
    return exact, partial

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/search", methods=["POST"])
def search():
    data = request.get_json()
    query = data.get("query", "").strip()

    if not query:
        return jsonify({"error": "Empty query"}), 400

    if cached_search.cache_info().hits > 0:
        print(f"Cache hit for: {query}")

    exact, partial = cached_search(query)

    exact_serializable = convert_sets_to_lists(exact)
    partial_serializable = convert_sets_to_lists(partial)

    if not exact_serializable and not partial_serializable:
        return jsonify({"exact": [], "partial": [], "message": "No results found"})

    return jsonify({
        "exact": exact_serializable[:10], 
        "partial": partial_serializable[:10],
        "more_exact": exact_serializable[10:],
        "more_partial": partial_serializable[10:]
    })

# def dummy_search():
#     data = request.get_json()
#     query = data.get("query", "").strip()

#     if not query:
#         return jsonify({"error": "Empty query"}), 400

#     # Load mock output from file
#     with open("mock_output.json", "r") as f:
#         mock_data = json.load(f)

#     exact = mock_data.get("exact", [])
#     partial = mock_data.get("partial", [])

#     if not exact and not partial:
#         return jsonify({"exact": [], "partial": [], "message": "No results found"})

#     return jsonify({
#         "exact": exact[:10],
#         "partial": partial[:10],
#         "more_exact": exact[10:],
#         "more_partial": partial[10:]
#     })


if __name__ == "__main__":
    port = find_free_port()
    print(f"Running on port {port}")
    app.run(debug=True, port=port)
