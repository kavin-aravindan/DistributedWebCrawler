#!/bin/bash

trap 'kill $(jobs -p)' SIGINT SIGTERM EXIT

source ../proj_venv/bin/activate

for i in {1..100}; do
    echo "Starting worker $i"
    python3 redis_test_worker.py &
    sleep 0.5
done

echo "All workers started. Press Ctrl+C to stop."
wait
echo "All workers stopped."
