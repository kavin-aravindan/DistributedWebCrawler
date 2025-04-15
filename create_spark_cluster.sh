#!/bin/bash

function kill_master_workers() {
  echo "Stopping Spark master and workers..."
  ~/spark/sbin/stop-master.sh
  ~/spark/sbin/stop-worker.sh
  exit
}

trap 'kill_master_workers' SIGINT SIGTERM EXIT
# Path to Spark home directory - replace with your actual path
SPARK_HOME=~/spark

source ../proj_venv/bin/activate

# Path to your Python script - replace with your actual path
PYTHON_SCRIPT="spark_inverted_index.py"

# Function to check if master is already running
is_master_running() {
  pgrep -f "org.apache.spark.deploy.master.Master" > /dev/null
  return $?
}

# Function to start the master if not already running
start_master() {
  if ! is_master_running; then
    echo "Starting Spark master..."
    $SPARK_HOME/sbin/start-master.sh
    # Wait for master to start properly
    sleep 5
  else
    echo "Spark master is already running."
  fi
}

# Function to start multiple workers
start_workers() {
  $SPARK_HOME/sbin/start-worker.sh spark://127.0.0.1:7077
  sleep 4
}

# Function to run the Python script
run_python_script() {
  echo "Running Python script: $PYTHON_SCRIPT"
  python3 $PYTHON_SCRIPT
}

# Main script execution
echo "Setting up Spark cluster and script runner..."

# Start the master
start_master

# Start the workers
start_workers

# # Run the Python script every minute
echo "Starting schedule to run script every minute..."
while true; do
  run_python_script
  echo "Waiting for next execution..."
  sleep 20
done

# ~/spark/sbin/stop-master.sh
# ~/spark/sbin/stop-worker.sh