from concurrent import futures
import grpc
import os
import time
import threading
import mapreduce_pb2
import mapreduce_pb2_grpc

class Master(_pb2_grpc.MasterServiceServicer):
    def __init__(self, input_file, num_reducers, task):
        self.map_tasks = []
        self.reduce_tasks = []
        self.results = {}
        self.output_files = []
        self.job_completed = False
        self.workers_notified = 0
        self.task = task
        self.lock = threading.Lock()
        
        # Split the input file and prepare map tasks
        self.num_mappers = self.split_input_file(input_file)
        self.num_reducers = num_reducers
        self.total_tasks = self.num_mappers + self.num_reducers
        
        # Prepare all map tasks
        for i in range(self.num_mappers):
            map_task = mapreduce_pb2.TaskResponse(
                mapTask=mapreduce_pb2.MapTaskResponse(
                    InputFile=f"partition_{i}.txt",
                    MapperId=i,
                    NumReduce=num_reducers,
                    Task=self.task
                )
            )
            self.map_tasks.append(map_task)
        
        # Start a thread to enqueue reduce tasks after map tasks are completed
        threading.Thread(target=self.monitor_map_completion).start()

    def split_input_file(self, input_file):
        """Split the input file into partitions based on newlines"""
        try:
            with open(input_file, 'r') as f:
                lines = f.readlines()
            
            # Determine number of partitions
            num_partitions = min(len(lines), 10)  # Limit to 10 partitions max
            lines_per_partition = max(1, len(lines) // num_partitions)
            
            for i in range(num_partitions):
                start_idx = i * lines_per_partition
                end_idx = (i + 1) * lines_per_partition if i < num_partitions - 1 else len(lines)
                
                with open(f"partition_{i}.txt", 'w') as f:
                    f.writelines(lines[start_idx:end_idx])
            
            return num_partitions
        except Exception as e:
            print(f"Error splitting input file: {e}")
            # If there's an error, create at least one partition
            with open("partition_0.txt", 'w') as f:
                f.write("")
            return 1

    def monitor_map_completion(self):
        """Monitor for map task completion and enqueue reduce tasks when ready"""
        while True:
            with self.lock:
                map_results_count = sum(1 for task_type, _ in self.results.keys() if task_type == "map")
                if map_results_count == self.num_mappers and not self.reduce_tasks:
                    # All map tasks are done, create reduce tasks
                    for r in range(self.num_reducers):
                        reduce_task = mapreduce_pb2.TaskResponse(
                            reduceTask=mapreduce_pb2.ReduceTaskResponse(
                                ReducerId=r,
                                NumMappers=self.num_mappers,
                                Task=self.task
                            )
                        )
                        self.reduce_tasks.append(reduce_task)
                    print(f"All map tasks completed. Created {self.num_reducers} reduce tasks.")
                    break
            time.sleep(1)

    def AssignTask(self, request, context):
        print("Assigning task...")
        with self.lock:
            if self.map_tasks:
                # Prioritize map tasks
                task = self.map_tasks.pop(0)
                map_task = task.mapTask
                print(f"Assigned map task for input: {map_task.InputFile}")
                return task
            elif self.reduce_tasks:
                # Then reduce tasks
                task = self.reduce_tasks.pop(0)
                reduce_task = task.reduceTask
                print(f"Assigned reduce task for reducer: {reduce_task.ReducerId}")
                return task
            elif self.job_completed:
                # Send done signal when job is complete
                self.workers_notified += 1
                return mapreduce_pb2.TaskResponse(done=True)
            else:
                # No tasks available yet, but job is not complete
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("no tasks available")
                return mapreduce_pb2.TaskResponse()

    def SubmitResult(self, request, context):
        with self.lock:
            task_type = request.TaskType
            task_id = request.TaskId
            output_file = request.OutputFile
            success = request.Success
            
            print(f"Received {task_type} result from task {task_id}: {output_file} (success: {success})")
            
            if success:
                self.results[(task_type, task_id)] = output_file
                if task_type == "reduce":
                    self.output_files.append(output_file)
            
            # Check if all tasks are completed
            if len(self.results) == self.total_tasks:
                self.job_completed = True
                print("All tasks completed. Job is done.")
                self.combine_outputs()
        
        return mapreduce_pb2.Empty()
    
    def combine_outputs(self):
        """Combine all reducer outputs into a final output file"""
        try:
            with open("final_output.txt", "w") as final_out:
                for output_file in self.output_files:
                    if os.path.exists(output_file):
                        with open(output_file, "r") as f:
                            final_out.write(f.read())
            print("Combined all outputs into final_output.txt")
        except Exception as e:
            print(f"Error combining outputs: {e}")

def start_master(input_file, num_reducers, task):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master = Master(input_file, num_reducers, task)
    mapreduce_pb2_grpc.add_MasterServiceServicer_to_server(master, server)
    server.add_insecure_port("[::]:50051")
    print(f"Master is running on port 50051 with {num_reducers} reducers")
    server.start()

    try:
        # Wait until job completion and all workers have been notified
        while not master.job_completed or master.workers_notified < master.total_tasks:
            time.sleep(2)
        print("All workers notified. Shutting down master.")
    finally:
        server.stop(0)
        print("Master: Server shutdown complete.")

class Worker:
    def __init__(self):
        self.channel = grpc.insecure_channel("localhost:50051")
        self.stub = mapreduce_pb2_grpc.MasterServiceStub(self.channel)
    
    def start(self):
        """Start the worker process"""
        worker_id = os.getpid()
        print(f"Worker {worker_id} started")
        
        while True:
            try:
                # Request a task from the master
                task_response = self.stub.AssignTask(mapreduce_pb2.Empty())
                
                # Check if we received a "done" signal
                if task_response.HasField("done"):
                    print(f"Worker {worker_id}: No more tasks, exiting.")
                    break
                
                # Process the task based on its type
                if task_response.HasField("mapTask"):
                    self.process_map_task(task_response.mapTask)
                elif task_response.HasField("reduceTask"):
                    self.process_reduce_task(task_response.reduceTask)
                
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE and "no tasks available" in e.details():
                    print(f"Worker {worker_id}: No tasks available, waiting...")
                    time.sleep(2)
                    continue
                else:
                    print(f"Worker {worker_id}: RPC error: {e.code()}, {e.details()}")
                    time.sleep(2)
                    break
            except Exception as e:
                print(f"Worker {worker_id}: Error: {e}")
                time.sleep(2)
                continue
        
        print(f"Worker {worker_id}: Exiting.")
    
    def process_map_task(self, map_task):
        """Process a map task"""
        try:
            mapper_id = map_task.MapperId
            input_file = map_task.InputFile
            num_reduce = map_task.NumReduce
            task = map_task.Task
            
            print(f"Processing map task {mapper_id} with input {input_file}")
            
            # Call the map function (to be implemented by users)
            output_files = self.map_function(input_file, mapper_id, num_reduce, task)
            
            # Report completion to the master
            result = mapreduce_pb2.ResultRequest(
                TaskType="map",
                TaskId=mapper_id,
                OutputFile=",".join(output_files),
                Success=True
            )
            self.stub.SubmitResult(result)
            
        except Exception as e:
            print(f"Error in map task {map_task.MapperId}: {e}")
            # Report failure
            result = mapreduce_pb2.ResultRequest(
                TaskType="map",
                TaskId=map_task.MapperId,
                OutputFile="",
                Success=False
            )
            self.stub.SubmitResult(result)
    
    def process_reduce_task(self, reduce_task):
        """Process a reduce task"""
        try:
            reducer_id = reduce_task.ReducerId
            num_mappers = reduce_task.NumMappers
            task = reduce_task.Task
            
            print(f"Processing reduce task {reducer_id}")
            
            # Call the reduce function (to be implemented by users)
            output_file = self.reduce_function(reducer_id, num_mappers, task)
            
            # Report completion to the master
            result = mapreduce_pb2.ResultRequest(
                TaskType="reduce",
                TaskId=reducer_id,
                OutputFile=output_file,
                Success=True
            )
            self.stub.SubmitResult(result)
            
        except Exception as e:
            print(f"Error in reduce task {reduce_task.ReducerId}: {e}")
            # Report failure
            result = mapreduce_pb2.ResultRequest(
                TaskType="reduce",
                TaskId=reduce_task.ReducerId,
                OutputFile="",
                Success=False
            )
            self.stub.SubmitResult(result)
    
    def map_function(self, input_file, mapper_id, num_reducers, task="wc"):
        """
        Map function to be implemented by users.
        Default implementation is word count.
        """
        try:
            # Read input file
            with open(input_file, "r") as f:
                content = f.read()
            
            # Split into words
            words = content.split()
            
            # Initialize partitions for each reducer
            partitions = {i: [] for i in range(num_reducers)}
            
            # Distribute words to partitions
            for word in words:
                bucket = self.get_partition(word, num_reducers)
                partitions[bucket].append(word)
            
            # Write intermediate files
            output_files = []
            for reducer_id, words in partitions.items():
                filename = f"map-{mapper_id}-reduce-{reducer_id}.txt"
                output_files.append(filename)
                with open(filename, "w") as f:
                    for word in words:
                        if task == "wc":
                            key_val = f"{word} 1\n"
                        elif task == "invert":
                            key_val = f"{word} {input_file}\n"
                        f.write(key_val)
            
            return output_files
        except Exception as e:
            print(f"Error in map_function: {e}")
            raise e
    
    def reduce_function(self, reducer_id, num_mappers, task="wc"):
        """
        Reduce function to be implemented by users.
        Default implementation is word count.
        """
        if task == "wc":
            try:
                word_counts = {}
                
                # Read all intermediate files for this reducer
                for mapper_id in range(num_mappers):
                    filename = f"map-{mapper_id}-reduce-{reducer_id}.txt"
                    if os.path.exists(filename):
                        with open(filename, "r") as f:
                            for line in f:
                                if line.strip():
                                    parts = line.strip().split()
                                    if len(parts) >= 2:
                                        word, count = parts[0], int(parts[1])
                                        word_counts[word] = word_counts.get(word, 0) + count
                
                # Write output file
                output_file = f"reduce_output_{reducer_id}.txt"
                with open(output_file, "w") as f:
                    for word, count in word_counts.items():
                        f.write(f"{word} {count}\n")
                
                return output_file
            except Exception as e:
                print(f"Error in reduce_function: {e}")
                raise e
        

        elif task == "invert":
            try:
                inverted_index = {}

                # Read all intermediate files for this reducer
                for mapper_id in range(num_mappers):
                    filename = f"map-{mapper_id}-reduce-{reducer_id}.txt"
                    if os.path.exists(filename):
                        with open(filename, "r") as f:
                            for line in f:
                                if line.strip():
                                    parts = line.strip().split()
                                    if len(parts) >= 2:
                                        word = parts[0]
                                        doc_id = parts[1]

                                        # Add the document ID to the word's list (avoid duplicates)
                                        if word not in inverted_index:
                                            inverted_index[word] = set()
                                        inverted_index[word].add(doc_id)

                # Write output file
                output_file = f"reduce_output_{reducer_id}.txt"
                with open(output_file, "w") as f:
                    for word, doc_ids in inverted_index.items():
                        doc_list = ",".join(sorted(doc_ids))
                        f.write(f"{word} {doc_list}\n")

                return output_file

            except Exception as e:
                print(f"Error in reduce_function: {e}")
                raise e

    
    def get_partition(self, key, num_buckets):
        """Determine which partition a key belongs to"""
        return sum(ord(c) for c in key) % num_buckets

def start_worker():
    worker = Worker()
    worker.start()
        
def calculate_mappers(input_file):
    """Split the input file into partitions based on newlines"""
    with open(input_file, 'r') as f:
        lines = f.readlines()
    
    num_partitions = min(len(lines), 10)  # Limit to 10 partitions max

    return num_partitions
        
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "worker":
        start_worker()
    else:
        if len(sys.argv) != 4:
            print("Usage: python q2.py <input_file> <num_reducers> <task>")
            sys.exit(1)
        
        input_file = sys.argv[1]
        num_reducers = int(sys.argv[2])
        task = sys.argv[3]
        num_mappers = calculate_mappers(input_file)

        for _ in range(num_mappers + num_reducers):
            os.system(f"python3 {sys.argv[0]} worker &")

        start_master(input_file, num_reducers, task)
        # Display final results
        print("\nMapReduce job completed!")
        print("Results can be found in final_output.txt")
        
        try:
            with open("final_output.txt", "r") as f:
                lines = f.readlines()
                print(f"Total entries in output: {len(lines)}")
                if len(lines) > 0:
                    print("Sample output (first 5 entries):")
                    for line in lines[:5]:
                        print(f"  {line.strip()}")
        except Exception as e:
            print(f"Error reading output file: {e}")

