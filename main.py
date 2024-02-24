import io
import logging
import multiprocessing
import os
import json
import signal
import time

import mpire
from unstructured.partition.auto import partition
from mpire import WorkerPool
from mpire.utils import make_single_arguments

DEBUG = False
def validate_directory(directory):
    if os.path.exists(directory):
        return directory

    tests = [os.path.dirname(__file__) + "/" + directory, os.path.dirname(__file__) + directory, "./"+directory]
    for test in tests:
        if os.path.exists(test):
            return test
    return False


dbg_file = "debug.log"
def dbg(msg, obj, alarm: int = 0):
    conv = ""
    try:
        conv=str(obj)
    except TypeError:
        conv="<Could not convert>"
    print(f"DEBUG: {msg} --- attached object {conv}")

    if alarm == 1:
        logging.warning(msg, obj)

class BulkTextExtract:

    extract_timeout = (60*60)*2 # 2 hours
    unstructured_settings = {
        "include_page_breaks" : True,
        "strategy" : 'fast',
        "chunking_strategy" : "by_title"
    }
    file_types_of_interest = ("pdf", "mobi", "epub")

    def find_files(self):
        """Scans a directory and its subdirectories for files of specified types."""
        files = set()
        for root, _, filenames in os.walk(directory):
            for filename in filenames:
                if filename.lower().endswith(BulkTextExtract.file_types_of_interest):
                    subj = os.path.join(root, filename)
                    files.add(subj)
        self.files = list(files.union(self.files))
        return files

    def complete_progress(self):
        if os.path.exists(self.progress_file):
            os.remove(self.progress_file)
            print("Removed old progress file.")
        self.running_pool = False


    @staticmethod
    def save_progress(file, files, progress_index):
        """Saves progress information to a JSON file."""
        data = {"files": files, "progress_index": progress_index}
        with open(file, "w") as f:
            json.dump(data, f)

    def attempt_load_progress(self):
        try:
            with open(self.progress_file, "r") as f:
                data = json.load(f)
                self.files = data.get("files")
                self.progress_index = data.get("progress_index")
        except FileNotFoundError:
            self.files = None
            self.progress_index = 0


    @staticmethod
    def finish_thread(progress):
        progress[0] += 1
        BulkTextExtract.save_progress(progress[2], progress[1], progress[0])

    def begin_extract(self):
        print("Spawning pool and beginning... This will take quite some time.")
        with WorkerPool(n_jobs=self.max_num_threads, shared_objects=(self.progress_index, self.files, self.progress_file)) as self.thread_pool:
            self.running_pool = True
            to_do = self.files[self.progress_index:]
            results = self.thread_pool.map_unordered(BulkTextExtract.textExtractor, make_single_arguments(to_do, generator=False), progress_bar=True, worker_exit=BulkTextExtract.finish_thread)

        self.complete_progress()


    @staticmethod
    def textExtractor(index, file):

        global DEBUG
        print(f"\nExtracting text from {file}")
        if DEBUG == True:
            time.sleep(1)
            part = ["test","test"]
        else:
            try:
                part = partition(filename=file, **BulkTextExtract.unstructured_settings)
            except OSError as e:
                part = ["Error", f"Failed to partition {file}"]
                print(f"There was a problem partitioning {file}. Logging information.")
                dbg(f"OSError logging {file}", e)
        try:

            directory = os.path.dirname(file)
            # Create a cleaned-up document name for directory and file naming
            document_name = os.path.splitext(os.path.basename(file))[0].replace(".", "_")

            # Create the directory for storing segments
            segment_dir = os.path.join(directory, document_name)
            os.makedirs(segment_dir, exist_ok=True)  # Create if it doesn't exist

            for segment_index, segment in enumerate(part):
                segment_filename = f"{document_name}_{segment_index}.json"
                segment_filepath = os.path.join(segment_dir, segment_filename)
                with open(segment_filepath, "w") as f:
                    json.dump(segment, f, indent=4)  # Indent for readability

            print(f"Saved {len(part)} segments.")
        except (ValueError or IOError) as e:
            dbg("Failed to save segments. Logging details. Skipping to next file.",e ,1)

    def signal_handler(self, signal_number, frame):
        print("\nReceived signal, terminating threads...")
        self.thread_pool.terminate()  # Terminate threads immediately
        self.thread_pool.join()  # Wait for termination to complete

    def thread_response_count_complete(self):
        self.progress_index += 1
        print(f"Finished conversion. Done: {self.progress_index}")

    def __init__(self, directory):
        self.running_pool = False
        self.directory = validate_directory(directory)
        self.progress_file = self.directory+"/progress.json"
        self.max_num_threads = 6
        self.thread_pool = None

        if directory == False:
            print("Couldn't find directory. Exiting")
            return

        self.progress_index = 0
        try:
            print("Looking for previous session in directory.")
            self.attempt_load_progress()
            if self.files is None:
                self.files = list()
                print("Didn't find anything to resume. Let's scan.")
                files_found = self.find_files()
                print(f"Found {len(files_found)} applicable files.")
            else:
                input("Found a progress file. Continue?")
                response = input("Begin conversion?").lower()
                if response != 'y':
                    print("Scanning for new list.")
                    files_found = self.find_files()
                    print(f"Found {len(files_found)} applicable files.")

            response = input("Begin conversion?").lower()
            if response != 'y':
                print("Response wasn't y. Breaking.")
                return

            self.begin_extract()

        except KeyboardInterrupt:  # Handle Ctrl+C
            if self.thread_pool is not None:
                self.thread_pool.terminate()
            print("\nExiting program...")
            self.save_progress()



if __name__ == "__main__":
    logging.basicConfig(filename=dbg_file, filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    directory = input("Enter the directory to scan: ")
    app = BulkTextExtract(directory)