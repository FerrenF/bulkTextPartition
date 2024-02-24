import io
import multiprocessing
import os
import json
import signal
import time

from unstructured.partition.auto import partition

DEBUG = False
def validate_directory(directory):
    if os.path.exists(directory):
        return directory

    tests = [os.path.dirname(__file__) + "/" + directory, os.path.dirname(__file__) + directory, "./"+directory]
    for test in tests:
        if os.path.exists(test):
            return test
    return False

class BulkTextExtract:

    unstructured_settings = {
        "include_page_breaks" : True,
        "strategy" : 'hi_res',
        "chunking_strategy" : "by_title"
    }
    file_types_of_interest = ("pdf", "mobi", "epub", "djvu")

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

    def save_progress(self):
        """Saves progress information to a JSON file."""
        data = {"files": self.files, "progress_index": self.progress_index}
        with open(self.progress_file, "w") as f:
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

    def begin_extract(self):
        print("Spawning pool and beginning... This will take quite some time.")
        self.thread_pool.starmap(BulkTextExtract.textExtractor, enumerate(self.files[self.progress_index:]))
        self.thread_pool.close()  # Signal end of tasks (normal completion)
        self.thread_pool.join()  # Wait for completion
        self.complete_progress()

    @staticmethod
    def textExtractor(index, file):
        """Extracts text from a file. Replace this with your actual implementation."""
        global DEBUG
        print(f"Extracting text from {file}")
        if DEBUG == True:
            time.sleep(1)
            part = ["test","test"]
        else:
            part = partition(filename=file, **BulkTextExtract.unstructured_settings)

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

    def signal_handler(self, signal_number, frame):
        print("\nReceived signal, terminating threads...")
        self.thread_pool.terminate()  # Terminate threads immediately
        self.thread_pool.join()  # Wait for termination to complete

    def thread_response_count_complete(self):
        self.progress_index += 1
        print(f"Finished conversion. Done: {self.progress_index}")

    def __init__(self, directory):

        self.directory = validate_directory(directory)
        self.progress_file = self.directory+"/progress.json"
        self.max_num_threads = 10
        self.thread_pool = multiprocessing.Pool(processes=self.max_num_threads)

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
            print("\nExiting program...")
            self.save_progress()  # Save progress when interrupted
            self.thread_pool.terminate()
            self.thread_pool.join()

if __name__ == "__main__":
    directory = input("Enter the directory to scan: ")
    app = BulkTextExtract(directory)