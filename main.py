import logging
import os
import json
import time
import shutil
from os.path import splitext

import mobi
from unstructured.partition.auto import partition
from mpire import WorkerPool
from unstructured.staging.base import elements_to_json
from unstructured.cleaners.core import clean_non_ascii_chars, clean_extra_whitespace, group_broken_paragraphs
from unstructured.documents.elements import NarrativeText
from unstructured.documents.elements import Title

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
        conv = str(obj)
    except TypeError:
        conv = "<Could not convert>"
        
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
    def convert_mobi(mobi_file_path):
        tempdir, filepath = (None, None)
        try:
            tempdir, filepath = mobi.extract(mobi_file_path)
        except Exception as e:
            print(f"Problem converting file {mobi_file_path}: {e.__str__()}")
            if tempdir is not None and os.path.exists(tempdir):
                shutil.rmtree(tempdir)
            return False
        else:
            (head, tail) = os.path.split(mobi_file_path)
            (root, oext) = os.path.splitext(tail)

            (nhead, ntail) = os.path.split(filepath)
            (nroot, next) = os.path.splitext(ntail)

            new_file = head+"/"+root + next
            shutil.move(filepath, new_file)
            shutil.rmtree(tempdir)
            return new_file

    def begin_extract(self):

        print("Looking for files that need conversion...")
        for file in self.files:
            ext = splitext(file)[-1].upper()
            if ext in [".MOBI", ".PRC", ".AZW", ".AZW3", ".AZW4"]:
                print(f"Converting {file} to an epub format...")
                result = BulkTextExtract.convert_mobi(file)

                if result is not False:
                    print(f"Success: {result}")
                    self.files[self.files.index(file)] = result

        print("Spawning pool and beginning... This will take quite some time.")
        with WorkerPool(n_jobs=self.max_num_threads) as self.thread_pool:
            self.running_pool = True
            to_do = self.files[self.progress_index:]
            self.thread_pool.map_unordered(BulkTextExtract.textExtractor, to_do, progress_bar=True)

        self.complete_progress()


    @staticmethod
    def textExtractor(file):

        global DEBUG
        print(f"\nExtracting text from {file}")
        if DEBUG == True:
            time.sleep(1)
            elements = ["test","test"]
        else:
            try:
                elements = partition(filename=file, **BulkTextExtract.unstructured_settings)
                for element in elements:
                    if isinstance(element, (NarrativeText, Title)):
                        element.apply(clean_non_ascii_chars, clean_extra_whitespace, group_broken_paragraphs)

            except OSError as e:
                elements = ["Error", f"Failed to partition {file}"]
                print(f"There was a problem partitioning {file}. Logging information.")
                dbg(f"OSError logging {file}", e)
        try:

            dir = os.path.dirname(file)
            document_name = os.path.splitext(os.path.basename(file))[0].replace(".", "_")
            segment_dir = os.path.join(dir, document_name)
            os.makedirs(segment_dir, exist_ok=True)  
            segment_filename = f"{document_name}_raw.json"
            segment_filepath = os.path.join(segment_dir, segment_filename)


            elements_to_json(elements, filename=segment_filepath, indent=4)
            print(f"Saved {len(elements)} segments.")
            
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

        if not self.directory:
            print("Couldn't find directory. Exiting")
            return

        self.progress_file = self.directory+"/progress.json"
        self.max_num_threads = 6
        self.thread_pool = None



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