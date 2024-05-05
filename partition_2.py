import abc
import io
import logging
import os
import json
import subprocess
import threading
import time
import shutil
from abc import abstractmethod

from typing import Tuple, List

from pypandoc.pandoc_download import download_pandoc

ppd_install_path = os.path.realpath("./pypandoc/pandoc.exe")
if not os.path.exists(ppd_install_path):
    print("Downloading pypandoc...")
    download_pandoc(targetfolder=ppd_install_path, delete_installer=True)
os.environ['PYPANDOC_PANDOC'] = ppd_install_path

# NTLK DownloadDir Config
nltk_install_path = os.path.abspath("./nltk_data")
os.environ['NLTK_DATA'] = nltk_install_path

import mobi
import unstructured
from unstructured.partition.auto import partition
from unstructured.partition.utils.constants import PartitionStrategy

DEBUG = True

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

def find_directory(directory):
    if os.path.exists(directory):
        return directory
    tests = [os.path.dirname(__file__) + "/" + directory, os.path.dirname(__file__) + directory, "./" + directory]
    for test in tests:
        if os.path.exists(test):
            return test
    return False


class TextResource(abc.ABC):
    def __init__(self, resource_path, resource_type=""):
        (resource_directory, resource_name) = os.path.split(resource_path)
        self.resource_name = resource_name
        self.resource_directory = resource_directory
        self.resource_type = resource_type
        self.resource_cached_content = ""
        self.resource_access_time = 0
        self.resource_hash = 0
        self.loading_status = 0

    @abstractmethod
    def load_resource(self):
        pass

    def check_exists(self):
        return os.path.exists(self.get_path())

    def get_path(self, include_filename=True):
        return self.resource_directory + ('\\' + self.resource_name) if include_filename else ""

    def get_content(self):
        if self.loading_status == 100:
            return self.resource_cached_content
        return None

    def get_partitioned_content(self):
        pass

    def get_cleaned_content(self):
        pass


class HTMLTextResource(TextResource):

    def __init__(self, full_path, ctype="HTML"):
        super().__init__(full_path, ctype)

    def load_resource(self):
        file = self.get_path()
        text = ""
        try:
            text = unstructured.partition.auto.partition_html(filename=file)
        except Exception as e:
            self.loading_status = -1
            return

        self.resource_cached_content = text
        self.resource_access_time = time.time()
        self.loading_status = 100


class PDFTextResource(TextResource):

    def __init__(self, full_path):
        super().__init__(full_path, "PDF")

    def load_resource(self):
        path = self.get_path()
        def load_and_partition():
            # Loading and partitioning
            dbg(f"Attempting to load PDF: {path}",self)
            text = ""


            try:
                text = unstructured.partition.auto.partition_pdf(filename=path, strategy=PartitionStrategy.FAST, chunking_strategy="by_title")
                self.resource_cached_content = text
                self.resource_access_time = time.time()
                self.loading_status = 100  # Loading completed
            except Exception as e:
                self.loading_status = -1
                dbg(f"Problem in thread working {path}", e)
            return text

        threading.Thread(target=load_and_partition).start()


class EPUBTextResource(TextResource):

    def __init__(self, full_path, ctype="EPUB"):
        super().__init__(full_path, ctype)

    def load_resource(self):
        path = self.get_path()
        # Loading and partitioning
        text, _ = unstructured.partition.auto.partition_epub(path)
        self.resource_cached_content = text
        self.resource_access_time = time.time()
        self.loading_status = 100  # Loading completed
        partitioned_content = partition(text)  # Assuming partition returns partitioned content
        return partitioned_content


class MOBITextResource(TextResource):

    def __init__(self, full_path, ctype="MOBI"):
        super().__init__(full_path, ctype)

    def load_resource(self):
        path = self.get_path()
        # Extracting content from MOBI and passing it to the EPUBTextResource's load_resource method
        tempdir = None
        try:
            tempdir, filepath = mobi.extract(self.get_path())
        except Exception as e:
            dbg(f"Problem converting file {path}: {e.__str__()}", self)
            if tempdir is not None and os.path.exists(tempdir):
                shutil.rmtree(tempdir)
            return False
        else:

            path, ext = os.path.splitext(filepath)

            if ext.lower().removeprefix('.') == 'html':
                newResource = HTMLTextResource(filepath)
                newResource.load_resource()
                while newResource.loading_status != 100:
                    continue
                self.resource_cached_content = newResource.resource_cached_content

            if ext.lower().removeprefix('.') == 'epub':
                newResource = EPUBTextResource(filepath)
                newResource.load_resource()
                while newResource.loading_status != 100:
                    continue
                self.resource_cached_content = newResource.resource_cached_content

            if ext.lower().removeprefix('.') == 'pdf':
                newResource = PDFTextResource(filepath)
                newResource.load_resource()
                while newResource.loading_status != 100:
                    continue
                self.resource_cached_content = newResource.resource_cached_content

            self.loading_status = 100
            self.resource_access_time = time.time()


class DJVUTextResource(TextResource):

    def __init__(self, full_path):
        super().__init__(full_path, "DJVU")

    def load_resource(self):

        path = self.get_path()
        # Check if the djvutext.exe tool exists
        djvutext_path = os.path.realpath("./tools/djvutxt.exe")
        if not os.path.exists(djvutext_path):
            raise FileNotFoundError("djvutxt.exe tool not found in tools directory.")

        # Run djvutext.exe to extract text from DJVU file
        output_file_path = os.path.splitext(path)[0] + ".txt"
        subprocess.run([djvutext_path, path, output_file_path], check=True)

        # Read the extracted text from the output file
        with open(output_file_path, 'r', encoding='utf-8') as output_file:
            self.resource_cached_content = output_file.read()
            self.resource_access_time = time.time()
            self.loading_status = 100

        # Delete the temporary output file
        os.remove(output_file_path)



class ResourceFinder:
    def __init__(self, directory: str, types: Tuple[str] = ('pdf','mobi','epub','html','djvu')):
        self.directory = find_directory(directory)
        dbg(f"Loading directory: {directory}", self)
        if not self.directory:
            raise RuntimeError(f"Failed to find directory: {directory}")

        self.types = types
        self.resources = []
        dbg(f"Found {self.scan()} files to extract.", self)

    def scan(self):
        if not self.directory:
            return False
        files = set()
        for root, _, filenames in os.walk(self.directory):
            for filename in filenames:
                if filename.lower().endswith(self.types):
                    subj = os.path.join(root, filename)
                    files.add(subj)
        self.resources = list(files.union(self.resources))
        return len(files)

    def get_resources(self)->List[str]:
        return self.resources if self.directory else []




if __name__ == "__main__":
    logging.basicConfig(filename=dbg_file, filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    directory = input("Enter the directory to scan: ")
