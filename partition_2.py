import abc
import io
import logging
import math
import os
import json
import subprocess
import threading
import time
import shutil
from abc import abstractmethod

from typing import Tuple, List, Any, Optional

from mpire import WorkerPool
from mpire.utils import make_single_arguments
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


class AssignedPool:

    def __init__(self, resource: Any, worker_pool: WorkerPool):
        """
        Assigned pool tracks a worker pool related to some work, and the TextResource attached to it.
        :param resource:
        :param worker_pool:
        """
        self.resource: Any = resource
        self.worker_pool: WorkerPool = worker_pool

    def get_resource(self) -> Any:
        return self.resource

    def get_worker_pool(self) -> WorkerPool:
        return self.worker_pool


class PoolManager:
    def __init__(self):
        self.assigned_pools: List[AssignedPool] = []

    def create_assigned_pool(self, resource: Any, num_workers: int) -> AssignedPool:
        worker_pool = WorkerPool(num_workers)
        assigned_pool = AssignedPool(resource, worker_pool)
        self.assigned_pools.append(assigned_pool)
        return assigned_pool

    def cleanup_assigned_pools(self, resources: Optional[List[str]]):

        if resources and len(resources):
            for resource in resources:
                pool = self.find_assigned_pool_by_name(resource)
                if pool is not None:
                    pool.get_worker_pool().terminate()
            return

        for assigned_pool in self.assigned_pools:
            assigned_pool.get_worker_pool().terminate()
        self.assigned_pools.clear()

    def find_assigned_pool_by_name(self, resource_name: str) -> Optional[AssignedPool]:
        for assigned_pool in self.assigned_pools:
            if assigned_pool.get_resource().resource_name == resource_name:
                return assigned_pool
        return None


## SECTION BEGIN: BulkPartition2

_PoolManager = PoolManager()



def calculate_breaks(file_size_mb, mb_break_threshold, max_breaks):
    """
    Calculate the number of breaks based on the file size.

    Args:
    - file_size_mb (float): The size of the file in megabytes.
    - mb_break_threshold (float): The threshold in megabytes.
    - max_breaks (int): The maximum number of breaks.

    Returns:
    - int: The number of breaks.
    """
    if file_size_mb - mb_break_threshold == 0:
        # sneakily make a divide by 0 impossible.
        return 2
    if file_size_mb <= mb_break_threshold:
        return 2
    elif file_size_mb > mb_break_threshold:
        # Calculate the number of breaks using linear interpolation
        slope = (max_breaks - 2) / (file_size_mb - mb_break_threshold)
        breaks = int(slope * (file_size_mb - mb_break_threshold) + 2)
        return min(breaks, max_breaks)


def find_directory(directory):
    if os.path.exists(directory):
        return directory
    tests = [os.path.dirname(__file__) + "/" + directory, os.path.dirname(__file__) + directory, "./" + directory]
    for test in tests:
        if os.path.exists(test):
            return test
    return False


class TextResource(abc.ABC):
    BREAK_SIZE_MB = 5  # threshold for breaking into parts
    BREAK_PARTS_MAX = 10  # max number of jobs per resource

    def __init__(self, resource_path, resource_type=""):
        (resource_directory, resource_name) = os.path.split(resource_path)
        self.resource_name = resource_name
        self.resource_directory = resource_directory
        self.resource_type = resource_type
        self.resource_cached_content = []
        self.resource_access_time = 0
        self.resource_hash = 0
        self.resource_parts = 1
        self.loading_status = 0

    @abstractmethod
    def load_resource(self):
        pass

    def check_exists(self):
        return os.path.exists(self.get_path())

    def get_path(self, include_filename=True):
        return self.resource_directory + ('\\' + self.resource_name) if include_filename else ""


    def get_content(self):
        return self.resource_cached_content

    def get_file_parts(self, with_ext=False):
        if self.resource_parts == 1:
            return [self.get_path()]

        (root, ext) = os.path.splitext(self.resource_name)
        if not with_ext:
            ext = ""

        return [self.resource_directory + '\\' + root + f"_part{(x+1)}{ext}" for x in range(self.resource_parts)]

    def resource_size(self, mb=False):
        _bytes = os.path.getsize(self.get_path())
        return _bytes if not mb else _bytes / 1 * math.exp(6)

    @abstractmethod
    def split_resource(self, num_parts=1) -> int:
        pass

    def mk_part_labl(self,x, use_ext=False):
        (root, ext) = os.path.splitext(self.resource_name)
        return f"{root}_part{x + 1}" + (".epub" if use_ext else "")

PARTITION_CONFIG_PDF = {"strategy": PartitionStrategy.FAST}
PARTITION_CONFIG_HTML = {}
PARTITION_CONFIG_MOBI = {}
PARTITION_CONFIG_EPUB = {}
PARTITION_CONFIG_FALLBACK = {}
def mp_pool_worker_work(packer) -> Tuple[int, Any]:
    (cached_content_reference, work_iterator) = packer
    (chunk_num, file) = work_iterator
    try:
        (root, ext) = os.path.splitext(file)
        str_ext = str(ext).lstrip('.').lower()
        if str_ext == 'html':
            text = unstructured.partition.auto.partition_html(filename=file, **PARTITION_CONFIG_HTML)
        elif str_ext == 'pdf':
            text = unstructured.partition.auto.partition_pdf(filename=file, **PARTITION_CONFIG_PDF)
        elif str_ext == 'epub':
            text = unstructured.partition.auto.partition_epub(filename=file, **PARTITION_CONFIG_EPUB)
        elif str_ext == 'mobi':
            raise RuntimeError("This shouldn't happen. We should not be processing a .mobi file")
        else:
            text = unstructured.partition.auto.partition(filename=file, **PARTITION_CONFIG_FALLBACK)
    except Exception as e:
        cached_content_reference[chunk_num] = -1
    else:
        cached_content_reference[chunk_num] = text
    return cached_content_reference


class HTMLTextResource(TextResource):

    def split_resource(self, num_parts=2) -> int:
        # we don't suppport splitting HTML files. they are generally ingested fast enough to not worry.
        return 1

    def __init__(self, full_path, ctype="html"):
        super().__init__(full_path, ctype)

    def load_resource(self):
        files = self.get_file_parts()
        self.resource_cached_content = [""] * len(files)
        _assignedPool = _PoolManager.create_assigned_pool(self, len(files))

        _argdict = map(lambda x: (self.resource_cached_content, x), enumerate(files))
        _m = _assignedPool.worker_pool.map(mp_pool_worker_work, make_single_arguments(_argdict), chunk_size=1)

        counter = 0
        for result in _m:
            counter += 1
            self.loading_status = (100 / len(files)) * counter
            dbg(f"Worker for resource {self.resource_name} completed a task. Load status {self.loading_status}", self)

        self.resource_access_time = time.time()
        _PoolManager.cleanup_assigned_pools([self.resource_name])
        dbg("Clean-up complete.", self)


class PDFTextResource(TextResource):

    def split_resource(self, num_parts=1) -> int:
        pass

    def __init__(self, full_path):
        super().__init__(full_path, "pdf")

    def load_resource(self):
        path = self.get_path()

        def load_and_partition():
            # Loading and partitioning
            dbg(f"Attempting to load PDF: {path}", self)
            text = ""

            try:
                text = unstructured.partition.auto.partition_pdf(filename=path, strategy=PartitionStrategy.FAST,
                                                                 chunking_strategy="by_title")
                self.resource_cached_content = text
                self.resource_access_time = time.time()
                self.loading_status = 100  # Loading completed
            except Exception as e:
                self.loading_status = -1
                dbg(f"Problem in thread working {path}", e)
            return text

        threading.Thread(target=load_and_partition).start()


from ebooklib import epub


class EPUBTextResource(TextResource):

    def split_resource(self, num_parts=2) -> List[str]:

        input_epub_path = self.get_path()
        book = epub.read_epub(input_epub_path)

        # We can get chapter information from our object structure,
        # which means it's easier to split this file in a coherent way.
        current_items_generator = book.get_items()
        current_items_list = list(current_items_generator)
        num_items = len(current_items_list)

        if num_items < 2:
            # cant split, shant split
            return self.get_file_parts()

        # Calculate the number of items per part
        items_per_part = num_items // num_parts
        dbg(f"Splitting item {self.resource_name} into {num_parts} parts.", self)

        new_resource_paths = [""] * num_parts



        for part_num in range(num_parts):

            new_title = self.mk_part_labl(part_num)
            new_book = epub.EpubBook()

            new_book.set_title(new_title)
            new_book.set_language('en')

            # Add a subset of items from the original book to the new book
            start_index = part_num * items_per_part
            end_index = (part_num + 1) * items_per_part if part_num < num_parts - 1 else num_items
            for index in range(start_index, end_index):
                item = current_items_list[index]
                new_book.add_item(item)

            # Add navigation and spine references to the new book
            new_book.add_item(epub.EpubNcx())
            new_book.add_item(epub.EpubNav())
            new_book.spine = list(new_book.get_items())

            part_filename = self.mk_part_labl(part_num, True)
            part_output_path = os.path.join(self.resource_directory, part_filename)
            epub.write_epub(part_output_path, new_book)
            new_resource_paths[part_num] = part_output_path

        self.resource_parts = num_parts
        dbg(f"Split for item {self.resource_name} successful",self)
        return new_resource_paths

    def __init__(self, full_path, ctype="epub"):
        super().__init__(full_path, ctype)

    def load_resource(self):

        msize = self.resource_size(True)
        if msize > TextResource.BREAK_SIZE_MB:
            # we should try and break this.
            dbg(f"Request to load large resource {self.resource_name} made. Attempting to split resource for optimization.", self)
            break_num = calculate_breaks(msize, TextResource.BREAK_SIZE_MB, TextResource.BREAK_PARTS_MAX)
            result = self.split_resource(break_num)
            if len(result) != break_num:
                dbg("Attempted to split resource, but the result is the same number of parts. It's probably too short or there's only one chapter.", self)


        files = self.get_file_parts()
        self.resource_cached_content = [""] * len(files)
        _assignedPool = _PoolManager.create_assigned_pool(self, len(files))
        dbg(f"Processing {len(files)} resources for {self.resource_name}", self)
        _argdict = map(lambda x: (self.resource_cached_content, x), enumerate(files))
        _m = _assignedPool.worker_pool.map(mp_pool_worker_work, make_single_arguments(_argdict), chunk_size=1)

        counter = 0
        for result in _m:
            counter += 1
            self.loading_status = (100 / len(files)) * counter
            dbg(f"Worker for resource {self.resource_name} completed a task. Load status {self.loading_status}", self)

        self.resource_access_time = time.time()
        _PoolManager.cleanup_assigned_pools([self.resource_name])
        dbg("Clean-up complete.", self)


class MOBITextResource(TextResource):

    def split_resource(self, num_parts=1) -> int:
        return 1

    def __init__(self, full_path, ctype="mobi"):
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

            root, ext = os.path.splitext(filepath)
            newResource = None
            newExt = ext.lower().removeprefix('.')
            if newExt == 'html':
                newResource = HTMLTextResource(filepath)

            elif newExt == 'epub':
                newResource = EPUBTextResource(filepath)

            elif newExt == 'pdf':
                newResource = PDFTextResource(filepath)
            else:
                dbg(f"Encounter an unknown filetype after conversion from mobi: {ext}", self)
                self.loading_status = -1
                return

            newResource.load_resource()
            self.resource_cached_content = newResource.resource_cached_content
            self.resource_name = filepath
            self.resource_parts = len(newResource.resource_cached_content)
            self.loading_status = 100
            self.resource_access_time = time.time()
            self.resource_type = newExt


class DJVUTextResource(TextResource):

    def split_resource(self, num_parts=1) -> int:
        return 1

    def __init__(self, full_path):
        super().__init__(full_path, "DJVU")

    def load_resource(self):
        path = self.get_path()
        # Check if the djvutext.exe tool exists
        djvutext_path = os.path.realpath("./tools/djvutxt.exe")
        if not os.path.exists(djvutext_path):
            self.loading_status = -1
            return

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
    def __init__(self, directory: str, types: Tuple[str] = ('pdf', 'mobi', 'epub', 'html', 'djvu')):
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

    def get_resources(self) -> List[str]:
        return self.resources if self.directory else []


if __name__ == "__main__":
    logging.basicConfig(filename=dbg_file, filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    directory = input("Enter the directory to scan: ")
