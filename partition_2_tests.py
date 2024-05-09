import os
import unittest
import tempfile
import shutil
from partition_2 import EPUBTextResource, PDFTextResource, MOBITextResource, HTMLTextResource, ResourceFinder, \
    DJVUTextResource, AssignedPool



def _log(x, src=None):
    print(f"DEBUG: {x} ;; {('No Object' if not src else src)}")

class TestTextResource(unittest.TestCase):

    def tearDown(self):
        # Clean up any resources used in the test cases
        pass

    def test_html_text_resource(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a sample HTML file
            html_content = "<html><body><h1>Hello, World!</h1></body></html>"
            html_file_path = os.path.join(temp_dir, "sample.html")
            with open(html_file_path, 'w') as html_file:
                html_file.write(html_content)

            # Test HTMLTextResource
            html_resource = HTMLTextResource(html_file_path)
            html_resource.load_resource()

            while html_resource.loading_status != 100:
                continue

            content = html_resource.get_content()
            self.assertIsNotNone(content)

    def test_pdf_text_resource(self):
        pdf_file_path = "D:\\Project\\Py\\bulkTextPartition\\TestOCR.pdf"

        # Test PDFTextResource
        pdf_resource = PDFTextResource(pdf_file_path)
        pdf_resource.load_resource()

        while pdf_resource.loading_status != 100:
            if pdf_resource.loading_status < 0:
                raise RuntimeError(f"Error encountered in worker thread for resource {pdf_file_path}")
            continue

        pdf_content = pdf_resource.get_content()
        self.assertIsNotNone(pdf_content)

    def test_epub_text_resource(self):
        epub_file_path = "D:\\Project\\Py\\bulkTextPartition\\test\\No Shadow of a Doubt_ The 1919 Eclipse That Confirmed Einstein’s Theory of Relativity_Daniel Kennefick_liber3.epub"

        # Test PDFTextResource
        epub_resource = EPUBTextResource(epub_file_path)
        epub_resource.load_resource()

        _log("Tracking progress.", epub_resource)
        prog = 0
        while epub_resource.loading_status != 100:
            st = epub_resource.loading_status
            if prog != st:
                _log(f"Progress made on resource - {st}", epub_resource)
            if st < 0:
                raise RuntimeError(f"Error encountered in worker thread for resource {epub_file_path}")
            prog = st
            continue

        new_epub_paths = epub_resource.get_file_parts(True)
        self.assertIsNotNone(new_epub_paths)

        _log("Cleaning up split files for resource.", epub_resource)

        for path in new_epub_paths:
            if os.path.exists(path):
                os.remove(path)

        _log("Complete")

    def test_mobi_text_resource(self):

        mobi_file_path = "D:\\Project\\Py\\bulkTextPartition\\test\\Out of My Later Years_Albert Einstein_liber3.mobi"
        # Test MOBITextResource
        mobi_resource = MOBITextResource(mobi_file_path)
        mobi_resource.load_resource()

        # at this point we no longer know if we have a mobi file, but we do know how many parts whatever text we did get comes in.

        _log("Tracking progress.", mobi_resource)
        prog = 0
        while mobi_resource.loading_status != 100:
            st = mobi_resource.loading_status
            if prog != st:
                _log(f"Progress made on resource - {st}", mobi_resource)
            if st < 0:
                raise RuntimeError(f"Error encountered in worker thread for resource {mobi_resource.resource_name}")
            prog = st
            continue

        # get_file_parts should still return the locations of our split or converted files.
        new_document_paths = mobi_resource.get_file_parts(True)
        self.assertIsNotNone(new_document_paths)

        _log("Cleaning up split files for resource.", new_document_paths)

        for path in new_document_paths:
            if os.path.exists(path):
                os.remove(path)

        _log("Complete")

    def test_resource_finder(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create sample files
            sample_files = ["sample.html", "sample.pdf", "sample.mobi"]
            for filename in sample_files:
                file_path = os.path.join(temp_dir, filename)
                with open(file_path, 'w') as file:
                    file.write("Sample content.")

            # Test ResourceFinder
            resource_finder = ResourceFinder(temp_dir)
            resources = resource_finder.get_resources()
            expected_resources = [os.path.join(temp_dir, filename) for filename in sample_files]
            self.assertEqual(sorted(resources), sorted(expected_resources))

            print("Now legs try that on real files...")
            rtest_dir = os.path.abspath("./test")
            resource_finder = ResourceFinder(rtest_dir)
            resources = resource_finder.get_resources()

            print(f"Found {len(resources)} resources.")
            print('\n'.join([resource for resource in resources]))

    def test_load_djvu_text(self):
        djvu_file_path = "test\\Jagdish Mehra - Einstein, Physics and Reality-World Scientific (1999).djvu"
        djvu_resource = DJVUTextResource(djvu_file_path)
        djvu_resource.load_resource()

        while djvu_resource.loading_status != 100:
            continue
        extracted_text = djvu_resource.get_content()
        # Verify that the extracted text is not empty
        self.assertIsNotNone(extracted_text)
        print(f"Length of hidden text found in djvu: {len(extracted_text)}")  # Print the extracted text for manual inspection

from mpire import WorkerPool
from partition_2 import PoolManager

# busy work
def fibonacci(n):
    if n <= 1:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)


class TestPoolManager(unittest.TestCase):
    def setUp(self):
        self.pool_manager = PoolManager()

    def tearDown(self):
        self.pool_manager.cleanup_assigned_pools()

    def test_create_assigned_pool(self):
        # Create three assigned pools with 10 workers each
        for i in range(3):
            resource = HTMLTextResource(f"Resource_{i}.html")
            assigned_pool = self.pool_manager.create_assigned_pool(resource, num_workers=10)
            self.assertIsInstance(assigned_pool, AssignedPool)
            self.assertIsInstance(assigned_pool.get_worker_pool(), WorkerPool)

    def test_find_assigned_pool_by_name(self):
        # Create three assigned pools with 10 workers each
        for i in range(3):
            resource = HTMLTextResource(f"Resource_{i}.html")
            self.pool_manager.create_assigned_pool(resource, num_workers=10)

        # Test finding assigned pools by name
        for i in range(3):
            assigned_pool = self.pool_manager.find_assigned_pool_by_name(f"Resource_{i}.html")
            self.assertIsNotNone(assigned_pool)
            self.assertEqual(assigned_pool.get_resource().resource_name, f"Resource_{i}.html")




if __name__ == '__main__':

    # see the documentation how to customize the installation path
    # but be aware that you then need to include it in the `PATH`

    unittest.main()