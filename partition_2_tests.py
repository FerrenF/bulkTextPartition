import os
import unittest
import tempfile
import shutil
from partition_2 import EPUBTextResource, PDFTextResource, MOBITextResource, HTMLTextResource, ResourceFinder, \
    DJVUTextResource


class TestTextResource(unittest.TestCase):

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

    def test_mobi_text_resource(self):

        mobi_file_path = "D:\\Project\\Py\\bulkTextPartition\\test\\Out of My Later Years_Albert Einstein_liber3.mobi"
        # Test MOBITextResource
        mobi_resource = MOBITextResource(mobi_file_path)
        mobi_resource.load_resource()

        while mobi_resource.loading_status != 100:
            continue
        partitioned_content = mobi_resource.get_content()
        self.assertIsNotNone(partitioned_content)

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


if __name__ == '__main__':

    # see the documentation how to customize the installation path
    # but be aware that you then need to include it in the `PATH`

    unittest.main()