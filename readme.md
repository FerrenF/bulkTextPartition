Absolutely! Here's a basic GitHub README for your code:

## Bulk Text Extractor

This Python script extracts text from various document formats and splits them into segments using the `unstructured-io` library.

### Features

* Extracts text from PDF, MOBI, EPUB, and DJVU files.
* Splits documents into segments based on specified settings.
* Saves extracted segments as JSON files.

### Requirements

* Python 3.x
* unstructured-io (with `all-docs` package)

### Usage

1. Install dependencies: `pip install unstructured[all-docs]`
2. Clone or download the repository.
3. Run the script: `python bulk_text_extractor.py <directory>`
    * Replace `<directory>` with the path to your documents directory.
4. The script will process each document and save extracted segments in a subdirectory within the specified directory.

### Options

* You can modify the `BulkTextExtract` class to customize settings like chunking strategy, page break handling, etc.
* Refer to the `unstructured-io` documentation for more advanced functionalities.

### License

MIT License
