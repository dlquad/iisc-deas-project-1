#!/bin/bash

# chown 1001:1001 /.cache
pip install nltk requests
python -m nltk.downloader punkt_tab
python -m nltk.downloader stopwords