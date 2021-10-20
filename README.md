# Search-Engine

AUTHORS:

Haoyuan Bai
Aliya Valieva


LANGUAGE: Python 3.9

DESCRIPTION:
The project consists of two parts, Indexer.py and SearchEngine.py.

The Indexer.py includes lemmatizing words, calculating the TF-IDF score and handling

HTML parsers. SearchEngine.py is the main module to run, it connects to the querying index database and retrieve 20 URLS for each query

Collecting statistics through tools including sql and sqlite. Libraries:

pip install spacy, parser, bs4, lemmatizer, Flask, sqlite3 python -m spacy download en_core_web_sm

Please download DB Browser for SQLite to have a better user friendly experience for viewing DOCINDEX.db

Running on: http://127.0.0.1:5000/
