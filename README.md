# Fidibo.com comments Sentiment Analyser
# Introduction
This project first grab fidibo.com books comment data using `grabber.py` and then save 2 `.csv` files named `books.csv` which is the name of books and other attributes and `commments.csv`.
The second file that do the actual sentiment analysis is `score_comments.py` which first loads the `comments.csv` and then do the sentiment analysis.
# Usage

```commandline
python3.9 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
./grabber.py
./score_comments.py
```