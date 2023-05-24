# 數據分析期末

## Setup
First set up virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
next edit .env file to set up your database connection:
```bash
DATA_DIR_PATH=[Change This to: path to data directory]
CONNECT_STR=sqlite:///database.db
```
load data into database:
```bash
python setup_dataset.py
```
