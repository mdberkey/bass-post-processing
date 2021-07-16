# bass-post-processing
## WNPRC post-processing for BASS assessments CSV files.
[![Python Version](https://img.shields.io/badge/Python-3.8%20-blue.svg)](https://www.python.org/)
![platform](https://img.shields.io/badge/Linux%20-red.svg)
![Version](https://img.shields.io/badge/Version-0.1-green)

## Setup:
1. Clone repo
```
git clone https://github.com/mdberkey/bass-post-processing.git
```
2. Create python virtual environment that program runs
```
python3 -m venv Desktop/bass/venv
```
3. Install modules on venv with requirements.txt
```
source Desktop/bass/venv/bin/activate && pip install -r requirements.txt
```

## Usage:
1. Add all properly formatted CSV files to Data folder.
2. Run program through venv
```
cd Desktop/bass && source venv/bin/activate && python 2021_bass_pipeline.py
```
4. Processed data is stored in Output folder

## Command Line Usage:
1. Add all properly formatted CSV files to Data folder.
2. Activate virtual environment (Python 3.8 with pandas and openpyxl)
3. run: python pipeline.py arg1 arg2
- Note: arg1 = empty data placeholder, arg2 = name of output .xlsx file
4. Processed data is stored in Output folder.
