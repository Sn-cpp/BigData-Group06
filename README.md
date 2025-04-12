# Big Data - Group 06

## Environment Setup

### Using WSL (Windows Subsystem for Linux)

1. Install WSL on Windows:
```
wsl --install
```

2. After installation, open WSL terminal

### Python Virtual Environment

1. Create a virtual environment:
```
python -m venv .venv
```

2. Activate the virtual environment:
   - In WSL:
   ```
   source venv/bin/activate
   ```

3. Install required packages:
```
pip install -r requirements.txt
```

## Datasets

### Classification
- Credit Card Fraud Detection: [https://www.kaggle.com/mlg-ulb/creditcardfraud](https://www.kaggle.com/mlg-ulb/creditcardfraud)

### Regression
- New York City Taxi Trip Duration: [https://www.kaggle.com/c/nyc-taxi-trip-duration/data](https://www.kaggle.com/c/nyc-taxi-trip-duration/data)