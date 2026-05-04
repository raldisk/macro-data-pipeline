from datetime import date
from src.utils.batch import normalize_batch_date

def test_normalize_batch_date_mid_month():
    assert normalize_batch_date(date(2023, 1, 15)) == date(2023, 1, 1)

def test_normalize_batch_date_first():
    assert normalize_batch_date(date(2023, 1, 1)) == date(2023, 1, 1)

def test_normalize_batch_date_last_day():
    assert normalize_batch_date(date(2023, 1, 31)) == date(2023, 1, 1)

def test_bronze_path_month_zero_padded():
    batch_date = normalize_batch_date(date(2024, 1, 5))
    assert f"month={batch_date.month:02d}" == "month=01"

def test_bronze_path_format():
    batch_date = normalize_batch_date(date(2024, 3, 15))
    path = f"bronze/psa/year={batch_date.year}/month={batch_date.month:02d}/test-id.parquet"
    assert path == "bronze/psa/year=2024/month=03/test-id.parquet"
