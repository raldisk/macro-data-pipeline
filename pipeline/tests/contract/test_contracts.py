import pytest
from src.contracts.loader import Contract, load_contract

@pytest.mark.parametrize("name", ["gold_macro_indicators", "gold_exchange_rates"])
def test_contract_loads(name):
    assert isinstance(load_contract(name), Contract)

@pytest.mark.parametrize("name", ["gold_macro_indicators", "gold_exchange_rates"])
def test_contract_has_quality_checks(name):
    assert len(load_contract(name).quality_checks) > 0

@pytest.mark.parametrize("name", ["gold_macro_indicators", "gold_exchange_rates"])
def test_contract_failure_threshold(name):
    assert load_contract(name).hard_failure_threshold == 0.05

def test_macro_row_count_min_100():
    c = load_contract("gold_macro_indicators")
    check = next(x for x in c.quality_checks if x.check == "row_count_min")
    assert check.value == 100

def test_fx_row_count_min_20():
    c = load_contract("gold_exchange_rates")
    check = next(x for x in c.quality_checks if x.check == "row_count_min")
    assert check.value == 20

def test_unknown_contract_raises():
    with pytest.raises(FileNotFoundError):
        load_contract("nonexistent")
