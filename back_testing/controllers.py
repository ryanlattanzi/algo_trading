from enum import Enum


class TestPeriodController(Enum):
    one_mo = "1mo"
    three_mo = "3mo"
    six_mo = "6mo"
    one_yr = "1yr"
    two_yr = "2yr"
    five_yr = "5yr"
    ten_yr = "10yr"
    max = "max"
