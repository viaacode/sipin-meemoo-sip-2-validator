import pytest
from meemoo_sip_validator.sip_validator import MeemooSIPValidator
from pathlib import Path


def test_sanity_check():
    assert True
    
    
def test_validator_library():
    validator = MeemooSIPValidator()
    succes, report = validator.validate(Path("./tests/resources/film_sip"))
    # At the moment the example sip is not EARK compliant.
    assert not succes
    assert report