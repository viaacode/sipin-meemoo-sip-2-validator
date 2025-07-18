from meemoo_sip_validator.sip_validator import MeemooSIPValidator
from pathlib import Path


def test_sanity_check():
    assert True


def test_validator_library():
    validator = MeemooSIPValidator(Path("./tests/resources/film_sip"))
    is_valid = validator.validate()
    # At the moment the example sip is not EARK compliant.
    assert not is_valid
    assert validator.validation_report.to_dict() # Simple check
