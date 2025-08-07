from pathlib import Path


def test_sanity():
    assert True


def test_submodule_present():
    examples_path = Path("tests/sip-examples")
    is_empty = len(list(examples_path.iterdir())) == 0
    assert not is_empty
