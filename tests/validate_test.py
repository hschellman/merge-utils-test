"""Tests for the validate module"""

import validate

def test_uniqueness():
    """Test unique file list"""
    input_files = [
        {'namespace': 'namespace1', 'name': 'file1', 'metadata': {'field1': 'value1'}},
        {'namespace': 'namespace1', 'name': 'file2', 'metadata': {'field1': 'value2'}},
        {'namespace': 'namespace1', 'name': 'file1', 'metadata': {'field1': 'value1'}},
    ]
    unique_files = validate.UniqueFileList()
    for file in input_files:
        unique_files.add(file)

    assert list(unique_files) == [input_files[0], input_files[1]]
    assert unique_files.dupes() == {'namespace1:file1': 1}
    assert 'namespace1:file1' in unique_files
    assert 'namespace1:file2' in unique_files

def test_get_checked_fields():
    """Test getting checked fields"""
    file = {
        'namespace': 'namespace1',
        'name': 'file1',
        'metadata': {
            'field1': 'value1',
            'core.run_type': 'run_type1',
            'core.file_type': 'file_type1',
            'dune.config_file': 'config_file1',
        }
    }
    checked_fields = validate.get_checked_fields(file)
    assert checked_fields == {
        'namespace': 'namespace1',
        'core.run_type': 'run_type1',
        'core.file_type': 'file_type1',
    }

    checked_fields_strict = validate.get_checked_fields(file, strict=True)
    assert checked_fields_strict == {
        'namespace': 'namespace1',
        'core.run_type': 'run_type1',
        'core.file_type': 'file_type1',
        'dune.config_file': 'config_file1',
    }
