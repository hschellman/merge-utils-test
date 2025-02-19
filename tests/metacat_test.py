"""Tests for the metacat utils module"""

from src import metacat_utils
from src.file_utils import DataFile, UniqueFileList

def fake_file(namespace, name, metadata = None):
    """Create a file dictionary for testing"""
    return {
        'namespace': namespace,
        'name': name,
        'size': 123,
        'metadata': metadata
    }

def test_uniqueness():
    """Test unique file list"""
    inputs = [
        ['namespace1', 'file1'],
        ['namespace1', 'file2'],
        ['namespace1', 'file1'],
    ]
    input_files = []
    unique_files = UniqueFileList()
    for i in inputs:
        file = fake_file(*i)
        input_files.append(DataFile(file))
        unique_files.add(file)

    assert list(unique_files) == [input_files[0], input_files[1]]
    assert unique_files.dupes() == {'namespace1:file1': 1}
    assert 'namespace1:file1' in unique_files
    assert 'namespace1:file2' in unique_files

def test_checked_fields_single():
    """Test getting checked fields individually"""
    for field in metacat_utils.CHECKED_FIELDS:
        file = DataFile(fake_file('namespace1', 'file1', {
            field: 'value1',
            'other.field': 'value2'
        }))
        assert metacat_utils.get_checked_fields(file) == {
            'namespace': 'namespace1',
            field: 'value1'
        }
        assert metacat_utils.get_checked_fields(file, strict=True) == {
            'namespace': 'namespace1',
            field: 'value1'
        }

def test_checked_fields_strict():
    """Test getting strict fields"""
    for field in metacat_utils.CHECKED_FIELDS_STRICT:
        file = DataFile(fake_file('namespace1', 'file1', {
            field: 'value1',
            'other.field': 'value2'
        }))
        assert metacat_utils.get_checked_fields(file) == {
            'namespace': 'namespace1'
        }
        assert metacat_utils.get_checked_fields(file, strict=True) == {
            'namespace': 'namespace1',
            field: 'value1'
        }

def test_checked_fields_all():
    """Test getting all checked fields at once"""
    target_fields = {'namespace': 'namespace1'}
    target_fields_strict = {'namespace': 'namespace1'}
    intput_fields = {'other.field': 'other_value'}
    for field in metacat_utils.CHECKED_FIELDS:
        value = field + '.value'
        intput_fields[field] = value
        target_fields[field] = value
        target_fields_strict[field] = value
    for field in metacat_utils.CHECKED_FIELDS_STRICT:
        value = field + '.value'
        intput_fields[field] = value
        target_fields_strict[field] = value
    file = DataFile(fake_file('namespace1', 'file1', intput_fields))
    assert metacat_utils.get_checked_fields(file) == target_fields
    assert metacat_utils.get_checked_fields(file, strict=True) == target_fields_strict
