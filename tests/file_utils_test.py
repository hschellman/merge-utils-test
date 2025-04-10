"""Tests for the metacat utils module"""

#import pytest
from merge_utils.merge_set import MergeFile, MergeSet

def fake_file(namespace, name, metadata = None):
    """Create a file dictionary for testing"""
    return {
        'namespace': namespace,
        'name': name,
        'fid': 123,
        'size': 456789,
        'checksums': {},
        'metadata': metadata
    }

def test_merge_file():
    """Test the MergeFile class behavior"""
    file = MergeFile(fake_file('test_namespace', 'test_name', {
        'checked.field1': 'value1',
        'checked.field2': 'value2',
        'other.field': 'other_value'
    }))
    assert file.did == 'test_namespace:test_name'
    assert file.namespace == 'test_namespace'
    assert file.name == 'test_name'
    assert file.size == 456789
    assert hash(file) == hash('test_namespace:test_name')
    assert str(file) == 'test_namespace:test_name'
    assert file.get_fields(['checked.field1', 'checked.field2']) == ('test_namespace', 'value1', 'value2')

def test_merge_set_uniqueness():
    """Test MergeSet uniqueness criteria"""
    inputs = [
        ['namespace1', 'file1'],
        ['namespace1', 'file2'],
        ['namespace1', 'file1'],
    ]
    input_files = []
    unique_files = MergeSet()
    for i in inputs:
        file = fake_file(*i)
        input_files.append(MergeFile(file))
        unique_files.add(file)

    assert list(unique_files) == [input_files[0], input_files[1]]
    assert unique_files.dupes() == {'namespace1:file1': 1}
    assert 'namespace1:file1' in unique_files
    assert 'namespace1:file2' in unique_files

def test_merge_set_consistency():
    """Test MergeSet consistency checks"""
    files = MergeSet()
    files.add(fake_file('namespace1', 'file1', {
        'checked.field1': 'value1',
        'checked.field2': 'value2',
        'other.field': 'other_value1'
    }))
    files.add(fake_file('namespace1', 'file2', {
        'checked.field1': 'value1',
        'checked.field2': 'value2',
        'other.field': 'other_value2'
    }))
    files.add(fake_file('namespace1', 'file3', {
        'checked.field1': 'value1',
        'checked.field2': 'value2',
        'other.field': 'other_value3'
    }))
    assert files.check_consistency(['checked.field1', 'checked.field2']) is True

    files['namespace1:file2'].metadata['checked.field1'] = 'bad_value'
    assert files.check_consistency(['checked.field1', 'checked.field2']) is False
    assert files.check_consistency(['checked.field1']) is False
    assert files.check_consistency(['checked.field2']) is True
    files['namespace1:file2'].metadata['checked.field1'] = 'value1'
    assert files.check_consistency(['checked.field1', 'checked.field2']) is True

    files['namespace1:file3'].metadata['checked.field2'] = 'bad_value'
    assert files.check_consistency(['checked.field1', 'checked.field2']) is False
    assert files.check_consistency(['checked.field1']) is True
    assert files.check_consistency(['checked.field2']) is False
    files['namespace1:file3'].metadata['checked.field2'] = 'value2'

    files.add(fake_file('namespace2', 'file4', {
        'checked.field1': 'value1',
        'checked.field2': 'value2',
        'other.field': 'other_value4'
    }))
    assert files.check_consistency(['checked.field1', 'checked.field2']) is False
    assert files.check_consistency([]) is False
