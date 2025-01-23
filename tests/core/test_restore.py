"""Tests for MongoDB restore operations."""

import json
from pathlib import Path
import pytest
from testcontainers.mongodb import MongoDbContainer
from mongowiz.core.restore import restore_collection, get_collections_info

@pytest.fixture(scope="module")
def mongodb_container():
    """Start MongoDB container for testing."""
    with MongoDbContainer() as container:
        yield container

@pytest.fixture(scope="module")
def mongodb_client(mongodb_container):
    """Get MongoDB client for testing."""
    return mongodb_container.get_connection_client()

@pytest.fixture
def test_data():
    """Sample test data."""
    return [
        {"_id": "1", "name": "Test 1", "value": 100},
        {"_id": "2", "name": "Test 2", "value": 200},
        {"_id": "3", "name": "Test 3", "value": 300},
    ]

@pytest.fixture
def backup_dir(tmp_path, test_data):
    """Create a test backup directory with sample data."""
    backup_dir = tmp_path / "backup"
    db_dir = backup_dir / "test_db"
    db_dir.mkdir(parents=True)
    
    # Create backup file
    backup_file = db_dir / "test_collection.json"
    with open(backup_file, "w") as f:
        json.dump(test_data, f)
    
    return backup_dir

def test_restore_collection(mongodb_client, backup_dir):
    """Test restoring a collection from backup."""
    # Perform restore
    result = restore_collection(mongodb_client, backup_dir, "test_db", "test_collection")
    assert result is True
    
    # Verify restored data
    db = mongodb_client["test_db"]
    collection = db["test_collection"]
    
    restored_docs = list(collection.find({}))
    assert len(restored_docs) == 3
    assert all(doc["name"].startswith("Test ") for doc in restored_docs)
    
    # Clean up
    collection.drop()

def test_get_collections_info(backup_dir):
    """Test getting collection information from backup directory."""
    collections_info = get_collections_info(backup_dir)
    
    assert "test_db" in collections_info
    test_collections = collections_info["test_db"]
    
    # Find test_collection info
    test_coll_info = next(
        (info for info in test_collections if info[0] == "test_collection"),
        None
    )
    
    assert test_coll_info is not None
    assert test_coll_info[1] == 3  # doc count
    assert test_coll_info[2] > 0  # size
