"""Tests for MongoDB backup operations."""

import json
from pathlib import Path
import pytest
from testcontainers.mongodb import MongoDbContainer
from mongowiz.core.backup import backup_collection, get_collections_info

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
def setup_test_collection(mongodb_client, test_data):
    """Set up test collection with sample data."""
    db = mongodb_client["test_db"]
    collection = db["test_collection"]
    collection.insert_many(test_data)
    yield collection
    collection.drop()

def test_backup_collection(tmp_path, mongodb_client, setup_test_collection):
    """Test backing up a collection."""
    # Perform backup
    backup_dir = tmp_path / "backup"
    result = backup_collection(mongodb_client, "test_db", "test_collection", backup_dir)
    assert result is True
    
    # Verify backup file exists
    backup_file = backup_dir / "test_db" / "test_collection.json"
    assert backup_file.exists()
    
    # Verify backup contents
    with open(backup_file) as f:
        backed_up_data = json.load(f)
    assert len(backed_up_data) == 3
    assert all(doc["name"].startswith("Test ") for doc in backed_up_data)

def test_get_collections_info(mongodb_client, setup_test_collection):
    """Test getting collection information."""
    collections_info = get_collections_info(mongodb_client)
    
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
