"""Tests for MongoDB restore operations."""

import json
from pathlib import Path
import pytest
from bson import ObjectId
from datetime import datetime
from testcontainers.mongodb import MongoDbContainer
from mongowiz.core.restore import restore_collection, get_collections_info, RestoreError

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
def complex_test_data():
    """Test data with various MongoDB data types."""
    return [
        {
            "_id": ObjectId(),
            "name": "Complex 1",
            "created_at": datetime.utcnow(),
            "tags": ["tag1", "tag2"],
            "nested": {"key": "value"},
            "number_types": {
                "int": 42,
                "float": 3.14,
                "decimal": 10.99
            }
        },
        {
            "_id": ObjectId(),
            "name": "Complex 2",
            "array_with_nulls": [1, None, 3],
            "empty_array": [],
            "empty_object": {},
            "null_field": None
        }
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

@pytest.fixture
def complex_backup_dir(tmp_path, complex_test_data):
    """Create a test backup directory with complex data types."""
    backup_dir = tmp_path / "backup"
    db_dir = backup_dir / "test_db"
    db_dir.mkdir(parents=True)
    
    # Create backup file
    backup_file = db_dir / "complex_collection.json"
    with open(backup_file, "w") as f:
        json.dump(complex_test_data, f, default=str)  # Use str for ObjectId and datetime
    
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

def test_restore_existing_collection(mongodb_client, backup_dir, test_data):
    """Test that restoring to an existing collection fails by default."""
    db = mongodb_client["test_db"]
    collection = db["test_collection"]
    
    # First create a collection with some data
    collection.insert_many(test_data)
    
    # Attempt to restore to the same collection should raise an error
    with pytest.raises(RestoreError) as exc_info:
        restore_collection(mongodb_client, backup_dir, "test_db", "test_collection")
    assert "already exists" in str(exc_info.value)
    
    # Verify original data is unchanged
    docs = list(collection.find({}))
    assert len(docs) == 3
    
    # Test force restore
    result = restore_collection(mongodb_client, backup_dir, "test_db", "test_collection", force=True)
    assert result is True
    
    # Verify data was overwritten
    docs = list(collection.find({}))
    assert len(docs) == 3
    assert all(doc["name"].startswith("Test ") for doc in docs)
    
    # Clean up
    collection.drop()

def test_restore_complex_data_types(mongodb_client, complex_backup_dir):
    """Test restoring a collection with complex MongoDB data types."""
    # Create test data
    test_data = [
        {
            "_id": {"$type": "ObjectId", "$value": str(ObjectId())},
            "name": "Complex 1",
            "created_at": {"$type": "datetime", "$value": datetime.now().isoformat()},
            "tags": ["tag1", "tag2"],
            "nested": {"key": "value"},
            "number_types": {
                "int": 42,
                "float": 3.14,
                "decimal": 10.99
            }
        },
        {
            "_id": {"$type": "ObjectId", "$value": str(ObjectId())},
            "name": "Complex 2",
            "array_with_nulls": [1, None, 3],
            "empty_array": [],
            "empty_object": {},
            "null_field": None
        }
    ]
    
    # Create backup file
    backup_dir = complex_backup_dir / "test_db"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_file = backup_dir / "complex_collection.json"
    with open(backup_file, "w") as f:
        json.dump(test_data, f)
    
    # Perform restore
    result = restore_collection(mongodb_client, complex_backup_dir, "test_db", "complex_collection")
    assert result is True
    
    # Verify restored data
    db = mongodb_client["test_db"]
    collection = db["complex_collection"]
    
    restored_docs = list(collection.find())
    assert len(restored_docs) == 2
    
    # Check first document
    doc1 = restored_docs[0]
    assert isinstance(doc1["_id"], ObjectId)
    assert isinstance(doc1["created_at"], datetime)
    assert isinstance(doc1["tags"], list)
    assert isinstance(doc1["nested"], dict)
    assert isinstance(doc1["number_types"], dict)
    assert doc1["number_types"]["float"] == 3.14
    
    # Check second document
    doc2 = restored_docs[1]
    assert isinstance(doc2["_id"], ObjectId)
    assert None in doc2["array_with_nulls"]
    assert len(doc2["empty_array"]) == 0
    assert len(doc2["empty_object"]) == 0
    assert doc2["null_field"] is None
    
    # Clean up
    collection.drop()

def test_restore_from_invalid_backup(mongodb_client, tmp_path):
    """Test restoring from an invalid backup file."""
    # Create an invalid backup file
    backup_dir = tmp_path / "backup"
    db_dir = backup_dir / "test_db"
    db_dir.mkdir(parents=True)
    
    backup_file = db_dir / "test_collection.json"
    with open(backup_file, "w") as f:
        f.write("invalid json")
    
    # Attempt restore
    result = restore_collection(mongodb_client, backup_dir, "test_db", "test_collection")
    assert result is False

def test_restore_from_nonexistent_backup(mongodb_client, tmp_path):
    """Test restoring from a nonexistent backup file."""
    backup_dir = tmp_path / "backup"
    result = restore_collection(mongodb_client, backup_dir, "test_db", "test_collection")
    assert result is False

def test_restore_empty_collection(mongodb_client, tmp_path):
    """Test restoring an empty collection."""
    # Create backup with empty array
    backup_dir = tmp_path / "backup"
    db_dir = backup_dir / "test_db"
    db_dir.mkdir(parents=True)
    
    backup_file = db_dir / "empty_collection.json"
    with open(backup_file, "w") as f:
        json.dump([], f)
    
    # Perform restore
    result = restore_collection(mongodb_client, backup_dir, "test_db", "empty_collection")
    assert result is True
    
    # Verify collection exists but is empty
    db = mongodb_client["test_db"]
    collection = db["empty_collection"]
    assert collection.count_documents({}) == 0
    
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
