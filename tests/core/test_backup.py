"""Tests for MongoDB backup operations."""

import json
from pathlib import Path
import pytest
from bson import ObjectId
from datetime import datetime
from testcontainers.mongodb import MongoDbContainer
from mongowiz.core.backup import backup_collection, get_collections_info
from pymongo import MongoClient

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
def setup_test_collection(mongodb_client, test_data):
    """Set up test collection with sample data."""
    db = mongodb_client["test_db"]
    collection = db["test_collection"]
    collection.insert_many(test_data)
    yield collection
    collection.drop()

@pytest.fixture
def setup_complex_collection(mongodb_client, complex_test_data):
    """Set up test collection with complex data types."""
    db = mongodb_client["test_db"]
    collection = db["complex_collection"]
    collection.insert_many(complex_test_data)
    yield collection
    collection.drop()

@pytest.fixture
def mongodb_limited_user(mongodb_client):
    """Create a user with only read permissions for testing."""
    try:
        # First create a test database and collection with some data
        db = mongodb_client["test_db"]
        collection = db["test_collection"]
        collection.insert_one({"test": "data"})
        
        # Create a user with only read permissions
        db.command(
            "createUser",
            "limited_user",
            pwd="password123",
            roles=[{"role": "read", "db": "test_db"}]
        )
        
        # Create new client with limited user
        limited_client = MongoClient(
            host=mongodb_client.address[0],
            port=mongodb_client.address[1],
            username="limited_user",
            password="password123",
            authSource="test_db"
        )
        
        yield limited_client
        
        # Cleanup
        db.command("dropUser", "limited_user")
        collection.drop()
        limited_client.close()
        
    except Exception as e:
        pytest.skip(f"Could not create limited user: {e}")

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

def test_backup_empty_collection(tmp_path, mongodb_client):
    """Test backing up an empty collection."""
    # Create empty collection
    db = mongodb_client["test_db"]
    db.create_collection("empty_collection")
    
    try:
        # Perform backup
        backup_dir = tmp_path / "backup"
        result = backup_collection(mongodb_client, "test_db", "empty_collection", backup_dir)
        assert result is True
        
        # Verify backup file exists and contains empty array
        backup_file = backup_dir / "test_db" / "empty_collection.json"
        assert backup_file.exists()
        with open(backup_file) as f:
            backed_up_data = json.load(f)
        assert isinstance(backed_up_data, list)
        assert len(backed_up_data) == 0
    finally:
        # Clean up
        db.drop_collection("empty_collection")

def test_backup_complex_data_types(tmp_path, mongodb_client, setup_complex_collection):
    """Test backing up a collection with complex MongoDB data types."""
    # Perform backup
    backup_dir = tmp_path / "backup"
    result = backup_collection(mongodb_client, "test_db", "complex_collection", backup_dir)
    assert result is True
    
    # Verify backup file exists
    backup_file = backup_dir / "test_db" / "complex_collection.json"
    assert backup_file.exists()
    
    # Verify backup contents
    with open(backup_file) as f:
        backed_up_data = json.load(f)
    
    assert len(backed_up_data) == 2
    
    # Check first document
    doc1 = backed_up_data[0]
    assert doc1["_id"]["$type"] == "ObjectId"
    assert isinstance(doc1["_id"]["$value"], str)
    assert doc1["created_at"]["$type"] == "datetime"
    assert isinstance(doc1["created_at"]["$value"], str)
    assert isinstance(doc1["tags"], list)
    assert isinstance(doc1["nested"], dict)
    assert isinstance(doc1["number_types"], dict)
    
    # Check second document
    doc2 = backed_up_data[1]
    assert None in doc2["array_with_nulls"]
    assert len(doc2["empty_array"]) == 0
    assert len(doc2["empty_object"]) == 0
    assert doc2["null_field"] is None

def test_backup_nonexistent_collection(tmp_path, mongodb_client):
    """Test attempting to backup a collection that doesn't exist."""
    backup_dir = tmp_path / "backup"
    result = backup_collection(mongodb_client, "test_db", "nonexistent_collection", backup_dir)
    assert result is False
    
    # Verify no backup file was created
    backup_file = backup_dir / "test_db" / "nonexistent_collection.json"
    assert not backup_file.exists()

def test_backup_with_invalid_path(mongodb_client, setup_test_collection):
    """Test backup with invalid backup path."""
    # Try to backup to a file path instead of directory
    with open("invalid_path.txt", "w") as f:
        f.write("test")
    
    result = backup_collection(mongodb_client, "test_db", "test_collection", Path("invalid_path.txt"))
    assert result is False
    
    # Clean up
    Path("invalid_path.txt").unlink()

def test_get_collections_info(mongodb_client, setup_test_collection):
    """Test getting collection information from MongoDB."""
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

def test_backup_with_limited_permissions(tmp_path, mongodb_limited_user):
    """Test that backup works with minimal (read-only) permissions."""
    try:
        # Attempt backup
        backup_dir = tmp_path / "backup"
        result = backup_collection(mongodb_limited_user, "test_db", "test_collection", backup_dir)
        assert result is True
        
        # Verify backup file exists and contains data
        backup_file = backup_dir / "test_db" / "test_collection.json"
        assert backup_file.exists()
        
        with open(backup_file) as f:
            backed_up_data = json.load(f)
        
        assert len(backed_up_data) == 1
        assert backed_up_data[0]["test"] == "data"
        
    except Exception as e:
        pytest.fail(f"Backup with limited permissions failed: {e}")
