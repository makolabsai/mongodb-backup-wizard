import os
import uuid
import json
import time
import random
import string
import pytest
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Generator, Tuple
from pymongo import MongoClient
from rich.console import Console
from mongodb_backup import backup_collection
from mongodb_restore import restore_collection
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

# Set up console for nice output
console = Console()

def generate_random_document() -> Dict:
    """Generate a random document with various data types."""
    return {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "number": random.randint(1, 1000),
        "float": random.uniform(0, 100),
        "string": ''.join(random.choices(string.ascii_letters + string.digits, k=20)),
        "boolean": random.choice([True, False]),
        "array": [random.randint(1, 100) for _ in range(5)],
        "nested": {
            "field1": ''.join(random.choices(string.ascii_letters, k=10)),
            "field2": random.randint(1, 100),
            "field3": [random.choice(string.ascii_letters) for _ in range(3)]
        }
    }

def generate_test_data(num_documents: int = 100) -> List[Dict]:
    """Generate a list of random test documents."""
    return [generate_random_document() for _ in range(num_documents)]

def compare_documents(original: Dict, restored: Dict) -> bool:
    """Compare original and backed up documents, accounting for type conversions."""
    try:
        # Convert ObjectId to string in original document
        if '_id' in original:
            original = dict(original)  # Create a copy to avoid modifying the original
            original['_id'] = str(original['_id'])
            
        # Convert complex types to strings in original document
        for key, value in original.items():
            if isinstance(value, (list, dict)):
                original[key] = str(value)
                
        # Compare the documents
        return original == restored
    except Exception as e:
        console.print(f"[red]Error comparing documents: {str(e)}[/red]")
        console.print(f"Original: {original}")
        console.print(f"Backup: {restored}")
        return False

@pytest.fixture(scope="session")
def mongodb_container() -> Generator[DockerContainer, None, None]:
    """Fixture to provide MongoDB container."""
    console.print("\n[bold cyan]Starting MongoDB Container[/bold cyan]")
    
    container = DockerContainer("mongo:latest")
    container.with_exposed_ports(27017)
    container.with_env("MONGO_INITDB_ROOT_USERNAME", "root")
    container.with_env("MONGO_INITDB_ROOT_PASSWORD", "example")
    container.start()

    # Wait for MongoDB to become ready
    timeout = 10  # seconds
    start_time = time.time()
    
    while True:
        try:
            host = container.get_container_host_ip()
            port = container.get_exposed_port(27017)
            connection_url = f"mongodb://root:example@{host}:{port}"
            
            console.print(f"  • Attempting connection to {connection_url}")
            client = MongoClient(connection_url, serverSelectionTimeoutMS=5000)
            client.admin.command('ismaster')
            console.print("[green]✓ MongoDB is ready[/green]")
            client.close()
            break
        except Exception as e:
            if time.time() - start_time > timeout:
                console.print(f"[red]Error: {str(e)}[/red]")
                container.stop()
                raise TimeoutError(f"MongoDB container did not become ready in {timeout} seconds.") from e
            time.sleep(1)
    
    yield container
    
    console.print("\n[bold cyan]Stopping MongoDB Container[/bold cyan]")
    container.stop()
    console.print("[green]✓ MongoDB container stopped[/green]")

@pytest.fixture(scope="function")
def mongodb_client(mongodb_container: DockerContainer) -> Generator[MongoClient, None, None]:
    """Fixture to provide MongoDB client."""
    console.print("\n[bold cyan]Setting up MongoDB Connection[/bold cyan]")
    
    # Get connection details
    host = mongodb_container.get_container_host_ip()
    port = mongodb_container.get_exposed_port(27017)
    connection_url = f"mongodb://root:example@{host}:{port}"
    
    console.print(f"  • Connecting to: {connection_url}")
    client = MongoClient(connection_url, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    console.print("[green]✓ Connected to MongoDB successfully[/green]")
    
    yield client
    
    client.close()
    console.print("[green]✓ MongoDB connection closed[/green]")

@pytest.fixture(scope="function")
def test_collection(mongodb_client: MongoClient) -> Generator[Tuple[str, str, List[Dict]], None, None]:
    """Fixture to set up and tear down test collection with data."""
    # Generate unique IDs for test
    test_id = uuid.uuid4().hex
    test_db = "test_db"
    test_collection = f"test_{test_id[:8]}"
    
    console.print(f"\n[bold]Setting up test collection[/bold]")
    console.print(f"  • Database: {test_db}")
    console.print(f"  • Collection: {test_collection}")
    
    # Generate and insert test data
    console.print("\n[bold]Generating test data[/bold]")
    test_data = generate_test_data(100)
    
    # Insert data
    db = mongodb_client[test_db]
    collection = db[test_collection]
    insert_result = collection.insert_many(test_data)
    console.print(f"[green]✓ Successfully inserted {len(insert_result.inserted_ids)} documents[/green]")
    
    yield test_db, test_collection, test_data
    
    # Cleanup
    console.print(f"\n[bold]Cleaning up test collection[/bold]")
    collection.drop()
    console.print(f"[green]✓ Test collection {test_db}.{test_collection} dropped[/green]")

@pytest.fixture(scope="function")
def backup_directory() -> Generator[Path, None, None]:
    """Fixture to create and cleanup backup directory."""
    test_id = uuid.uuid4().hex
    backup_dir = Path(f"mongodb_backup_{test_id}")
    backup_dir.mkdir(exist_ok=True)
    
    console.print(f"\n[bold]Created backup directory[/bold]")
    console.print(f"  • Location: {backup_dir}")
    
    yield backup_dir
    
    # Cleanup
    console.print(f"\n[bold]Cleaning up backup directory[/bold]")
    shutil.rmtree(backup_dir)
    console.print(f"[green]✓ Backup directory {backup_dir} removed[/green]")

def test_backup_restore_workflow(
    mongodb_container: DockerContainer,
    mongodb_client: MongoClient,
    test_collection: Tuple[str, str, List[Dict]],
    backup_directory: Path
) -> None:
    """Test the complete backup and restore workflow."""
    test_db, collection_name, test_data = test_collection
    
    # Step 1: Run backup
    console.print("\n[bold]Step 1: Running backup[/bold]")
    console.print(f"  • Backing up {test_db}.{collection_name}...")
    backup_result = backup_collection(mongodb_client, test_db, collection_name, backup_directory)
    assert backup_result, "Backup operation failed"
    console.print("[green]✓ Backup completed[/green]")
    
    # Step 2: Verify backup files
    console.print("\n[bold]Step 2: Verifying backup files[/bold]")
    backup_file = backup_directory / test_db / f"{collection_name}.json"
    assert backup_file.exists(), f"Backup file not found: {backup_file}"
    
    # Read and verify backup data
    with open(backup_file) as f:
        backed_up_data = json.load(f)
    
    assert len(backed_up_data) == len(test_data), \
        f"Backup data count mismatch. Expected {len(test_data)}, got {len(backed_up_data)}"
    
    for i, (orig, backup) in enumerate(zip(test_data, backed_up_data)):
        assert compare_documents(orig, backup), f"Document mismatch at position {i}"
    console.print("[green]✓ All backup documents verified successfully[/green]")
    
    # Step 3: Drop the test collection for restore test
    console.print("\n[bold]Step 3: Preparing for restore[/bold]")
    mongodb_client[test_db][collection_name].drop()
    console.print("[green]✓ Test collection dropped[/green]")
    
    # Step 4: Run restore
    console.print("\n[bold]Step 4: Running restore[/bold]")
    restore_result = restore_collection(mongodb_client, backup_directory, test_db, collection_name)
    assert restore_result, "Restore operation failed"
    console.print("[green]✓ Restore completed[/green]")
    
    # Step 5: Verify restored data
    console.print("\n[bold]Step 5: Verifying restored data[/bold]")
    restored_docs = list(mongodb_client[test_db][collection_name].find({}))
    
    assert len(restored_docs) == len(test_data), \
        f"Restored data count mismatch. Expected {len(test_data)}, got {len(restored_docs)}"
    
    for i, (orig, restored) in enumerate(zip(test_data, restored_docs)):
        assert compare_documents(orig, restored), f"Document mismatch at position {i}"
    console.print("[green]✓ All restored documents verified successfully[/green]")
