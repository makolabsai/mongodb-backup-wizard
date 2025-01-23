import os
import json
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient
import logging
from dotenv import load_dotenv
import questionary
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
import humanize
from typing import Dict, List, Tuple

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set up rich console
console = Console()

def get_backup_folders():
    """Get all backup folders sorted by date (newest first)."""
    current_dir = Path.cwd()
    backup_folders = [
        d for d in current_dir.iterdir()
        if d.is_dir() and d.name.startswith('mongodb_backup_')
    ]
    return sorted(backup_folders, reverse=True)

def format_backup_choice(folder):
    """Format backup folder for selection menu with additional info."""
    try:
        # Parse the timestamp from folder name
        timestamp = datetime.strptime(folder.name.replace('mongodb_backup_', ''), '%Y%m%d_%H%M%S')
        
        # Count databases and collections
        db_count = sum(1 for x in folder.iterdir() if x.is_dir())
        collection_count = sum(1 for x in folder.rglob("*.json"))
        
        # Calculate total size
        total_size = sum(f.stat().st_size for f in folder.rglob("*.json"))
        
        return f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')} ({db_count} DBs, {collection_count} collections, {humanize.naturalsize(total_size)})"
    except Exception as e:
        return str(folder.name)

def get_collections_info(backup_folder: Path) -> Dict[str, List[Tuple[str, int, int]]]:
    """Get information about all collections in the backup.
    Returns a dict of db_name -> list of (collection_name, doc_count, size)"""
    collections_info = {}
    
    for db_path in backup_folder.iterdir():
        if not db_path.is_dir():
            continue
            
        db_name = db_path.name
        collections_info[db_name] = []
        
        for coll_file in db_path.glob("*.json"):
            try:
                size = coll_file.stat().st_size
                # Read file line by line to count documents
                doc_count = 0
                with open(coll_file) as f:
                    content = f.read().strip()
                    if content:  # Skip empty files
                        try:
                            docs = json.loads(content)
                            if isinstance(docs, list):
                                doc_count = len(docs)
                            else:
                                console.print(f"[yellow]Warning: {coll_file} does not contain a list of documents[/yellow]")
                                continue
                        except json.JSONDecodeError as e:
                            console.print(f"[red]Error reading {coll_file}: {str(e)}[/red]")
                            continue
                
                collections_info[db_name].append((coll_file.stem, doc_count, size))
            except Exception as e:
                console.print(f"[red]Error processing {coll_file}: {str(e)}[/red]")
                continue
    
    return collections_info

def select_collection(collections_info: Dict[str, List[Tuple[str, int, int]]]) -> Tuple[str, str]:
    """Let user select which collection to restore."""
    # Prepare all collections as choices
    choices = []
    for db_name, collections in collections_info.items():
        for coll_name, doc_count, size in collections:
            choice_text = f"{db_name}.{coll_name} ({doc_count:,} docs, {humanize.naturalsize(size)})"
            choices.append({"name": choice_text, "value": f"{db_name}.{coll_name}"})
    
    # Ask user to select a collection
    selected = questionary.select(
        "Select a collection to restore",
        choices=choices
    ).ask()
    
    if not selected:
        return None, None
    
    return selected.split('.')

def check_existing_collections(client, backup_folder, selected_collections):
    """Check for collections that would clash with the backup."""
    clashing_collections = []
    
    for db_name, collections in selected_collections.items():
        db = client[db_name]
        existing_collections = set(db.list_collection_names())
        
        clashes = existing_collections.intersection(set(collections))
        if clashes:
            clashing_collections.extend([f"{db_name}.{coll}" for coll in clashes])
    
    return clashing_collections

def print_question_history(history):
    """Print the history of questions and answers."""
    console.clear()
    console.print("\n[bold blue]MongoDB Restore[/bold blue]")
    console.print("=" * 50)
    for q, a in history:
        console.print(f"\n[cyan]Q: {q}[/cyan]")
        console.print(f"[green]A: {a}[/green]")
    console.print("\n" + "=" * 50 + "\n")

def restore_collection(client, backup_path: Path, db_name: str, collection_name: str) -> bool:
    """Execute the restore operation with the given parameters."""
    try:
        # Connect and check for conflicts
        client.admin.command('ping')  # Test connection
        
        # Check for conflicts
        db = client[db_name]
        if collection_name in db.list_collection_names():
            console.print("\n[red]Warning: This collection already exists. Please backup and drop it first if you want to proceed.[/red]")
            return False
            
        # Read and restore data
        collection_file = backup_path / db_name / f"{collection_name}.json"
        if not collection_file.exists():
            console.print(f"[red]Error: Backup file not found: {collection_file}[/red]")
            return False
            
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            try:
                # Read the backup file
                with open(collection_file) as f:
                    content = f.read().strip()
                    if content:
                        documents = json.loads(content)
                        if documents:  # Only create collection if there's data
                            collection = db[collection_name]
                            
                            # Create progress task
                            task = progress.add_task(
                                f"[cyan]Restoring {db_name}.{collection_name}...",
                                total=len(documents)
                            )
                            
                            # Insert in batches
                            batch_size = 1000
                            for i in range(0, len(documents), batch_size):
                                batch = documents[i:i + batch_size]
                                collection.insert_many(batch)
                                progress.update(task, advance=len(batch))
                            
                            console.print(f"[green]✓ Restored {len(documents)} documents[/green]")
                            return True
                            
            except Exception as e:
                console.print(f"[red]Error during restore: {str(e)}[/red]")
                return False
                
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        return False

def restore_backup():
    """Main function to restore a MongoDB backup."""
    # Keep track of questions and answers
    question_history = []
    
    try:
        # Get MongoDB connection string
        mongodb_url = os.getenv('MONGODB_URL')
        if not mongodb_url:
            console.print("[red]Error: MONGODB_URL environment variable is not set[/red]")
            return
        
        # Get available backups
        backup_folders = get_backup_folders()
        if not backup_folders:
            console.print("[red]Error: No backup folders found[/red]")
            return
        
        # Create backup selection choices
        backup_choices = {format_backup_choice(f): f for f in backup_folders}
        
        # Get user's backup selection
        print_question_history(question_history)
        selected_backup_name = questionary.select(
            "Select a backup to restore",
            choices=list(backup_choices.keys())
        ).ask()
        
        if not selected_backup_name:
            console.print("[yellow]No backup selected. Exiting...[/yellow]")
            return
        
        question_history.append(("Select a backup to restore", selected_backup_name))
        selected_backup = backup_choices[selected_backup_name]
        
        # Get collection information
        collections_info = get_collections_info(selected_backup)
        if not collections_info:
            console.print("[red]Error: No valid collections found in backup[/red]")
            return
        
        # Let user select collections
        print_question_history(question_history)
        db_name, collection_name = select_collection(collections_info)
        if not db_name or not collection_name:
            console.print("[yellow]No collection selected. Exiting...[/yellow]")
            return
        
        collections_summary = f"{db_name}.{collection_name}"
        question_history.append(("Selected collection", collections_summary))
        
        # Connect to MongoDB
        client = MongoClient(mongodb_url)
        
        # Execute restore
        restore_collection(client, selected_backup, db_name, collection_name)
        
    except Exception as e:
        console.print(f"[red]Error during restore: {str(e)}[/red]")
    finally:
        client.close()

if __name__ == "__main__":
    restore_backup()
