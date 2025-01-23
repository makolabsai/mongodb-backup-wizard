"""Interactive command-line wizard for MongoDB backup and restore operations."""

import os
from datetime import datetime
from pathlib import Path
import questionary
from rich.console import Console
import humanize
from typing import Dict, List, Tuple

from ..core.backup import backup_collection, get_collections_info as get_source_collections_info
from ..core.restore import restore_collection, get_collections_info as get_backup_collections_info

# Set up rich console
console = Console()

def get_backups_dir() -> Path:
    """Get or create the backups directory."""
    backups_dir = Path.cwd() / "backups"
    backups_dir.mkdir(exist_ok=True)
    return backups_dir

def select_backup_collection(collections_info: Dict[str, List[Tuple[str, int, int]]]) -> Tuple[str, str]:
    """Let user select which collection to backup."""
    # Prepare all collections as choices
    choices = []
    for db_name, collections in collections_info.items():
        for collection_info in collections:
            choice = format_collection_choice(db_name, collection_info)
            choices.append(choice)
    
    # Ask user to select a collection
    selected = questionary.select(
        "Select a collection to backup",
        choices=choices
    ).ask()
    
    if not selected:
        return None, None
    
    return selected.split('.')

def format_collection_choice(db_name: str, collection_info: Tuple[str, int, int]) -> dict:
    """Format collection information for display in questionary choices."""
    collection_name, doc_count, size = collection_info
    size_str = f"{size / 1024:.1f} KB" if size >= 1024 else f"{size} bytes"
    display = f"{db_name}.{collection_name} ({doc_count} docs, {size_str})"
    value = f"{db_name}.{collection_name}"
    return {"name": display, "value": value}

def get_backup_location() -> Path:
    """Get the backup location from user or use default."""
    backups_dir = get_backups_dir()
    default_location = backups_dir / f"mongodb_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Ask for backup location
    location = questionary.text(
        "Enter backup location (press Enter for default)",
        default=str(default_location)
    ).ask()
    
    backup_path = Path(location)
    
    # If user provided a relative path, make it relative to backups directory
    if not backup_path.is_absolute():
        backup_path = backups_dir / backup_path
        
    return backup_path

def get_backup_folders():
    """Get all backup folders sorted by date (newest first)."""
    backups_dir = get_backups_dir()
    backup_folders = [
        d for d in backups_dir.iterdir()
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
        return str(folder)

def select_restore_collection(collections_info: Dict[str, List[Tuple[str, int, int]]]) -> Tuple[str, str]:
    """Let user select which collection to restore."""
    choices = []
    for db_name, collections in collections_info.items():
        for collection_info in collections:
            choice = format_collection_choice(db_name, collection_info)
            choices.append(choice)
    
    selected = questionary.select(
        "Select a collection to restore",
        choices=choices
    ).ask()
    
    if not selected:
        return None, None
    
    return selected.split('.')

def run_backup_wizard(client):
    """Run the interactive backup wizard."""
    try:
        # Get collections info
        collections_info = get_source_collections_info(client)
        if not collections_info:
            console.print("[red]No collections found to backup[/red]")
            return False
        
        # Let user select collection
        db_name, collection_name = select_backup_collection(collections_info)
        if not db_name or not collection_name:
            return False
            
        # Get backup location
        backup_path = get_backup_location()
        
        # Perform backup
        console.print(f"\nBacking up {db_name}.{collection_name} to {backup_path}")
        if backup_collection(client, db_name, collection_name, backup_path):
            console.print("[green]Backup completed successfully![/green]")
            return True
        else:
            console.print("[red]Backup failed![/red]")
            return False
            
    except Exception as e:
        console.print(f"[red]Error during backup: {str(e)}[/red]")
        return False

def run_restore_wizard(client):
    """Run the interactive restore wizard."""
    try:
        # Get available backups
        backup_folders = get_backup_folders()
        if not backup_folders:
            console.print("[red]No backups found![/red]")
            return False
        
        # Let user select backup folder
        choices = [{"name": format_backup_choice(f), "value": f} for f in backup_folders]
        selected_folder = questionary.select(
            "Select a backup to restore from",
            choices=choices
        ).ask()
        
        if not selected_folder:
            return False
            
        # Get collections in backup
        collections_info = get_backup_collections_info(selected_folder)
        if not collections_info:
            console.print("[red]No collections found in backup![/red]")
            return False
            
        # Let user select collection
        db_name, collection_name = select_restore_collection(collections_info)
        if not db_name or not collection_name:
            return False
            
        # Confirm restore
        if not questionary.confirm(
            f"Are you sure you want to restore {db_name}.{collection_name}?",
            default=False
        ).ask():
            return False
            
        # Perform restore
        console.print(f"\nRestoring {db_name}.{collection_name} from {selected_folder}")
        if restore_collection(client, selected_folder, db_name, collection_name):
            console.print("[green]Restore completed successfully![/green]")
            return True
        else:
            console.print("[red]Restore failed![/red]")
            return False
            
    except Exception as e:
        console.print(f"[red]Error during restore: {str(e)}[/red]")
        return False
