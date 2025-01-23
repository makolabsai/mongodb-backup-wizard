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

def select_backup_collection(collections_info: Dict[str, List[Tuple[str, int, int]]]) -> Tuple[str, str]:
    """Let user select which collection to backup."""
    # Prepare all collections as choices
    choices = []
    for db_name, collections in collections_info.items():
        for coll_name, doc_count, size in collections:
            choice_text = f"{db_name}.{coll_name} ({doc_count:,} docs, {humanize.naturalsize(size)})"
            choices.append({"name": choice_text, "value": f"{db_name}.{coll_name}"})
    
    # Ask user to select a collection
    selected = questionary.select(
        "Select a collection to backup",
        choices=choices
    ).ask()
    
    if not selected:
        return None, None
    
    return selected.split('.')

def get_backup_location() -> Path:
    """Get the backup location from user or use default."""
    default_location = Path.cwd() / f"mongodb_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Ask for backup location
    location = questionary.text(
        "Enter backup location (press Enter for default)",
        default=str(default_location)
    ).ask()
    
    return Path(location)

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
        return str(folder)

def select_restore_collection(collections_info: Dict[str, List[Tuple[str, int, int]]]) -> Tuple[str, str]:
    """Let user select which collection to restore."""
    choices = []
    for db_name, collections in collections_info.items():
        for coll_name, doc_count, size in collections:
            choice_text = f"{db_name}.{coll_name} ({doc_count:,} docs, {humanize.naturalsize(size)})"
            choices.append({"name": choice_text, "value": f"{db_name}.{coll_name}"})
    
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

        # Select collection
        db_name, collection_name = select_backup_collection(collections_info)
        if not db_name or not collection_name:
            console.print("[yellow]Backup cancelled[/yellow]")
            return False

        # Get backup location
        backup_dir = get_backup_location()
        
        # Execute backup
        return backup_collection(client, db_name, collection_name, backup_dir)
    
    except Exception as e:
        console.print(f"[red]Error during backup: {str(e)}[/red]")
        return False

def run_restore_wizard(client):
    """Run the interactive restore wizard."""
    try:
        # Get available backups
        backup_folders = get_backup_folders()
        if not backup_folders:
            console.print("[red]No backup folders found[/red]")
            return False

        # Format choices
        choices = [
            {"name": format_backup_choice(folder), "value": str(folder)}
            for folder in backup_folders
        ]

        # Select backup
        selected_backup = questionary.select(
            "Select a backup to restore from",
            choices=choices
        ).ask()

        if not selected_backup:
            console.print("[yellow]Restore cancelled[/yellow]")
            return False

        backup_path = Path(selected_backup)
        
        # Get collections in backup
        collections_info = get_backup_collections_info(backup_path)
        if not collections_info:
            console.print("[red]No collections found in backup[/red]")
            return False

        # Select collection
        db_name, collection_name = select_restore_collection(collections_info)
        if not db_name or not collection_name:
            console.print("[yellow]Restore cancelled[/yellow]")
            return False

        # Execute restore
        return restore_collection(client, backup_path, db_name, collection_name)

    except Exception as e:
        console.print(f"[red]Error during restore: {str(e)}[/red]")
        return False
