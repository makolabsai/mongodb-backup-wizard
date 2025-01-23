"""Command-line entry point for MongoDB Backup Wizard."""

import os
import sys
from pathlib import Path
import questionary
from pymongo import MongoClient
from dotenv import load_dotenv

from .ui.wizard import run_backup_wizard, run_restore_wizard
from rich.console import Console

console = Console()

def main():
    """Main entry point for the MongoDB Backup Wizard."""
    # Load environment variables
    load_dotenv()
    
    # Get MongoDB connection
    mongodb_url = os.getenv("MONGODB_URL")
    if not mongodb_url:
        console.print("[red]Error: MONGODB_URL environment variable not set[/red]")
        sys.exit(1)
    
    try:
        client = MongoClient(mongodb_url)
        
        # Ask user what they want to do
        action = questionary.select(
            "What would you like to do?",
            choices=[
                {"name": "Backup a collection", "value": "backup"},
                {"name": "Restore a collection", "value": "restore"},
                {"name": "Exit", "value": "exit"}
            ]
        ).ask()
        
        if action == "backup":
            run_backup_wizard(client)
        elif action == "restore":
            run_restore_wizard(client)
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)
    finally:
        client.close()

if __name__ == "__main__":
    main()
