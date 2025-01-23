"""Tests for the command-line wizard interface."""

import json
from pathlib import Path
import pytest
from unittest.mock import patch, MagicMock
from mongowiz.ui.wizard import (
    get_backups_dir,
    get_backup_location,
    get_backup_folders,
    format_backup_choice,
    select_backup_collection,
    select_restore_collection,
)

def test_get_backups_dir(tmp_path):
    """Test getting/creating backups directory."""
    with patch("pathlib.Path.cwd", return_value=tmp_path):
        backups_dir = get_backups_dir()
        assert backups_dir.exists()
        assert backups_dir.is_dir()
        assert backups_dir.name == "backups"

def test_get_backup_location(tmp_path):
    """Test getting backup location with default and custom paths."""
    with patch("pathlib.Path.cwd", return_value=tmp_path):
        # Mock questionary to return default value
        with patch("questionary.text") as mock_text:
            mock_text.return_value.ask.return_value = str(tmp_path / "backups" / "mongodb_backup_20250123_120000")
            location = get_backup_location()
            assert location.parent.name == "backups"
            assert location.name.startswith("mongodb_backup_")
        
        # Mock questionary to return custom value
        with patch("questionary.text") as mock_text:
            mock_text.return_value.ask.return_value = str(tmp_path / "custom_backup")
            location = get_backup_location()
            assert location.name == "custom_backup"

def test_get_backup_folders(tmp_path):
    """Test getting and sorting backup folders."""
    with patch("pathlib.Path.cwd", return_value=tmp_path):
        backups_dir = tmp_path / "backups"
        backups_dir.mkdir()
        
        # Create some backup folders
        folders = [
            "mongodb_backup_20250123_120000",
            "mongodb_backup_20250123_110000",
            "mongodb_backup_20250122_120000"
        ]
        
        for folder in folders:
            (backups_dir / folder).mkdir()
        
        # Add a non-backup folder
        (backups_dir / "other_folder").mkdir()
        
        # Get backup folders
        backup_folders = get_backup_folders()
        
        # Check sorting (newest first)
        assert len(backup_folders) == 3
        assert backup_folders[0].name == folders[0]
        assert backup_folders[-1].name == folders[-1]

def test_format_backup_choice(tmp_path):
    """Test formatting backup folder for display."""
    backup_folder = tmp_path / "mongodb_backup_20250123_120000"
    backup_folder.mkdir(parents=True)
    
    # Create some sample backup content
    db_dir = backup_folder / "test_db"
    db_dir.mkdir()
    
    with open(db_dir / "collection1.json", "w") as f:
        json.dump([{"test": "data"}], f)
    with open(db_dir / "collection2.json", "w") as f:
        json.dump([{"test": "data"}], f)
    
    # Format the choice
    choice_text = format_backup_choice(backup_folder)
    
    assert "2025-01-23 12:00:00" in choice_text
    assert "1 DBs" in choice_text
    assert "2 collections" in choice_text

def test_select_backup_collection():
    """Test selecting a collection for backup."""
    collections_info = {
        "db1": [
            ("coll1", 100, 1024),
            ("coll2", 200, 2048)
        ],
        "db2": [
            ("coll3", 300, 4096)
        ]
    }
    
    # Mock questionary to select first collection
    with patch("questionary.select") as mock_select:
        mock_select.return_value.ask.return_value = "db1.coll1"
        db_name, coll_name = select_backup_collection(collections_info)
        assert db_name == "db1"
        assert coll_name == "coll1"
        
        # Verify choices format
        choices = mock_select.call_args[1]["choices"]
        assert len(choices) == 3
        assert "db1.coll1" in choices[0]["value"]
        assert "100" in choices[0]["name"]
        assert "1024" in choices[0]["name"] or "1.0 KB" in choices[0]["name"]

def test_select_restore_collection():
    """Test selecting a collection for restore."""
    collections_info = {
        "db1": [
            ("coll1", 100, 1024),
            ("coll2", 200, 2048)
        ]
    }
    
    # Mock questionary to select second collection
    with patch("questionary.select") as mock_select:
        mock_select.return_value.ask.return_value = "db1.coll2"
        db_name, coll_name = select_restore_collection(collections_info)
        assert db_name == "db1"
        assert coll_name == "coll2"
        
        # Verify choices format
        choices = mock_select.call_args[1]["choices"]
        assert len(choices) == 2
        assert "db1.coll2" in choices[1]["value"]
        assert "200" in choices[1]["name"]
        assert "2048" in choices[1]["name"] or "2.0 KB" in choices[1]["name"]
