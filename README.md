# MongoDB Backup Wizard

An interactive command-line tool for backing up and restoring MongoDB collections with progress tracking and rich console output.

## Features

- Backup MongoDB collections to JSON files
- Restore collections from JSON backups
- Interactive console mode with collection selection
- Progress bars for backup and restore operations
- Rich console output with detailed status information
- Support for various data types (ObjectId, datetime, etc.)
- Smart backup selection with timestamps and size information
- Collection statistics (document count, size)
- Automatic backup directory management
- Safety checks for existing collections during restore
- Detailed logging with timestamps

## Installation

### From PyPI (Recommended)

```bash
pip install mongowiz
```

### From Source

1. Clone the repository:

```bash
git clone https://github.com/makolabsai/mongodb-backup-wizard.git
cd mongodb-backup-wizard
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
```

3. Install in development mode:

```bash
pip install -e .
```

## Usage

### Environment Setup

Set your MongoDB connection URL as an environment variable:

```bash
export MONGODB_URL="mongodb://username:password@localhost:27017"
# or for Windows:
# set MONGODB_URL=mongodb://username:password@localhost:27017
```

You can also create a `.env` file in your working directory:

```
MONGODB_URL=mongodb://username:password@localhost:27017
```

### Command-line Usage

After installation, you can run the wizard using:

```bash
mongowiz
```

This will start the interactive wizard that guides you through:
- Choosing between backup and restore operations
- Selecting databases and collections
- Choosing backup locations
- Monitoring progress with rich console output

### Python API Usage

You can also use the package programmatically in your Python code:

```python
from pymongo import MongoClient
from mongowiz.core.backup import backup_collection
from mongowiz.core.restore import restore_collection
from pathlib import Path

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")

# Backup a collection
backup_dir = Path("my_backups")
backup_collection(client, "my_database", "my_collection", backup_dir)

# Restore a collection
restore_collection(client, backup_dir, "my_database", "my_collection")
```

### Backup Directory Structure

Backups are organized in a `backups` directory within your project:

```
backups/
└── mongodb_backup_YYYYMMDD_HHMMSS/
    ├── database_name/
    │   └── collection_name.json
    └── ...
```

Each backup includes:
- Timestamp in the directory name
- Separate directories for each database
- JSON files for each collection
- Collection metadata and statistics

By default, backups are stored in the `backups` directory, but you can specify a different location during the backup process.

## Project Structure

```
mongowiz/
├── core/               # Core functionality
│   ├── backup.py      # Backup operations
│   └── restore.py     # Restore operations
├── ui/                # User interface
│   └── wizard.py      # Interactive CLI wizard
├── utils/             # Utility functions
└── __main__.py        # Entry point

tests/
├── core/              # Core functionality tests
└── ui/                # UI tests
```

## Development

1. Clone the repository and install development dependencies:

```bash
git clone https://github.com/makolabsai/mongodb-backup-wizard.git
cd mongodb-backup-wizard
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
pip install -e ".[dev]"
```

2. Run tests:

```bash
pytest
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
