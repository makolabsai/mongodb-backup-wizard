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

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Environment Setup

Set your MongoDB connection URL as an environment variable:

```bash
export MONGODB_URL="mongodb://username:password@localhost:27017"
# or for Windows:
# set MONGODB_URL=mongodb://username:password@localhost:27017
```

You can also create a `.env` file in the project directory:

```
MONGODB_URL=mongodb://username:password@localhost:27017
```

### Interactive Mode

Simply run the scripts without arguments to enter interactive mode:

```bash
python mongodb_backup.py
```

In interactive mode, you can:

- View all available databases and collections
- See collection sizes and document counts
- Select which collection to backup
- Choose backup location
- Monitor backup progress with a rich progress bar

For restore:

```bash
python mongodb_restore.py
```

Restore features:

- Browse backups by timestamp
- View backup statistics (databases, collections, total size)
- Preview collection contents before restore
- Safety checks for existing collections
- Progress tracking during restore

### Command-line Mode

If you prefer to run without interaction, use command-line arguments:

#### Backup a Collection

```bash
python mongodb_backup.py --database your_database --collection your_collection --output ./backups
```

Options:

- `--database` or `-d`: MongoDB database name
- `--collection` or `-c`: Collection name to backup
- `--output` or `-o`: Output directory for backup files (default: ./backups)

#### Restore a Collection

```bash
python mongodb_restore.py --database your_database --collection your_collection --input ./backups
```

Options:

- `--database` or `-d`: Target MongoDB database name
- `--collection` or `-c`: Target collection name
- `--input` or `-i`: Input directory containing backup files (default: ./backups)

### Example Usage

1. Interactive backup:

```bash
python mongodb_backup.py
# Follow the prompts to select database, collection, and backup location
```

2. Direct backup:

```bash
python mongodb_backup.py -d myapp -c users -o ./my_backups
```

3. Interactive restore:

```bash
python mongodb_restore.py
# Follow the prompts to select backup and target collection
```

4. Direct restore:

```bash
python mongodb_restore.py -d myapp -c users -i ./my_backups
```

The tool will show progress bars and detailed information during the backup/restore process.

### Backup Directory Structure

Backups are organized as follows:

```
mongodb_backup_YYYYMMDD_HHMMSS/
├── database_name/
│   └── collection_name.json
└── ...
```

Each backup includes:

- Timestamp in the directory name
- Separate directories for each database
- JSON files for each collection
- Collection metadata and statistics

## Development

### Requirements

- Python 3.8+
- MongoDB 4.0+
- Docker (for running tests)

### Running Tests

The test suite uses pytest and testcontainers to run against a real MongoDB instance:

```bash
pytest -v -s test_backup_restore.py
```

The tests verify:

- Backup and restore functionality
- Data integrity
- Various data types (ObjectId, datetime, arrays, nested documents)
- Error handling
- Container management

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
