# Contributing to MongoDB Backup and Restore Tool

First off, thank you for considering contributing to this project! 

## Development Process

1. Fork the repository
2. Create a new branch for your feature
3. Make your changes
4. Run the test suite
5. Submit a Pull Request

## Setting Up Development Environment

1. Clone your fork:
```bash
git clone https://github.com/yourusername/forty-two-migration.git
cd forty-two-migration
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running Tests

The test suite requires Docker to be running on your machine:

```bash
pytest -v -s test_backup_restore.py
```

## Code Style

- Follow PEP 8 guidelines
- Use type hints
- Write docstrings for functions and classes
- Keep functions focused and small
- Add comments for complex logic

## Pull Request Process

1. Update the README.md with details of changes if needed
2. Update the requirements.txt if you add dependencies
3. Add tests for new functionality
4. Ensure all tests pass
5. Update documentation if needed

## Code of Conduct

### Our Standards

* Using welcoming and inclusive language
* Being respectful of differing viewpoints and experiences
* Gracefully accepting constructive criticism
* Focusing on what is best for the community
* Showing empathy towards other community members

### Our Responsibilities

Project maintainers are responsible for clarifying the standards of acceptable behavior and are expected to take appropriate and fair corrective action in response to any instances of unacceptable behavior.

## Questions?

Feel free to open an issue for any questions or concerns.
