# Baal Orchestration

A Dagster-based orchestration repository for data pipelines.

## Setup

1. **Install Python 3.12** (if not already installed)

2. **Install dependencies:**
   ```powershell
   pip install -e .
   ```

3. **Install development dependencies (optional):**
   ```powershell
   pip install -e ".[dev]"
   ```

## Running Dagster

1. **Start the Dagster web server:**
   ```powershell
   dagster dev
   ```

2. **Open your browser and navigate to:**
   ```
   http://localhost:3000
   ```

## Project Structure

```
orchestration/
├── orchestration/
│   ├── __init__.py          # Main Dagster definitions
│   ├── assets.py            # Asset definitions
│   ├── jobs.py              # Job definitions
│   ├── schedules.py         # Schedule definitions
│   ├── sensors.py           # Sensor definitions
│   └── resources.py         # Resource definitions
├── workspace.yaml           # Dagster workspace configuration
├── pyproject.toml          # Python project configuration
└── README.md               # This file
```

## Getting Started

The project comes with example assets, jobs, schedules, and sensors. You can:

1. View and materialize assets in the Dagster UI
2. Run jobs manually or through schedules
3. Monitor pipeline execution and logs
4. Explore the asset lineage graph

## Development

- **Format code:** `black .`
- **Sort imports:** `isort .`
- **Lint code:** `ruff check .`
- **Type checking:** `mypy orchestration/`
- **Run tests:** `pytest`

## Next Steps

1. Replace the example assets with your actual data sources and transformations
2. Configure proper resources (databases, APIs, file systems)
3. Set up appropriate schedules and sensors for your use case
4. Add data quality checks and alerting
5. Configure proper logging and monitoring
