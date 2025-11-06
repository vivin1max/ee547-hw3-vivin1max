# Problem 1: Metro Transit Database

PostgreSQL database for transit schedules with Docker setup.

## Quick Start

```bash
./build.sh   # Build Docker images
./run.sh     # Load data and test
./test.sh    # Run all queries
```

Adminer UI: http://localhost:8080 (Server: db, User: transit, Password: transit123)

## Schema Decisions

**Natural vs Surrogate Keys:**
- Used surrogate keys (line_id, stop_id) because line/stop names could change and numeric IDs are faster for joins
- Kept trip_id natural since it's already unique and stable in the data
- Composite keys for line_stops and stop_events to enforce one-to-many relationships

**Constraints:**
- CHECK constraints: vehicle_type in ('rail','bus'), lat/lon ranges, non-negative sequences
- UNIQUE on line_name and stop_name for easy lookups
- Intentionally skipped UNIQUE(line_id, stop_id) because lines can revisit the same stop

**Complex Query:**
Q10 was hardest - needed two-level aggregation to compare each stop's ridership against the overall average. Getting the CTE structure right took some thinking.

**Foreign Keys:**
They prevent:
- Trips referencing non-existent lines
- Stop events for stops that don't exist
- Orphaned line_stops when a line is deleted (CASCADE handles this)

**Why Relational:**
Transit data is highly structured with clear relationships. SQL's joins make route analysis natural, and constraints keep schedules consistent. The normalized design scales well and queries are fast with proper indexes.

