
# EE547 HW3 - Database Systems

**Name:** Vivin Thiyagarajan  
**USC Email:** vthiyaga@usc.edu

## Setup

**Problem 1:** Standard Docker Compose setup - see `problem1/README.md`  
**Problem 2:** AWS Region: us-west-2 | EC2: http://52.15.129.32:8080

## Design Decisions

**Problem 1:**
- Surrogate keys for lines/stops (stable IDs), natural key for trips
- Removed UNIQUE(line_id, stop_id) constraint - lines can revisit same stop
- Removed UNIQUE(vehicle_id, scheduled_departure) - dataset has valid duplicates

**Problem 2:**
- Denormalized schema: 4.0x storage multiplication (each paper → 4+ items)
- 3 GSIs for different access patterns (author, paper ID, keyword)
- Trade-off: Fast queries (<200ms) vs increased storage and write complexity

