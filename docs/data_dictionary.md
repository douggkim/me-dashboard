# Data Dictionary - Doug Dashboard

**Generated**: 2025-09-07  
**Source**: DuckDB Analysis Database  
**Database**: `/data/testing/analysis.duckdb`

## Overview

This data dictionary documents the available datasets in the doug-dashboard project, generated through automated analysis using the DuckDB MCP subagent. The project follows a medallion architecture (bronze/silver/gold) with personal data from various sources including fitness tracking and location services.

## Available Tables Summary

| Table | Layer | Domain | Rows | Columns | Description |
|-------|-------|--------|------|---------|-------------|
| `workout_data_bronze` | Bronze | Fitness | 7 | 17 | Raw workout activity data |
| `location_data_bronze` | Bronze | Location | 1 | 1 | Raw GPS location tracking data |

## Table Schemas

### workout_data_bronze
**Source**: Bronze Layer - `/data/testing/bronze/workout/`  
**Location**: CSV files in nested date directories  
**Update Frequency**: On-demand (manual data collection)

#### Schema Details
The workout data appears to have parsing issues with the current CSV structure, showing generic column names and boundary markers. This suggests the CSV file format may need preprocessing or has embedded multipart content.

**Identified Issues**:
- Column names are generic (`column01`, `column02`, etc.)
- First column contains boundary markers suggesting multipart HTTP content
- Data may need preprocessing before analysis

#### Recommendations
1. Investigate source CSV structure
2. Implement custom parsing logic for workout data
3. Define proper column mappings for fitness metrics

### location_data_bronze
**Source**: Bronze Layer - `/data/testing/bronze/location/`  
**Location**: JSON files in nested date directories  
**Update Frequency**: Real-time GPS collection

#### Schema
| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| locations | STRUCT[] | Yes | Array of location points with nested properties |

#### Nested Structure
The `locations` column contains structured data with:
- **type**: Geographic feature type
- **geometry**: GeoJSON geometry with coordinates
- **properties**: Rich metadata including:
  - `timestamp`: When location was recorded
  - `speed`, `course`, `altitude`: Movement metrics
  - `horizontal_accuracy`, `vertical_accuracy`: GPS precision
  - `battery_level`, `battery_state`: Device status
  - `device_id`: Device identifier
  - `motion`: Array of motion states
  - `wifi`: WiFi network information

#### Sample Structure
```json
{
  "type": "Feature",
  "geometry": {
    "type": "Point", 
    "coordinates": [longitude, latitude]
  },
  "properties": {
    "timestamp": "2025-08-10T...",
    "speed": 0,
    "horizontal_accuracy": 5,
    "battery_level": 0.85,
    "device_id": "...",
    "altitude": 100
  }
}
```

## Data Quality Assessment

### Coverage Analysis
- **Location Data**: Good structure with comprehensive GPS metadata
- **Workout Data**: Requires data preprocessing due to format issues

### Data Freshness
- Latest location data: 2025-08-10 (based on file paths)
- Latest workout data: 2025-09-07 (based on file paths)

### Completeness
- Location: 1 record loaded (low sample size)
- Workout: 7 records loaded (limited dataset)

## Domain Analysis

### Location Domain
**Business Context**: Personal location tracking for movement analysis and geographic insights.

**Key Capabilities**:
- GPS coordinate tracking with accuracy metrics
- Movement pattern analysis (speed, course)
- Device and battery monitoring
- WiFi network association
- High-precision temporal tracking

**Use Cases**:
- Movement pattern analysis
- Location-based activity correlation
- Battery usage optimization
- GPS accuracy monitoring

### Fitness Domain
**Business Context**: Personal fitness and workout activity tracking.

**Current Limitations**:
- Data format requires preprocessing
- Schema definition needed for proper analysis
- Column mapping required for meaningful insights

**Potential Capabilities** (after preprocessing):
- Workout duration and intensity tracking
- Activity type classification
- Performance trend analysis
- Health metric correlation

## Integration Notes

### Data Loading Process
- Bronze layer data loaded from nested date-structured directories
- Automated discovery of CSV and JSON files
- Error handling for format inconsistencies
- Data catalog generation for metadata tracking

### MCP Integration
- DuckDB MCP server configured at `./data/testing/analysis.duckdb`
- Real-time query capabilities through `mcp__duckdb-data-agent__query`
- Schema introspection and data profiling support
- Extensible for additional data sources

### Next Steps
1. **Workout Data Preprocessing**: Implement custom parsing for fitness data
2. **Silver Layer Integration**: Add Delta Lake table loading for processed data
3. **Schema Validation**: Define proper schemas for all domains
4. **Data Quality Monitoring**: Automated checks for completeness and accuracy
5. **Documentation Automation**: Schedule regular data dictionary updates

## Technical Configuration

### DuckDB Extensions
- `spatial`: Geographic data processing
- `delta`: Delta Lake table support

### File Locations
- **Database**: `/data/testing/analysis.duckdb`
- **Bronze Data**: `/data/testing/bronze/{domain}/`
- **Silver Data**: `/data/minio/me-dashboard/silver/{domain}/`
- **Loading Script**: `scripts/load_data_to_duckdb.py`

### Query Examples
```sql
-- Data catalog overview
SELECT * FROM data_catalog;

-- Location data exploration
SELECT COUNT(*) as location_count FROM location_data_bronze;

-- Schema inspection
DESCRIBE location_data_bronze;
```