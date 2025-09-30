# Prefect ETL Pipeline for Geospatial Data Processing

This repository contains a comprehensive ETL (Extract, Transform, Load) pipeline built with Prefect for processing and standardizing geospatial data, particularly focused on US infrastructure datasets including distribution lines, broadband coverage, and road networks.

## 🏗️ Project Overview

This ETL pipeline processes large-scale geospatial datasets and standardizes them for storage in PostGIS databases. The system handles various data formats (GeoJSON, Parquet, Shapefiles) and performs data transformation, standardization, and upload operations.

## 📁 Repository Structure

```
prefect_ELT/
├── README.md                    # This file
├── requirements.txt            # Python dependencies
├── etl_buildings.log          # Log file for ETL operations
├── output_path/               # Output directory for processed files
├── unzipped/                  # Directory for extracted files
│
├── Core ETL Scripts:
├── standard_name.py           # Main standardization and mapping logic
├── distribution.py            # Distribution lines ETL pipeline
├── broadband.py               # Broadband coverage ETL pipeline
├── roads.py                   # Road networks ETL pipeline
│
├── Data Format Converters:
├── geojsonTOgeoparquet.py     # GeoJSON to GeoParquet conversion
├── parquetTOgeojson.py        # GeoParquet to GeoJSON conversion
├── H3TOgeom.py               # H3 hexagon to geometry conversion
│
├── Data Processing Utilities:
├── duplicat.py               # Duplicate detection and removal
├── fixed.py                  # Data fixing utilities
├── nameChange.py             # Name standardization utilities
├── tigershapefile.py         # TIGER shapefile processing
├── CDP_lsad.py              # CDP (Census Designated Place) processing
├── cdp_noncdp.py            # CDP vs non-CDP classification
├── distri.py                 # Distribution utilities
├── alisa.py                  # Alias mapping utilities
└── roads.py                  # Road network processing
```

## 🚀 Key Features

### Data Standardization
- **Operator Mapping**: Automatically maps different operator names to standardized formats using Excel configuration files
- **Field Standardization**: Converts various field names to consistent schema across different data sources
- **Alias Resolution**: Fetches and applies field aliases from external APIs for better data mapping

### Format Conversion
- **GeoJSON ↔ GeoParquet**: Bidirectional conversion between GeoJSON and GeoParquet formats
- **H3 Hexagon Processing**: Converts H3 hexagon indices to polygon geometries
- **Shapefile Processing**: Handles TIGER shapefiles and other vector formats

### Database Integration
- **PostGIS Upload**: Streams large datasets to PostGIS databases with proper geometry handling
- **Batch Processing**: Processes data in configurable batches to handle memory constraints
- **CRS Standardization**: Ensures all geometries are in EPSG:4326 coordinate reference system

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd prefect_ELT
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   Create a `.env` file with your database connection strings:
   ```env
   DATABASE_URL=postgresql://user:password@host:port/database
   DATABASE_URL_LOCAL=postgresql://user:password@localhost:port/database
   ```

## 📊 Data Processing Workflows

### 1. Distribution Lines Processing (`distribution.py`)
- Processes electrical distribution line data
- Standardizes voltage levels, feeder information, and hosting capacity
- Uploads to `us_distribution_lines_test` table

### 2. Broadband Coverage Processing (`broadband.py`)
- Processes FCC broadband coverage data
- Handles technology types, speed information, and geographic coverage
- Uploads to `us_broadband` table

### 3. Road Networks Processing (`roads.py`)
- Processes TIGER road network data
- Standardizes road names and types
- Uploads to `us_roads` table

### 4. Data Standardization (`standard_name.py`)
- Main standardization pipeline for operator data
- Uses Excel mapping files to standardize field names
- Applies operator-specific transformations
- Handles raw data preservation in JSON format

## 🔧 Usage Examples

### Basic Data Processing
```python
# Process distribution data
python distribution.py

# Process broadband data  
python broadband.py

# Process road networks
python roads.py
```

### Data Format Conversion
```python
# Convert GeoJSON to GeoParquet
python geojsonTOgeoparquet.py

# Convert GeoParquet to GeoJSON
python parquetTOgeojson.py

# Convert H3 indices to geometries
python H3TOgeom.py --input data.parquet --output output.geoparquet
```

### Data Standardization
```python
# Run standardization pipeline
python standard_name.py
```

## 🗄️ Database Schema

The pipeline creates and populates the following PostGIS tables:

- **`us_distribution_lines_test`**: Electrical distribution infrastructure
- **`us_broadband`**: Broadband coverage and availability data  
- **`us_roads`**: Road network data

Each table includes:
- `owner_name`: Standardized operator/owner name
- `raw_data`: Original data preserved as JSON
- `geometry`: Spatial geometry in EPSG:4326

## ⚙️ Configuration

### Batch Processing Settings
- **CHUNKSIZE**: Rows per database insert (default: 750,000)
- **BATCHSIZE**: Rows read from source files (default: 1,000,000)
- **CHUNK_SIZE**: Processing batch size for road data (default: 50,000)

### Column Selection
Each ETL script includes `SELECT_COLUMNS` configuration to specify which fields to preserve during processing.

## 🔍 Data Quality Features

- **Duplicate Detection**: Identifies and handles duplicate records
- **Data Validation**: Ensures geometry validity and CRS consistency
- **Error Handling**: Comprehensive error logging and recovery
- **Memory Management**: Streaming processing for large datasets

## 📈 Performance Optimization

- **Streaming Processing**: Handles datasets larger than available RAM
- **Batch Uploads**: Efficient database insertion with configurable batch sizes
- **Geometry Optimization**: Converts geometries to appropriate types (MultiLineString for roads)
- **Compression**: Uses uncompressed Parquet for faster processing

## 🚨 Error Handling

The pipeline includes comprehensive error handling for:
- Missing files and directories
- Database connection issues
- Invalid geometries
- Memory constraints
- Network timeouts for API calls

## 📝 Logging

All operations are logged to `etl_buildings.log` with detailed information about:
- Processing progress
- Error conditions
- Data quality issues
- Performance metrics

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the log files for error details
2. Verify database connectivity
3. Ensure all required dependencies are installed
4. Check file paths and permissions

## 🔄 Dependencies

Key dependencies include:
- **Prefect**: Workflow orchestration
- **GeoPandas**: Geospatial data processing
- **PyArrow**: Parquet file handling
- **SQLAlchemy**: Database operations
- **PostGIS**: Spatial database support
- **Shapely**: Geometry operations
- **H3**: Hexagonal indexing system
