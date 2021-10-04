[![Build Status](https://travis-ci.com/paypal/dione.svg?branch=main)](https://travis-ci.com/paypal/dione)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Dione
Dione - an indexing Library for data on HDFS and Spark.

The main offering is APIs for building an index for data on HDFS and querying the index in both:
- _Multi-row load_ - using Spark as a distributed processing engine, load a subset of the data (0.1% to 100% of key space) much faster than Spark/Hive joins.
- _Single-row Fetch_ - `get(key)` with seconds latency, and low throughput.

This way we can reuse HDFS data, that is primarily used for batch processing, for more ad-hoc access use-cases. 

There are three main building blocks:
- `HdfsIndexer` - a library for indexing HDFS data and loading back the data given the index metadata.
- `AvroBtreeFile` - an Avro based file format for storing rows in a file in a B-Tree order for fast search.
- `IndexManager` - a high-level API for index management using Spark.

For deeper overview please see our [Dione documentation](docs/detailed_doc.md). 

## Main Features
- Data and index are available for batch processing.
- Use the same technology stack for the index and for the data.
- No data duplication.
- Support multiple indices for the same data.
- No need to be the data owner.

## Quick Start
Check out our [Quick Start](docs/quick_start.md) or [Quick Start Python](docs/quick_start_python.md) guides.

## Compatibility Matrix
| Dione | Spark|
|-------|------|
| 0.5.x | 2.3.x|
| 0.6.x | 2.4.x|

## Reporting an issue
Please open issues in the [GitHub issues](https://github.com/paypal/dione/issues).

## License
This project is licensed under the [Apache 2 License](LICENSE).
