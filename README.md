[![Build Status](https://travis-ci.com/paypal/dione.svg?branch=main)](https://travis-ci.com/paypal/dione)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Dione
Dione - an indexing Library for data on HDFS.

The main offering is APIs for building an index for data on HDFS and querying the index in both:
- _Random Access Fetches_ - `get(key)` with near-realtime latency (in Hadoop terms), and low throughput.
- _Batch Processing of Specific Rows_ - using Spark as a distributed processing engine, retrive subset of the data (0.1% to 100% of key space) much faster than Spark/Hive joins.

## Main Features
- Data and index are available for batch processing.
- Use the same technology stack for the index and for the data.
- No data duplication.
- Support multiple indices for the same data.
- No need to be the data owner.

## Quick Start
Check out our [Quick Start](docs/quick_start.md) or [Quick Start Python](docs/quick_start_python.md) guides.


## Detailed Documentation
Please refer to our [Dione documentation](docs/detailed_doc.md).

## Reporting an issue
Please open issues in the [GitHub issues](https://github.com/paypal/dione/issues).

## License
This project is licensed under the [Apache 2 License](LICENSE).
