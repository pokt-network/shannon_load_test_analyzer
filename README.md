# Shannon Load Test analyzer Hack <!-- omit in toc -->

> [!IMPORTANT]
> This is a hacky project to analyze the Shannon Load Test data.
> Mostly generated by claude as a starting point

A Python utility for analyzing blockchain block data, specifically designed for the POKT Network. This tool fetches block data and their results, analyzes transactions, and provides detailed metrics about claims, compute units, and unique addresses involved in the network.

- [TODOs](#todos)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
  - [Command Line Arguments](#command-line-arguments)
  - [Example Output](#example-output)
- [Configuration](#configuration)
- [Metrics Tracked](#metrics-tracked)
- [Error Handling](#error-handling)
- [Caching](#caching)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## TODOs

- [ ] Use the proper `protobuf`s from the Shannon repo
- [ ] Add a `Jupyter` notebook for analysis
- [ ] Switch to using `uv`
- [ ] Update the structure to a proper `python` project
- [ ] Use the [poktroll-clients-py](https://github.com/pokt-network/poktroll-clients-py) with native bindings

## Features

- Fetches and analyzes block data from POKT Network nodes
- Tracks transaction sizes and block sizes
- Monitors claimed POKT tokens and compute units
- Counts unique applications, suppliers, and session IDs
- Provides both per-block and aggregated statistics
- Supports both text and tabular output formats
- Implements rate limiting and automatic retries for reliable data fetching

## Prerequisites

- Python 3.7+
- `poktrolld` CLI tool installed and accessible in your PATH
- Access to a POKT Network node

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/block-checker.git
cd block-checker
```

2. Install required dependencies:

```bash
pip install pandas tabulate
```

## Usage

Basic usage with default parameters:

```bash
python3 block_checker.py --block-start=57333 --block-end=57334
```

### Command Line Arguments

- `--block-start`: Starting block height (default: 58108)
- `--block-end`: Ending block height (default: 58119)
- `--node-url`: Optional URL for the POKT Network node

### Example Output

```bash
| block_id                | num_txs |   tx_size_mb |   block_size_mb |   claimed_pokt |   claimed_compute_units |   estimated_compute_units |   num_relays |   unique_applications |   unique_suppliers |   unique_sessions |
|------------------------|----------|--------------|-----------------|----------------|------------------------|--------------------------|--------------|---------------------|-------------------|------------------|
| 57333                  |       4  |         8.43 |          11.25 |      18.403770 |                438185 |                  438185 |        87637 |              16087 |                 4 |            16087 |
| 57334                  |       0  |         0.00 |           0.00 |       0.000000 |                     0 |                       0 |            0 |                  0 |                 0 |                0 |
| Total (57333-57334)    |       4  |         8.43 |          11.25 |      18.403770 |                438185 |                  438185 |        87637 |              16087 |                 4 |            16087 |
```

## Configuration

The tool can be configured through the `Config` class:

```python
@dataclass
class Config:
    node_url: str = "https://shannon-testnet-grove-rpc.beta.poktroll.com"
    output_dir: Path = Path("./block_results")
    rate_limit: float = 0.5
    max_retries: int = 3
    retry_delay: float = 1.0
```

- `node_url`: URL of the POKT Network node
- `output_dir`: Directory for storing fetched block data
- `rate_limit`: Minimum time (in seconds) between requests
- `max_retries`: Maximum number of retry attempts for failed requests
- `retry_delay`: Delay (in seconds) between retry attempts

## Metrics Tracked

- Transaction Metrics:

  - Number of transactions
  - Transaction size (MB)
  - Block size (MB)

- POKT Network Metrics:

  - Claimed POKT tokens
  - Claimed compute units
  - Estimated compute units
  - Number of relays

- Unique Address Tracking:
  - Unique application addresses
  - Unique supplier addresses
  - Unique session IDs

## Error Handling

The tool implements robust error handling with:

- Automatic retries for failed requests
- Rate limiting to prevent overwhelming the node
- JSON validation for responses
- Detailed logging of errors and warnings

## Caching

Block data is cached in the output directory to prevent unnecessary refetching. Use the `--force` flag to bypass the cache and fetch fresh data.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- POKT Network team for their blockchain infrastructure
- Contributors to the Python ecosystem
