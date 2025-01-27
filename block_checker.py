import json
import logging
import time
import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, List
import subprocess
import base64


@dataclass
class Config:
    node_url: str = "https://shannon-testnet-grove-rpc.beta.poktroll.com"
    output_dir: Path = Path("/tmp/block_results")
    rate_limit: float = 0.5
    max_retries: int = 3
    retry_delay: float = 1.0


@dataclass
class EventMetrics:
    claimed_pokt: float = 0  # Changed from upokt to pokt
    claimed_compute_units: int = 0
    estimated_compute_units: int = 0
    num_relays: int = 0


@dataclass
class BlockStats:
    tx_mb: float
    num_txs: int
    total_claimed_pokt: float  # Changed from upokt to pokt
    total_claimed_compute_units: int
    total_estimated_compute_units: int
    total_relays: int

    @classmethod
    def zero(cls):
        return cls(0.0, 0, 0.0, 0, 0, 0)  # Updated to include float for pokt

    def __add__(self, other):
        return BlockStats(
            tx_mb=self.tx_mb + other.tx_mb,
            num_txs=self.num_txs + other.num_txs,
            total_claimed_pokt=self.total_claimed_pokt + other.total_claimed_pokt,
            total_claimed_compute_units=self.total_claimed_compute_units + other.total_claimed_compute_units,
            total_estimated_compute_units=self.total_estimated_compute_units + other.total_estimated_compute_units,
            total_relays=self.total_relays + other.total_relays,
        )


@dataclass
class BlockData:
    block_id: int
    block_file: Path
    results_file: Path
    stats: BlockStats


@dataclass
class RangeStats:
    blocks: List[BlockData]
    total_stats: BlockStats

    def __str__(self) -> str:
        output = []
        output.append("--------------------")
        output.append("Per Block Statistics:")
        output.append("--------------------")
        for block in self.blocks:
            output.append(f"Block {block.block_id}:")
            output.append(f"  Transactions: {block.stats.num_txs:,}")
            output.append(f"  Transaction size: {block.stats.tx_mb:.2f} MB")
            output.append(f"  Claimed POKT: {block.stats.total_claimed_pokt:.6f}")
            output.append(f"  Claimed compute units: {block.stats.total_claimed_compute_units:,}")
            output.append(f"  Estimated compute units: {block.stats.total_estimated_compute_units:,}")
            output.append(f"  Number of relays: {block.stats.total_relays:,}")
            output.append("")

        first_block = self.blocks[0].block_id if self.blocks else 0
        last_block = self.blocks[-1].block_id if self.blocks else 0
        output.append("------------------------------------------")
        output.append(f"Total Statistics ({first_block} - {last_block}):")
        output.append("------------------------------------------")
        output.append(f"Total Transactions: {self.total_stats.num_txs:,}")
        output.append(f"Total Transaction size: {self.total_stats.tx_mb:.2f} MB")
        output.append(f"Total Claimed POKT: {self.total_stats.total_claimed_pokt:.6f}")
        output.append(f"Total Claimed compute units: {self.total_stats.total_claimed_compute_units:,}")
        output.append(f"Total Estimated compute units: {self.total_stats.total_estimated_compute_units:,}")
        output.append(f"Total Number of relays: {self.total_stats.total_relays:,}")

        return "\n".join(output)


class BlockFetchError(Exception):
    pass


class BlockFetcher:
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self._setup_logging()
        self._setup_output_dir()
        self._last_request_time = 0

    def _setup_logging(self):
        self.logger = logging.getLogger("BlockFetcher")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def _setup_output_dir(self):
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Output directory set to: {self.config.output_dir}")

    def _apply_rate_limit(self):
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        if time_since_last < self.config.rate_limit:
            time.sleep(self.config.rate_limit - time_since_last)
        self._last_request_time = time.time()

    def _get_command(self, block_id: int, is_block_results: bool) -> list[str]:
        cmd = ["poktrolld", "query"]

        if is_block_results:
            cmd.append("block-results")
        else:
            cmd.extend(["block", "--type=height"])

        cmd.extend([str(block_id), "-o", "json", f"--node={self.config.node_url}"])
        return cmd

    def _validate_response(self, output_file: Path) -> bool:
        try:
            with open(output_file) as f:
                json.load(f)
            return True
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON in response: {output_file}")
            return False

    def _extract_event_metrics(self, event: Dict) -> Dict:
        if event.get("type") != "poktroll.proof.EventClaimCreated":
            return {"claimed_pokt": 0, "claimed_compute_units": 0, "estimated_compute_units": 0, "num_relays": 0}

        metrics = {}
        for attr in event.get("attributes", []):
            key = attr["key"]
            value = attr["value"]

            if key == "claimed_upokt":
                try:
                    # Convert uPOKT to POKT by dividing by 10^6
                    upokt_amount = int(json.loads(value)["amount"])
                    metrics["claimed_pokt"] = upokt_amount / 1_000_000
                except (json.JSONDecodeError, KeyError):
                    metrics["claimed_pokt"] = 0

            elif key == "num_claimed_compute_units":
                try:
                    metrics["claimed_compute_units"] = int(value.strip('"'))
                except ValueError:
                    metrics["claimed_compute_units"] = 0

            elif key == "num_estimated_compute_units":
                try:
                    metrics["estimated_compute_units"] = int(value.strip('"'))
                except ValueError:
                    metrics["estimated_compute_units"] = 0

            elif key == "num_relays":
                try:
                    metrics["num_relays"] = int(value.strip('"'))
                except ValueError:
                    metrics["num_relays"] = 0

        return metrics

    def _compute_block_stats(self, block_data: dict, block_results: dict) -> BlockStats:
        # Calculate transaction stats
        txs = block_data.get("data", {}).get("txs", [])
        total_bytes = sum(len(base64.b64decode(tx)) for tx in txs)
        tx_mb = total_bytes / (1024 * 1024)  # Convert to MB

        # Initialize counters for new metrics
        total_claimed_pokt = 0.0  # Changed from upokt to pokt
        total_claimed_compute_units = 0
        total_estimated_compute_units = 0
        total_relays = 0

        # Process block results
        tx_results = block_results.get("txs_results", [])
        tx_results = tx_results or []  # Ensure tx_results is a list
        for tx_result in tx_results:
            for event in tx_result.get("events", []):
                metrics = self._extract_event_metrics(event)
                total_claimed_pokt += metrics["claimed_pokt"]
                total_claimed_compute_units += metrics["claimed_compute_units"]
                total_estimated_compute_units += metrics["estimated_compute_units"]
                total_relays += metrics["num_relays"]

        return BlockStats(
            tx_mb=tx_mb,
            num_txs=len(txs),
            total_claimed_pokt=total_claimed_pokt,
            total_claimed_compute_units=total_claimed_compute_units,
            total_estimated_compute_units=total_estimated_compute_units,
            total_relays=total_relays,
        )

    def _fetch_single(self, block_id: int, is_block_results: bool, force: bool = False) -> tuple[Path, Optional[dict]]:
        file_prefix = "block-results" if is_block_results else "block"
        output_file = self.config.output_dir / f"{file_prefix}_{block_id}.json"

        if not force and output_file.exists() and self._validate_response(output_file):
            self.logger.info(f"Using cached {file_prefix} {block_id} from {output_file}")
            with open(output_file) as f:
                return output_file, json.load(f)

        for attempt in range(self.config.max_retries):
            try:
                self._apply_rate_limit()

                self.logger.info(f"Fetching {file_prefix} {block_id} (attempt {attempt + 1})")
                process = subprocess.run(
                    self._get_command(block_id, is_block_results),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=True,
                    text=True,
                )

                data = json.loads(process.stdout)
                output_file.write_text(process.stdout)

                if self._validate_response(output_file):
                    self.logger.info(f"Successfully saved {file_prefix} {block_id} to {output_file}")
                    return output_file, data

            except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {file_prefix} {block_id}: {str(e)}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay)
                continue

        raise BlockFetchError(f"Failed to fetch {file_prefix} {block_id} after {self.config.max_retries} attempts")

    def fetch_block(self, block_id: int, force: bool = False) -> BlockData:
        """
        Fetch both block and block-results for a given block ID.

        Args:
            block_id: The block height to query
            force: If True, fetch even if cached files exist

        Returns:
            BlockData containing paths to both files and block statistics
        """
        block_file, block_data = self._fetch_single(block_id, is_block_results=False, force=force)
        results_file, results_data = self._fetch_single(block_id, is_block_results=True, force=force)

        stats = self._compute_block_stats(block_data, results_data)
        return BlockData(block_id, block_file, results_file, stats)

    def fetch_range(self, start_block: int, end_block: int, force: bool = False) -> RangeStats:
        """
        Fetch blocks and compute statistics for a range of block heights.

        Args:
            start_block: Starting block height
            end_block: Ending block height (inclusive)
            force: If True, fetch even if cached files exist

        Returns:
            RangeStats containing per-block data and aggregated statistics
        """
        blocks = []
        total_stats = BlockStats.zero()

        for block_id in range(start_block, end_block + 1):
            try:
                block_data = self.fetch_block(block_id, force)
                blocks.append(block_data)
                total_stats = total_stats + block_data.stats
            except BlockFetchError as e:
                self.logger.error(f"Failed to fetch block {block_id}: {e}")
                continue

        return RangeStats(blocks, total_stats)


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch block data from poktroll network")
    # parser.add_argument("--block-start", type=int, default=57333, help="Starting block height")
    parser.add_argument("--block-start", type=int, default=58108, help="Starting block height")
    # parser.add_argument("--block-end", type=int, default=57335, help="Ending block height")
    parser.add_argument("--block-end", type=int, default=58119, help="Ending block height")
    parser.add_argument("--output-dir", type=Path, default="./block_results", help="Output directory")
    parser.add_argument("--node-url", help="Node URL")
    parser.add_argument("--force", action="store_true", help="Force fetch even if cached")
    return parser.parse_args()


def main():
    args = parse_args()

    config = Config(output_dir=args.output_dir, node_url=args.node_url if args.node_url else Config.node_url)
    fetcher = BlockFetcher(config)

    try:
        range_stats = fetcher.fetch_range(args.block_start, args.block_end, args.force)
        print(range_stats)
    except BlockFetchError as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
