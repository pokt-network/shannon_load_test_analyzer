import json
import logging
import time
import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, List, Set
import subprocess
import base64


@dataclass
class Config:
    node_url: str = "https://shannon-testnet-grove-rpc.beta.poktroll.com"
    output_dir: Path = Path("./block_results")
    rate_limit: float = 0.5
    max_retries: int = 3
    retry_delay: float = 1.0


@dataclass
class EventMetrics:
    claimed_pokt: float = 0
    claimed_compute_units: int = 0
    estimated_compute_units: int = 0
    num_relays: int = 0
    application_addresses: Set[str] = None
    supplier_addresses: Set[str] = None
    session_ids: Set[str] = None

    def __post_init__(self):
        # Initialize sets in post_init to avoid mutable default argument issues
        self.application_addresses = set() if self.application_addresses is None else self.application_addresses
        self.supplier_addresses = set() if self.supplier_addresses is None else self.supplier_addresses
        self.session_ids = set() if self.session_ids is None else self.session_ids


@dataclass
class BlockStats:
    tx_mb: float
    block_mb: float
    num_txs: int
    total_claimed_pokt: float
    total_claimed_compute_units: int
    total_estimated_compute_units: int
    total_relays: int
    unique_applications: Set[str]
    unique_suppliers: Set[str]
    unique_sessions: Set[str]

    @classmethod
    def zero(cls):
        return cls(0.0, 0.0, 0, 0.0, 0, 0, 0, set(), set(), set())

    def __add__(self: "BlockStats", other: "BlockStats"):
        return BlockStats(
            tx_mb=self.tx_mb + other.tx_mb,
            block_mb=self.block_mb + other.block_mb,
            num_txs=self.num_txs + other.num_txs,
            total_claimed_pokt=self.total_claimed_pokt + other.total_claimed_pokt,
            total_claimed_compute_units=self.total_claimed_compute_units + other.total_claimed_compute_units,
            total_estimated_compute_units=self.total_estimated_compute_units + other.total_estimated_compute_units,
            total_relays=self.total_relays + other.total_relays,
            unique_applications=self.unique_applications | other.unique_applications,
            unique_suppliers=self.unique_suppliers | other.unique_suppliers,
            unique_sessions=self.unique_sessions | other.unique_sessions,
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
            output.append(f"  Num Txs: {block.stats.num_txs:,}")
            output.append(f"  Txs size: {block.stats.tx_mb:.2f} MB")
            output.append(f"  Full block size: {block.stats.block_mb:.2f} MB")
            output.append(f"  Claimed POKT: {block.stats.total_claimed_pokt:.6f}")
            output.append(f"  Claimed compute units: {block.stats.total_claimed_compute_units:,}")
            output.append(f"  Estimated compute units: {block.stats.total_estimated_compute_units:,}")
            output.append(f"  Number of relays: {block.stats.total_relays:,}")
            output.append(f"  Unique applications: {len(block.stats.unique_applications):,}")
            output.append(f"  Unique suppliers: {len(block.stats.unique_suppliers):,}")
            output.append(f"  Unique sessions: {len(block.stats.unique_sessions):,}")
            output.append("")

        first_block = self.blocks[0].block_id if self.blocks else 0
        last_block = self.blocks[-1].block_id if self.blocks else 0
        output.append("------------------------------------------")
        output.append(f"Total Statistics ({first_block} - {last_block}):")
        output.append("------------------------------------------")
        output.append(f"Total Txs: {self.total_stats.num_txs:,}")
        output.append(f"Total Txs size: {self.total_stats.tx_mb:.2f} MB")
        output.append(f"Total Block size: {self.total_stats.block_mb:.2f} MB")
        output.append(f"Total Claimed POKT: {self.total_stats.total_claimed_pokt:.6f}")
        output.append(f"Total Claimed compute units: {self.total_stats.total_claimed_compute_units:,}")
        output.append(f"Total Estimated compute units: {self.total_stats.total_estimated_compute_units:,}")
        output.append(f"Total Number of relays: {self.total_stats.total_relays:,}")
        output.append(f"Total Unique applications: {len(self.total_stats.unique_applications):,}")
        output.append(f"Total Unique suppliers: {len(self.total_stats.unique_suppliers):,}")
        output.append(f"Total Unique sessions: {len(self.total_stats.unique_sessions):,}")

        return "\n".join(output)

    def to_dataframe(self) -> "pd.DataFrame":
        """Convert block stats to a pandas DataFrame."""
        import pandas as pd

        # Prepare data for each block
        data = []
        for block in self.blocks:
            row = {
                "block_id": block.block_id,
                "num_txs": block.stats.num_txs,
                "tx_size_mb": round(block.stats.tx_mb, 2),
                "block_size_mb": round(block.stats.block_mb, 2),
                "claimed_pokt": round(block.stats.total_claimed_pokt, 6),
                "claimed_compute_units": block.stats.total_claimed_compute_units,
                "estimated_compute_units": block.stats.total_estimated_compute_units,
                "num_relays": block.stats.total_relays,
                "unique_applications": len(block.stats.unique_applications),
                "unique_suppliers": len(block.stats.unique_suppliers),
                "unique_sessions": len(block.stats.unique_sessions),
            }
            data.append(row)

        # Create DataFrame
        df = pd.DataFrame(data)

        # Add a row for totals
        totals = {
            "block_id": f"Total ({self.blocks[0].block_id}-{self.blocks[-1].block_id})",
            "num_txs": self.total_stats.num_txs,
            "tx_size_mb": round(self.total_stats.tx_mb, 2),
            "block_size_mb": round(self.total_stats.block_mb, 2),
            "claimed_pokt": round(self.total_stats.total_claimed_pokt, 6),
            "claimed_compute_units": self.total_stats.total_claimed_compute_units,
            "estimated_compute_units": self.total_stats.total_estimated_compute_units,
            "num_relays": self.total_stats.total_relays,
            "unique_applications": len(self.total_stats.unique_applications),
            "unique_suppliers": len(self.total_stats.unique_suppliers),
            "unique_sessions": len(self.total_stats.unique_sessions),
        }

        # Append totals row
        df.loc[len(df)] = totals

        return df

    def tabulate(self) -> str:
        """Return a tabulated string representation of the stats."""
        from tabulate import tabulate

        df = self.to_dataframe()
        return tabulate(df, headers="keys", tablefmt="pipe", floatfmt=".6f")


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

    def _extract_event_metrics_created(self, event: Dict) -> EventMetrics:
        metrics = EventMetrics()
        if event.get("type") == "poktroll.proof.EventClaimCreated":
            for attr in event.get("attributes", []) or []:
                key = attr["key"]
                value = attr["value"]
                if key == "claimed_upokt":
                    metrics.claimed_pokt = int(json.loads(value)["amount"]) / 1_000_000
                elif key == "num_claimed_compute_units":
                    metrics.claimed_compute_units = int(value.strip('"'))
                elif key == "num_estimated_compute_units":
                    metrics.estimated_compute_units = int(value.strip('"'))
                elif key == "num_relays":
                    metrics.num_relays = int(value.strip('"'))
                elif key == "claim":
                    try:
                        claim_data = json.loads(value)
                        metrics.supplier_addresses.add(claim_data["supplier_operator_address"])
                        metrics.application_addresses.add(claim_data["session_header"]["application_address"])
                        metrics.session_ids.add(claim_data["session_header"]["session_id"])
                    except (json.JSONDecodeError, KeyError) as e:
                        self.logger.warning(f"Error parsing claim data: {e}")
        return metrics

    def _extract_event_metrics_settled(self, event: Dict) -> EventMetrics:
        metrics = EventMetrics()
        if event.get("type") == "poktroll.proof.EventClaimSettled":
            for attr in event.get("attributes", []) or []:
                print("OLSH", attr)
                key = attr["key"]
                value = attr["value"]
                if key == "claimed_upokt":
                    metrics.claimed_pokt = int(json.loads(value)["amount"]) / 1_000_000
                elif key == "num_claimed_compute_units":
                    metrics.claimed_compute_units = int(value.strip('"'))
                elif key == "num_estimated_compute_units":
                    metrics.estimated_compute_units = int(value.strip('"'))
                elif key == "num_relays":
                    metrics.num_relays = int(value.strip('"'))
        return metrics

    def _compute_block_stats(self, block_data: dict, block_results: dict) -> BlockStats:
        txs = block_data.get("data", {}).get("txs", [])
        total_tx_bytes = sum(len(base64.b64decode(tx)) for tx in txs)
        tx_mb = total_tx_bytes / (1024 * 1024)

        block_json = json.dumps(block_data)
        block_mb = len(block_json.encode("utf-8")) / (1024 * 1024)

        total_claimed_pokt = 0.0
        total_claimed_compute_units = 0
        total_estimated_compute_units = 0
        total_relays = 0
        unique_applications = set()
        unique_suppliers = set()
        unique_sessions = set()

        for tx_result in block_results.get("txs_results", []) or []:
            for event in tx_result.get("events", []):
                created_metrics = self._extract_event_metrics_created(event)
                settled_metrics = self._extract_event_metrics_settled(event)

                total_claimed_pokt += created_metrics.claimed_pokt + settled_metrics.claimed_pokt
                total_claimed_compute_units += (
                    created_metrics.claimed_compute_units + settled_metrics.claimed_compute_units
                )
                total_estimated_compute_units += (
                    created_metrics.estimated_compute_units + settled_metrics.estimated_compute_units
                )
                total_relays += created_metrics.num_relays + settled_metrics.num_relays

                # Update unique sets
                unique_applications.update(created_metrics.application_addresses)
                unique_suppliers.update(created_metrics.supplier_addresses)
                unique_sessions.update(created_metrics.session_ids)

        return BlockStats(
            tx_mb=tx_mb,
            block_mb=block_mb,
            num_txs=len(txs),
            total_claimed_pokt=total_claimed_pokt,
            total_claimed_compute_units=total_claimed_compute_units,
            total_estimated_compute_units=total_estimated_compute_units,
            total_relays=total_relays,
            unique_applications=unique_applications,
            unique_suppliers=unique_suppliers,
            unique_sessions=unique_sessions,
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

    def fetch_range(self, start_block: int, end_block: int, force: bool = False) -> RangeStats:
        blocks = []
        total_stats = BlockStats.zero()
        for block_id in range(start_block, end_block + 1):
            try:
                block_file, block_data = self._fetch_single(block_id, is_block_results=False, force=force)
                results_file, results_data = self._fetch_single(block_id, is_block_results=True, force=force)
                stats = self._compute_block_stats(block_data, results_data)
                blocks.append(BlockData(block_id, block_file, results_file, stats))
                total_stats += stats
            except BlockFetchError as e:
                self.logger.error(f"Failed to fetch block {block_id}: {e}")
        return RangeStats(blocks, total_stats)


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch block data")
    parser.add_argument("--block-start", type=int, default=58108, help="Starting block")
    parser.add_argument("--block-end", type=int, default=58119, help="Ending block")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    fetcher = BlockFetcher()
    stats = fetcher.fetch_range(args.block_start, args.block_end)
    # print(stats)
    print(stats.tabulate())
