import json
from tabulate import tabulate
import pandas as pd
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
class MsgMetrics:
    msg_type: str = ""
    sender: str = ""
    module: str = ""
    msg_index: str = ""


@dataclass
class EventMetrics:
    claimed_pokt: float = 0
    claimed_compute_units: int = 0
    estimated_compute_units: int = 0
    num_relays: int = 0
    application_addresses: Set[str] = None
    supplier_addresses: Set[str] = None
    session_ids: Set[str] = None
    service_ids: Set[str] = None

    def __post_init__(self):
        # Initialize sets in post_init to avoid mutable default argument issues
        self.application_addresses = set() if self.application_addresses is None else self.application_addresses
        self.supplier_addresses = set() if self.supplier_addresses is None else self.supplier_addresses
        self.session_ids = set() if self.session_ids is None else self.session_ids
        self.service_ids = set() if self.service_ids is None else self.service_ids


@dataclass
class BlockStats:
    tx_mb: float
    block_mb: float
    num_txs: int
    # Claimed metrics
    claimed_pokt: float
    claimed_compute_units: int
    claimed_estimated_compute_units: int
    claimed_relays: int
    # Settled metrics
    settled_pokt: float
    settled_compute_units: int
    settled_estimated_compute_units: int
    settled_relays: int
    # Sets
    unique_applications: Set[str]
    unique_suppliers: Set[str]
    unique_sessions: Set[str]
    unique_services: Set[str]
    # Message counts
    total_messages: int = 0
    claim_messages: int = 0
    proof_messages: int = 0

    @classmethod
    def zero(cls):
        return cls(
            0.0,
            0.0,
            0,  # Basic stats
            0.0,
            0,
            0,
            0,  # Claimed metrics
            0.0,
            0,
            0,
            0,  # Settled metrics
            set(),
            set(),
            set(),  # Sets
            set(),  # unique_services
            0,
            0,
            0,  # Message counts
        )

    def __add__(self: "BlockStats", other: "BlockStats"):
        return BlockStats(
            tx_mb=self.tx_mb + other.tx_mb,
            block_mb=self.block_mb + other.block_mb,
            num_txs=self.num_txs + other.num_txs,
            # Claimed metrics
            claimed_pokt=self.claimed_pokt + other.claimed_pokt,
            claimed_compute_units=self.claimed_compute_units + other.claimed_compute_units,
            claimed_estimated_compute_units=self.claimed_estimated_compute_units
            + other.claimed_estimated_compute_units,
            claimed_relays=self.claimed_relays + other.claimed_relays,
            # Settled metrics
            settled_pokt=self.settled_pokt + other.settled_pokt,
            settled_compute_units=self.settled_compute_units + other.settled_compute_units,
            settled_estimated_compute_units=self.settled_estimated_compute_units
            + other.settled_estimated_compute_units,
            settled_relays=self.settled_relays + other.settled_relays,
            # Sets
            unique_applications=self.unique_applications | other.unique_applications,
            unique_suppliers=self.unique_suppliers | other.unique_suppliers,
            unique_sessions=self.unique_sessions | other.unique_sessions,
            unique_services=self.unique_services | other.unique_services,
            # Message counts
            total_messages=self.total_messages + other.total_messages,
            claim_messages=self.claim_messages + other.claim_messages,
            proof_messages=self.proof_messages + other.proof_messages,
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

    def to_dataframe(self) -> pd.DataFrame:
        """Convert block stats to a pandas DataFrame."""

        # Prepare data for each block
        data = []
        for block in self.blocks:
            row = {
                "block_id": block.block_id,
                "num_txs": block.stats.num_txs,
                "tx_size_mb": round(block.stats.tx_mb, 2),
                "block_size_mb": round(block.stats.block_mb, 2),
                # Claimed metrics
                "claimed_pokt": round(block.stats.claimed_pokt, 6),
                "claimed_compute_units": block.stats.claimed_compute_units,
                "claimed_estimated_compute_units": block.stats.claimed_estimated_compute_units,
                "claimed_relays": block.stats.claimed_relays,
                # Settled metrics
                "settled_pokt": round(block.stats.settled_pokt, 6),
                "settled_compute_units": block.stats.settled_compute_units,
                "settled_estimated_compute_units": block.stats.settled_estimated_compute_units,
                "settled_relays": block.stats.settled_relays,
                # Sets
                "unique_applications": len(block.stats.unique_applications),
                "unique_suppliers": len(block.stats.unique_suppliers),
                "unique_sessions": len(block.stats.unique_sessions),
                "unique_services": len(block.stats.unique_services),
                # Message counts
                "total_messages": block.stats.total_messages,
                "claim_messages": block.stats.claim_messages,
                "proof_messages": block.stats.proof_messages,
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
            # Claimed metrics
            "claimed_pokt": round(self.total_stats.claimed_pokt, 6),
            "claimed_compute_units": self.total_stats.claimed_compute_units,
            "claimed_estimated_compute_units": self.total_stats.claimed_estimated_compute_units,
            "claimed_relays": self.total_stats.claimed_relays,
            # Settled metrics
            "settled_pokt": round(self.total_stats.settled_pokt, 6),
            "settled_compute_units": self.total_stats.settled_compute_units,
            "settled_estimated_compute_units": self.total_stats.settled_estimated_compute_units,
            "settled_relays": self.total_stats.settled_relays,
            # Sets
            "unique_applications": len(self.total_stats.unique_applications),
            "unique_suppliers": len(self.total_stats.unique_suppliers),
            "unique_sessions": len(self.total_stats.unique_sessions),
            "unique_services": len(self.total_stats.unique_services),
            # Message counts
            "total_messages": self.total_stats.total_messages,
            "claim_messages": self.total_stats.claim_messages,
            "proof_messages": self.total_stats.proof_messages,
        }

        # Append totals row
        df.loc[len(df)] = totals

        return df

    def tabulate(self) -> str:
        """Return a tabulated string representation of the stats."""
        df = self.to_dataframe()
        return tabulate(df, headers="keys", tablefmt="pipe", floatfmt=".6f")

    def tabulate_vertical(self) -> str:
        """Return a tabulated string representation of the stats."""
        df = self.to_dataframe()
        # Transpose and prepare the DataFrame
        df_transposed = df.set_index("block_id").T.reset_index()
        df_transposed.columns.name = None
        df_transposed = df_transposed.rename(columns={"index": "metric"})

        # Format numbers with commas and 2 decimal places
        for col in df_transposed.columns:
            if col != "metric":  # Skip the metric column
                df_transposed[col] = df_transposed[col].apply(
                    lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) else x
                )

        return tabulate(
            df_transposed, headers="keys", tablefmt="pipe", showindex=False, numalign="right", stralign="left"
        )


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

    def _extract_msg_metrics(self, event: Dict) -> Optional[MsgMetrics]:
        if event.get("type") == "message":
            metrics = MsgMetrics()
            for attr in event.get("attributes", []) or []:
                key = attr["key"]
                value = attr["value"]
                if key == "action" and value in ["/poktroll.proof.MsgCreateClaim", "/poktroll.proof.MsgSubmitProof"]:
                    metrics.msg_type = value
                elif key == "sender":
                    metrics.sender = value
                elif key == "module":
                    metrics.module = value
                elif key == "msg_index":
                    metrics.msg_index = value
            return metrics if metrics.msg_type else None
        return None

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
                        metrics.service_ids.add(claim_data["session_header"]["service_id"])
                    except (json.JSONDecodeError, KeyError) as e:
                        self.logger.warning(f"Error parsing claim data: {e}")
        return metrics

    def _extract_event_metrics_settled(self, event: Dict) -> EventMetrics:
        metrics = EventMetrics()
        if event.get("type") == "poktroll.proof.EventClaimSettled":
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
        return metrics

    def _compute_block_stats(self, block_data: dict, block_results: dict) -> BlockStats:
        txs = block_data.get("data", {}).get("txs", [])
        total_tx_bytes = sum(len(base64.b64decode(tx)) for tx in txs)
        tx_mb = total_tx_bytes / (1024 * 1024)

        block_json = json.dumps(block_data)
        block_mb = len(block_json.encode("utf-8")) / (1024 * 1024)

        # Claimed metrics
        claimed_pokt = 0.0
        claimed_compute_units = 0
        claimed_estimated_compute_units = 0
        claimed_relays = 0

        # Settled metrics
        settled_pokt = 0.0
        settled_compute_units = 0
        settled_estimated_compute_units = 0
        settled_relays = 0

        unique_applications = set()
        unique_suppliers = set()
        unique_sessions = set()
        unique_services = set()

        total_messages = 0
        claim_messages = 0
        proof_messages = 0

        for tx_result in block_results.get("txs_results", []) or []:
            for event in tx_result.get("events", []):
                created_metrics = self._extract_event_metrics_created(event)
                settled_metrics = self._extract_event_metrics_settled(event)

                # Update claimed metrics
                claimed_pokt += created_metrics.claimed_pokt
                claimed_compute_units += created_metrics.claimed_compute_units
                claimed_estimated_compute_units += created_metrics.estimated_compute_units
                claimed_relays += created_metrics.num_relays

                # Update settled metrics
                settled_pokt += settled_metrics.claimed_pokt
                settled_compute_units += settled_metrics.claimed_compute_units
                settled_estimated_compute_units += settled_metrics.estimated_compute_units
                settled_relays += settled_metrics.num_relays

                # Update unique sets
                unique_applications.update(created_metrics.application_addresses)
                unique_suppliers.update(created_metrics.supplier_addresses)
                unique_sessions.update(created_metrics.session_ids)
                unique_services.update(created_metrics.service_ids)

                msg_metrics = self._extract_msg_metrics(event)
                if msg_metrics:
                    total_messages += 1
                    if msg_metrics.msg_type == "/poktroll.proof.MsgCreateClaim":
                        claim_messages += 1
                    elif msg_metrics.msg_type == "/poktroll.proof.MsgSubmitProof":
                        proof_messages += 1

        return BlockStats(
            tx_mb=tx_mb,
            block_mb=block_mb,
            num_txs=len(txs),
            claimed_pokt=claimed_pokt,
            claimed_compute_units=claimed_compute_units,
            claimed_estimated_compute_units=claimed_estimated_compute_units,
            claimed_relays=claimed_relays,
            settled_pokt=settled_pokt,
            settled_compute_units=settled_compute_units,
            settled_estimated_compute_units=settled_estimated_compute_units,
            settled_relays=settled_relays,
            unique_applications=unique_applications,
            unique_suppliers=unique_suppliers,
            unique_sessions=unique_sessions,
            unique_services=unique_services,
            total_messages=total_messages,
            claim_messages=claim_messages,
            proof_messages=proof_messages,
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
    print(stats.tabulate_vertical())
    # print(stats.tabulate())
