import json
import logging
import time
import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict
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
class BlockStats:
    total_tx_bytes: int
    num_txs: int


@dataclass
class BlockData:
    block_file: Path
    results_file: Path
    stats: BlockStats


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

    def _compute_block_stats(self, block_data: dict) -> BlockStats:
        txs = block_data.get("data", {}).get("txs", [])
        total_bytes = sum(len(base64.b64decode(tx)) for tx in txs)
        return BlockStats(total_bytes, len(txs))

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
        results_file, _ = self._fetch_single(block_id, is_block_results=True, force=force)

        stats = self._compute_block_stats(block_data)
        return BlockData(block_file, results_file, stats)


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch block data from poktroll network")
    parser.add_argument("--block-id", type=int, default=57333, help="Block height to query")
    parser.add_argument("--output-dir", type=Path, default="/tmp/block_results", help="Output directory")
    parser.add_argument("--node-url", help="Node URL")
    parser.add_argument("--force", action="store_true", help="Force fetch even if cached")
    return parser.parse_args()


def main():
    args = parse_args()

    config = Config(output_dir=args.output_dir, node_url=args.node_url if args.node_url else Config.node_url)

    fetcher = BlockFetcher(config)

    try:
        block_data = fetcher.fetch_block(args.block_id, args.force)
        print("########")
        print(f"Block data saved to: {block_data.block_file}")
        print(f"Block results saved to: {block_data.results_file}")
        print(f"Block stats:")
        print(f"  Total transactions: {block_data.stats.num_txs}")
        print(f"  Total transaction bytes: {block_data.stats.total_tx_bytes}")
        print("########")
    except BlockFetchError as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
