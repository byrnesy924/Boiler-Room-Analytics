"""Central registry of every file path the pipeline reads from or writes to.

All paths are resolved from the project root (not the current working directory), so
scripts behave the same regardless of where they are launched from.

Usage:
    from src.utils.data_loading import PATHS

    df = pd.read_csv(PATHS.raw.tracklist_1001_set_lists)
    df.to_csv(PATHS.processed.cleaned_boiler_room_data_csv, index=False)

For tests, build an isolated instance: ``ProjectPaths(root=tmp_path)``.
"""

from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class RawDataPaths:
    """Files in ``data/raw/``."""

    dir: Path

    @property
    def tracklist_1001_set_lists(self) -> Path:
        """Output of the 1001tracklists crawler; input to cleaning."""
        return self.dir / "1001_tracklist_set_lists.csv"

    @property
    def live_tracklist_set_lists(self) -> Path:
        """Output of the livetracklist crawler."""
        return self.dir / "live_tracklist_set_lists.csv"

    @property
    def cleaned_boiler_room_data_parquet(self) -> Path:
        """Cleaned data (parquet); input to genre download and network analytics."""
        return self.dir / "cleaned_boiler_room_data.parquet"


@dataclass(frozen=True)
class ProcessedDataPaths:
    """Files in ``data/processed/``."""

    dir: Path

    @property
    def cleaned_boiler_room_data_csv(self) -> Path:
        """Cleaned data (csv)."""
        return self.dir / "cleaned_boiler_room_data.csv"

    @property
    def cleaned_boiler_room_data_with_genre(self) -> Path:
        """Cleaned data enriched with Discogs/Spotify genres."""
        return self.dir / "cleaned_boiler_room_data_with_genre.parquet"

    @property
    def preprocessed_br_data(self) -> Path:
        """Intermediate output of cleaning (before fuzzy artist matching)."""
        return self.dir / "preprocessed_br_data.csv"

    @property
    def fuzzy_matching(self) -> Path:
        """Artist pairs with string similarity > 65, for manual review."""
        return self.dir / "FuzzyMatching.csv"



@dataclass(frozen=True)
class ScraperCachePaths:
    """Locally cached HTML from 1001tracklists (the site rate-limits requests)."""

    root: Path

    @property
    def tracklist_html_dir(self) -> Path:
        """One cached HTML file per set."""
        return self.root / "1001_tracklist_htmls"

    @property
    def boiler_room_index_html(self) -> Path:
        """Manually saved Boiler Room index page (fallback when the request fails)."""
        return self.root / "1001Tracklist-BR-Index.html"

    def tracklist_html(self, set_file_stem: str) -> Path:
        """Cached HTML file for a single set."""
        return self.tracklist_html_dir / f"{set_file_stem}.html"


@dataclass(frozen=True)
class ImagePaths:
    """Figures in ``Images/``."""

    dir: Path

    @property
    def similarity_histogram(self) -> Path:
        return self.dir / "histogram_of_similarity_values.png"

    @property
    def similarity_histogram_tail(self) -> Path:
        return self.dir / "tail_of_histogram.png"


@dataclass(frozen=True)
class ProjectPaths:
    """Top-level container for all project paths."""

    root: Path = PROJECT_ROOT

    @property
    def data_dir(self) -> Path:
        return self.root / "data"

    @property
    def raw(self) -> RawDataPaths:
        return RawDataPaths(dir=self.data_dir / "raw")

    @property
    def processed(self) -> ProcessedDataPaths:
        return ProcessedDataPaths(dir=self.data_dir / "processed")

    @property
    def scraper_cache(self) -> ScraperCachePaths:
        return ScraperCachePaths(root=self.root)

    @property
    def images(self) -> ImagePaths:
        return ImagePaths(dir=self.root / "Images")

    @property
    def env_file(self) -> Path:
        """Credentials for Graphistry, Spotify and Discogs."""
        return self.root / ".env"


PATHS = ProjectPaths()
