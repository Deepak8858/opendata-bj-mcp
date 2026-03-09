"""Preview handlers for different resource formats.

This module provides a modular architecture for previewing various file formats
including CSV, JSON, Excel, and HTML.
"""

import csv
import json
from abc import ABC, abstractmethod
from io import BytesIO, StringIO
from typing import List, Optional, Tuple, Union

from opendata_bj.config import MAX_PREVIEW_BYTES


class PreviewHandler(ABC):
    """Abstract base class for resource preview handlers."""

    @abstractmethod
    def supports(self, format_str: str, mimetype: Optional[str] = None) -> bool:
        """Check if this handler supports the given format.

        Args:
            format_str: The file format (e.g., "CSV", "HTML", "JSON")
            mimetype: Optional MIME type for additional validation

        Returns:
            True if this handler can process the format
        """
        pass

    @abstractmethod
    async def preview(self, content: bytes, max_rows: int) -> Tuple[List[str], List[List[str]]]:
        """Preview the content and return tabular data.

        Args:
            content: Raw bytes content of the resource
            max_rows: Maximum number of data rows to return

        Returns:
            Tuple of (headers, data_rows) where headers is a list of column names
            and data_rows is a list of row values
        """
        pass


class CSVHandler(PreviewHandler):
    """Handler for CSV and TSV files."""

    SUPPORTED_FORMATS = ["CSV", "TXT", "TSV"]

    def supports(self, format_str: str, mimetype: Optional[str] = None) -> bool:
        return format_str.upper() in self.SUPPORTED_FORMATS

    async def preview(self, content: bytes, max_rows: int) -> Tuple[List[str], List[List[str]]]:
        text = content.decode("utf-8", errors="replace")
        lines = text.split("\n")[: max_rows + 1]

        if not lines or not lines[0].strip():
            return [], []

        # Detect delimiter
        sample = lines[0] if lines else ""
        delimiter = "\t" if "\t" in sample else ","

        reader = csv.reader(StringIO("\n".join(lines)), delimiter=delimiter)
        rows = list(reader)

        if not rows:
            return [], []

        headers = rows[0]
        data_rows = rows[1 : max_rows + 1]

        # Pad rows to match header length
        for row in data_rows:
            while len(row) < len(headers):
                row.append("")

        return headers, data_rows


class JSONHandler(PreviewHandler):
    """Handler for JSON files - flattens nested structures."""

    SUPPORTED_FORMATS = ["JSON"]

    def supports(self, format_str: str, mimetype: Optional[str] = None) -> bool:
        return format_str.upper() in self.SUPPORTED_FORMATS

    def _flatten_dict(self, d: dict, parent_key: str = "", sep: str = ".") -> dict:
        """Flatten nested dictionaries into flat key-value pairs."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list) and v and isinstance(v[0], dict):
                items.append((new_key, f"[Array of {len(v)} items]"))
            else:
                items.append((new_key, str(v) if v is not None else ""))
        return dict(items)

    async def preview(self, content: bytes, max_rows: int) -> Tuple[List[str], List[List[str]]]:
        text = content.decode("utf-8", errors="replace")
        data = json.loads(text)

        if isinstance(data, dict):
            # Single object - display as key-value pairs
            flat = self._flatten_dict(data)
            headers = ["Key", "Value"]
            data_rows = [[k, v] for k, v in list(flat.items())[:max_rows]]
            return headers, data_rows

        elif isinstance(data, list) and data:
            # Array of objects
            if isinstance(data[0], dict):
                # Flatten each object and collect all unique keys
                flat_items = [self._flatten_dict(item) for item in data[:max_rows]]
                all_keys = set()
                for item in flat_items:
                    all_keys.update(item.keys())
                headers = sorted(list(all_keys))

                data_rows = []
                for item in flat_items:
                    row = [str(item.get(k, "")) for k in headers]
                    data_rows.append(row)

                return headers, data_rows
            else:
                # Array of primitives
                headers = ["Value"]
                data_rows = [[str(item)] for item in data[:max_rows]]
                return headers, data_rows

        return [], []


class ExcelHandler(PreviewHandler):
    """Handler for Excel files (XLS, XLSX)."""

    SUPPORTED_FORMATS = ["XLS", "XLSX"]

    def supports(self, format_str: str, mimetype: Optional[str] = None) -> bool:
        return format_str.upper() in self.SUPPORTED_FORMATS

    async def preview(self, content: bytes, max_rows: int) -> Tuple[List[str], List[List[str]]]:
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for Excel preview. Install with: pip install pandas")

        df = pd.read_excel(BytesIO(content), nrows=max_rows)

        headers = list(df.columns.astype(str))
        data_rows = df.astype(str).values.tolist()[:max_rows]

        return headers, data_rows


class HTMLHandler(PreviewHandler):
    """Handler for extracting tabular data and structured content from HTML pages."""

    SUPPORTED_FORMATS = ["HTML", "HTM"]

    def supports(self, format_str: str, mimetype: Optional[str] = None) -> bool:
        return format_str.upper() in self.SUPPORTED_FORMATS

    def _extract_table(self, table_soup, max_rows: int) -> Tuple[List[str], List[List[str]]]:
        """Extract headers and rows from a BeautifulSoup table element."""
        headers = []
        rows = []

        # Try to find headers in thead or first row
        thead = table_soup.find("thead")
        if thead:
            th_cells = thead.find_all(["th", "td"])
            headers = [cell.get_text(strip=True) for cell in th_cells]

        tbody = table_soup.find("tbody") or table_soup
        tr_elements = tbody.find_all("tr")

        for tr in tr_elements[:max_rows]:
            cells = tr.find_all(["td", "th"])
            row_data = [cell.get_text(strip=True) for cell in cells]

            if row_data:
                if not headers and tr == tr_elements[0]:
                    # First row might be headers if no thead
                    headers = row_data
                else:
                    rows.append(row_data)

        return headers, rows

    async def preview(self, content: bytes, max_rows: int) -> Tuple[List[str], List[List[str]]]:
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            raise ImportError(
                "beautifulsoup4 is required for HTML preview. "
                "Install with: pip install beautifulsoup4"
            )

        text = content.decode("utf-8", errors="replace")
        soup = BeautifulSoup(text, "html.parser")

        # 1. Try to extract HTML tables
        tables = soup.find_all("table")
        for table in tables:
            headers, rows = self._extract_table(table, max_rows)
            if headers or rows:
                # Pad rows to match header length
                if headers:
                    for row in rows:
                        while len(row) < len(headers):
                            row.append("")
                return headers, rows

        # 2. Try definition lists (dl/dt/dd)
        dls = soup.find_all("dl")
        if dls:
            rows = []
            for dl in dls:
                dts = dl.find_all("dt")
                dds = dl.find_all("dd")
                for dt, dd in zip(dts, dds[:max_rows]):
                    rows.append([dt.get_text(strip=True), dd.get_text(strip=True)])
            if rows:
                return ["Property", "Value"], rows[:max_rows]

        # 3. Try structured sections (headings with content)
        sections = []
        headings = soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"])
        for heading in headings[:max_rows]:
            title = heading.get_text(strip=True)
            # Get next sibling text until next heading
            content_parts = []
            sibling = heading.find_next_sibling()
            while sibling and sibling.name not in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                if sibling.name in ["p", "div", "span"]:
                    text_content = sibling.get_text(strip=True)
                    if text_content:
                        content_parts.append(text_content)
                sibling = sibling.find_next_sibling()

            if title:
                sections.append([heading.name.upper(), title, " ".join(content_parts)[:200]])

        if sections:
            return ["Type", "Title", "Content"], sections

        # 4. Fallback: extract paragraphs with meaningful content
        paragraphs = soup.find_all("p")
        rows = []
        for p in paragraphs[:max_rows]:
            text_content = p.get_text(strip=True)
            if text_content and len(text_content) > 20:
                rows.append([text_content[:500]])

        if rows:
            return ["Content"], rows

        return ["Info"], [["No extractable structured data found in HTML"]]


# Registry of all available handlers
_HANDLERS: List[PreviewHandler] = [
    CSVHandler(),
    JSONHandler(),
    ExcelHandler(),
    HTMLHandler(),
]


def get_handler(format_str: str, mimetype: Optional[str] = None) -> Optional[PreviewHandler]:
    """Get the appropriate handler for a given format.

    Args:
        format_str: The file format (e.g., "CSV", "HTML")
        mimetype: Optional MIME type for additional validation

    Returns:
        PreviewHandler instance or None if no handler supports the format
    """
    for handler in _HANDLERS:
        if handler.supports(format_str, mimetype):
            return handler
    return None


def register_handler(handler: PreviewHandler) -> None:
    """Register a new preview handler.

    Args:
        handler: PreviewHandler instance to add to the registry
    """
    _HANDLERS.append(handler)


def get_supported_formats() -> List[str]:
    """Get list of all supported formats.

    Returns:
        List of uppercase format strings
    """
    formats = set()
    for handler in _HANDLERS:
        if hasattr(handler, "SUPPORTED_FORMATS"):
            formats.update(handler.SUPPORTED_FORMATS)
    return sorted(list(formats))
