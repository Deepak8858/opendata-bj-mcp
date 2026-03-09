"""Tests for preview handlers."""

import pytest
from opendata_bj.tools.preview_handlers import (
    CSVHandler,
    JSONHandler,
    HTMLHandler,
    get_handler,
    get_supported_formats,
)


class TestCSVHandler:
    """Tests for CSV preview handler."""

    @pytest.mark.asyncio
    async def test_supports_csv_format(self):
        handler = CSVHandler()
        assert handler.supports("CSV") is True
        assert handler.supports("csv") is True
        assert handler.supports("TXT") is True
        assert handler.supports("TSV") is True
        assert handler.supports("JSON") is False

    @pytest.mark.asyncio
    async def test_preview_simple_csv(self):
        handler = CSVHandler()
        content = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago"

        headers, rows = await handler.preview(content, max_rows=10)

        assert headers == ["name", "age", "city"]
        assert len(rows) == 3
        assert rows[0] == ["Alice", "30", "NYC"]
        assert rows[1] == ["Bob", "25", "LA"]

    @pytest.mark.asyncio
    async def test_preview_csv_with_max_rows(self):
        handler = CSVHandler()
        content = b"a,b,c\n1,2,3\n4,5,6\n7,8,9"

        headers, rows = await handler.preview(content, max_rows=2)

        assert headers == ["a", "b", "c"]
        assert len(rows) == 2
        assert rows[0] == ["1", "2", "3"]
        assert rows[1] == ["4", "5", "6"]

    @pytest.mark.asyncio
    async def test_preview_tsv(self):
        handler = CSVHandler()
        content = b"name\tage\tcity\nAlice\t30\tNYC\nBob\t25\tLA"

        headers, rows = await handler.preview(content, max_rows=10)

        assert headers == ["name", "age", "city"]
        assert len(rows) == 2


class TestJSONHandler:
    """Tests for JSON preview handler."""

    @pytest.mark.asyncio
    async def test_supports_json_format(self):
        handler = JSONHandler()
        assert handler.supports("JSON") is True
        assert handler.supports("json") is True
        assert handler.supports("CSV") is False

    @pytest.mark.asyncio
    async def test_preview_single_object(self):
        handler = JSONHandler()
        content = b'{"name": "Alice", "age": 30, "city": "NYC"}'

        headers, rows = await handler.preview(content, max_rows=10)

        assert headers == ["Key", "Value"]
        assert len(rows) == 3
        assert ["name", "Alice"] in rows
        assert ["age", "30"] in rows
        assert ["city", "NYC"] in rows

    @pytest.mark.asyncio
    async def test_preview_array_of_objects(self):
        handler = JSONHandler()
        content = b'[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]'

        headers, rows = await handler.preview(content, max_rows=10)

        assert "age" in headers
        assert "name" in headers
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_preview_nested_object(self):
        handler = JSONHandler()
        content = b'{"user": {"name": "Alice", "email": "alice@example.com"}}'

        headers, rows = await handler.preview(content, max_rows=10)

        assert headers == ["Key", "Value"]
        assert ["user.name", "Alice"] in rows
        assert ["user.email", "alice@example.com"] in rows

    @pytest.mark.asyncio
    async def test_preview_array_of_primitives(self):
        handler = JSONHandler()
        content = b'["apple", "banana", "cherry"]'

        headers, rows = await handler.preview(content, max_rows=10)

        assert headers == ["Value"]
        assert len(rows) == 3
        assert ["apple"] in rows
        assert ["banana"] in rows


class TestHTMLHandler:
    """Tests for HTML preview handler."""

    @pytest.mark.asyncio
    async def test_supports_html_format(self):
        handler = HTMLHandler()
        assert handler.supports("HTML") is True
        assert handler.supports("html") is True
        assert handler.supports("HTM") is True
        assert handler.supports("CSV") is False

    @pytest.mark.asyncio
    async def test_preview_html_table(self):
        handler = HTMLHandler()
        content = b"""
        <html>
            <body>
                <table>
                    <tr><th>Name</th><th>Age</th></tr>
                    <tr><td>Alice</td><td>30</td></tr>
                    <tr><td>Bob</td><td>25</td></tr>
                </table>
            </body>
        </html>
        """

        headers, rows = await handler.preview(content, max_rows=10)

        assert "Name" in headers
        assert "Age" in headers
        assert len(rows) == 2
        assert ["Alice", "30"] in rows or any("Alice" in str(cell) for row in rows for cell in row)

    @pytest.mark.asyncio
    async def test_preview_html_with_thead(self):
        handler = HTMLHandler()
        content = b"""
        <table>
            <thead><tr><th>Product</th><th>Price</th></tr></thead>
            <tbody>
                <tr><td>Apple</td><td>1.50</td></tr>
                <tr><td>Banana</td><td>0.75</td></tr>
            </tbody>
        </table>
        """

        headers, rows = await handler.preview(content, max_rows=10)

        assert "Product" in headers
        assert "Price" in headers
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_preview_html_definition_list(self):
        handler = HTMLHandler()
        content = b"""
        <html>
            <body>
                <dl>
                    <dt>Author</dt><dd>John Doe</dd>
                    <dt>Published</dt><dd>2024</dd>
                </dl>
            </body>
        </html>
        """

        headers, rows = await handler.preview(content, max_rows=10)

        assert headers == ["Property", "Value"]
        assert len(rows) == 2
        assert ["Author", "John Doe"] in rows

    @pytest.mark.asyncio
    async def test_preview_html_no_structured_data(self):
        handler = HTMLHandler()
        content = b"<html><body><p>Just some text here.</p></body></html>"

        headers, rows = await handler.preview(content, max_rows=10)

        assert len(headers) >= 1
        assert len(rows) >= 1


class TestHandlerRegistry:
    """Tests for handler registry functions."""

    def test_get_handler_csv(self):
        handler = get_handler("CSV")
        assert handler is not None
        assert isinstance(handler, CSVHandler)

    def test_get_handler_json(self):
        handler = get_handler("JSON")
        assert handler is not None
        assert isinstance(handler, JSONHandler)

    def test_get_handler_html(self):
        handler = get_handler("HTML")
        assert handler is not None
        assert isinstance(handler, HTMLHandler)

    def test_get_handler_unsupported(self):
        handler = get_handler("PDF")
        assert handler is None

    def test_get_supported_formats(self):
        formats = get_supported_formats()
        assert "CSV" in formats
        assert "JSON" in formats
        assert "HTML" in formats
        assert "XLS" in formats
        assert "XLSX" in formats
