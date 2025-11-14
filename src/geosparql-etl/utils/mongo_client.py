"""MongoDB connection management utilities with context manager support."""

from contextlib import contextmanager
from typing import Optional

from pymongo import MongoClient
from pymongo.database import Database


class MongoConnection:
    """MongoDB connection wrapper with context manager support.

    Example usage:
        with MongoConnection("mongodb://localhost:27018/", "camic") as db:
            results = db.analysis.find()
    """

    def __init__(self, uri: str, db_name: str):
        """Initialize MongoDB connection parameters.

        Args:
            uri: MongoDB connection URI
            db_name: Database name to connect to
        """
        self.uri = uri
        self.db_name = db_name
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None

    def __enter__(self) -> Database:
        """Establish connection and return database object."""
        self.client = MongoClient(self.uri)
        self.db = self.client[self.db_name]
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection on exit."""
        if self.client:
            self.client.close()
        return False


@contextmanager
def mongo_connection(uri: str, db_name: str):
    """Context manager for MongoDB connections.

    Args:
        uri: MongoDB connection URI
        db_name: Database name to connect to

    Yields:
        Database: MongoDB database object

    Example:
        from utils.mongo_client import mongo_connection

        with mongo_connection("mongodb://localhost:27018/", "camic") as db:
            collection = db["analysis"]
            for doc in collection.find():
                print(doc)
    """
    client = None
    try:
        client = MongoClient(uri)
        db = client[db_name]
        yield db
    finally:
        if client:
            client.close()


class ManagedMongoClient:
    """Class-based MongoDB client with manual connection management.

    Useful for long-running processes or when you need explicit control
    over connection lifecycle.

    Example:
        client = ManagedMongoClient("mongodb://localhost:27018/", "camic")
        try:
            client.connect()
            db = client.get_database()
            # ... use db ...
        finally:
            client.close()
    """

    def __init__(self, uri: str, db_name: str):
        """Initialize connection parameters.

        Args:
            uri: MongoDB connection URI
            db_name: Database name to connect to
        """
        self.uri = uri
        self.db_name = db_name
        self.client: Optional[MongoClient] = None

    def connect(self) -> None:
        """Establish connection to MongoDB."""
        if not self.client:
            self.client = MongoClient(self.uri)

    def get_database(self) -> Database:
        """Get database object.

        Returns:
            Database: MongoDB database object

        Raises:
            RuntimeError: If connect() hasn't been called yet
        """
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")
        return self.client[self.db_name]

    def close(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            self.client = None

    def __enter__(self):
        """Support for context manager usage."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support for context manager usage."""
        self.close()
        return False
