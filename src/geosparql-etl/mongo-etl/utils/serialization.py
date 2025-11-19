"""JSON serialization utilities for MongoDB documents."""

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict

from bson import ObjectId


class MongoJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for MongoDB documents.

    Handles ObjectId, datetime, date, and Decimal types.

    Example:
        from utils.serialization import MongoJSONEncoder

        doc = {"_id": ObjectId(), "timestamp": datetime.now()}
        json_str = json.dumps(doc, cls=MongoJSONEncoder)
    """

    def default(self, obj: Any) -> Any:
        """Convert MongoDB-specific types to JSON-serializable formats.

        Args:
            obj: Object to serialize

        Returns:
            JSON-serializable representation of the object
        """
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def clean_mongo_document(
    doc: Dict[str, Any], convert_id: bool = True
) -> Dict[str, Any]:
    """Clean MongoDB document for JSON serialization.

    Recursively converts ObjectId and datetime objects to strings.

    Args:
        doc: MongoDB document dictionary
        convert_id: Whether to convert _id field (default: True)

    Returns:
        Cleaned document dictionary

    Example:
        from utils.serialization import clean_mongo_document

        doc = db.collection.find_one()
        clean_doc = clean_mongo_document(doc)
        json.dumps(clean_doc)  # Now serializable
    """
    if not isinstance(doc, dict):
        return doc

    cleaned = {}
    for key, value in doc.items():
        if key == "_id" and convert_id and isinstance(value, ObjectId):
            cleaned[key] = str(value)
        elif isinstance(value, ObjectId):
            cleaned[key] = str(value)
        elif isinstance(value, (datetime, date)):
            cleaned[key] = value.isoformat()
        elif isinstance(value, Decimal):
            cleaned[key] = float(value)
        elif isinstance(value, dict):
            cleaned[key] = clean_mongo_document(value, convert_id)
        elif isinstance(value, list):
            cleaned[key] = [
                (
                    clean_mongo_document(item, convert_id)
                    if isinstance(item, dict)
                    else item
                )
                for item in value
            ]
        else:
            cleaned[key] = value

    return cleaned


def serialize_mongo_document(doc: Dict[str, Any], indent: int = 2) -> str:
    """Serialize MongoDB document to JSON string.

    Args:
        doc: MongoDB document
        indent: JSON indentation (default: 2)

    Returns:
        JSON string representation

    Example:
        doc = db.collection.find_one()
        json_str = serialize_mongo_document(doc)
    """
    return json.dumps(doc, cls=MongoJSONEncoder, indent=indent)


def serialize_mongo_documents(docs: list, indent: int = 2) -> str:
    """Serialize list of MongoDB documents to JSON string.

    Args:
        docs: List of MongoDB documents
        indent: JSON indentation (default: 2)

    Returns:
        JSON string representation

    Example:
        docs = list(db.collection.find())
        json_str = serialize_mongo_documents(docs)
    """
    return json.dumps(docs, cls=MongoJSONEncoder, indent=indent)


def save_mongo_document_to_file(
    doc: Dict[str, Any], file_path: str, indent: int = 2
) -> None:
    """Save MongoDB document to JSON file.

    Args:
        doc: MongoDB document
        file_path: Path to output file
        indent: JSON indentation (default: 2)

    Example:
        doc = db.collection.find_one()
        save_mongo_document_to_file(doc, "output.json")
    """
    with open(file_path, "w") as f:
        json.dump(doc, f, cls=MongoJSONEncoder, indent=indent)


def save_mongo_documents_to_file(docs: list, file_path: str, indent: int = 2) -> None:
    """Save list of MongoDB documents to JSON file.

    Args:
        docs: List of MongoDB documents
        file_path: Path to output file
        indent: JSON indentation (default: 2)

    Example:
        docs = list(db.collection.find())
        save_mongo_documents_to_file(docs, "output.json")
    """
    with open(file_path, "w") as f:
        json.dump(docs, f, cls=MongoJSONEncoder, indent=indent)


def objectid_to_str(obj_id: ObjectId) -> str:
    """Convert ObjectId to string.

    Args:
        obj_id: ObjectId to convert

    Returns:
        String representation of ObjectId

    Example:
        from bson import ObjectId
        obj_id = ObjectId()
        str_id = objectid_to_str(obj_id)
    """
    return str(obj_id)


def str_to_objectid(str_id: str) -> ObjectId:
    """Convert string to ObjectId.

    Args:
        str_id: String to convert

    Returns:
        ObjectId instance

    Raises:
        ValueError: If string is not a valid ObjectId

    Example:
        str_id = "507f1f77bcf86cd799439011"
        obj_id = str_to_objectid(str_id)
    """
    try:
        return ObjectId(str_id)
    except Exception as e:
        raise ValueError(f"Invalid ObjectId string: {str_id}") from e
