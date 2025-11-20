"""MongoDB Harvester utilities package.

This package provides utility modules for MongoDB ETL operations including:
- MongoDB connection management
- Configuration handling
- Checkpoint/state management
- Logging utilities
- JSON serialization for MongoDB documents
- Geometry processing
- File operations
- RDF/Graph utilities
"""

from .checkpoint import CheckpointManager, SimpleCheckpoint
from .config import AppConfig, ETLConfig, MongoConfig, load_config_from_file
from .file_utils import (directory_exists, ensure_directory,
                         ensure_parent_directory, file_exists,
                         generate_batch_filename, generate_output_filename,
                         list_files_with_extension)
from .geometry import (calculate_geometry_area, calculate_geometry_bounds,
                       calculate_geometry_length, extract_geometry_from_mark,
                       geometry_to_geojson, geometry_to_wkt, get_geometry_type,
                       is_valid_geometry, safe_geometry_to_wkt)
from .logger import (get_logger, setup_basic_logger, setup_etl_logger,
                     setup_logger)
from .mongo_client import ManagedMongoClient, MongoConnection, mongo_connection
from .rdf_utils import (EX, GEO, PROV, add_label, add_provenance, add_type,
                        add_wkt_geometry, create_analysis_uri,
                        create_execution_uri, create_graph, create_image_uri,
                        create_mark_uri, create_uri, load_graph,
                        serialize_graph)
from .serialization import (MongoJSONEncoder, clean_mongo_document,
                            objectid_to_str, save_mongo_document_to_file,
                            save_mongo_documents_to_file,
                            serialize_mongo_document,
                            serialize_mongo_documents, str_to_objectid)

__version__ = "0.1.3"

__all__ = [
    # mongo_client
    "MongoConnection",
    "mongo_connection",
    "ManagedMongoClient",
    # config
    "MongoConfig",
    "ETLConfig",
    "AppConfig",
    "load_config_from_file",
    # checkpoint
    "CheckpointManager",
    "SimpleCheckpoint",
    # logger
    "setup_logger",
    "setup_basic_logger",
    "setup_etl_logger",
    "get_logger",
    # serialization
    "MongoJSONEncoder",
    "clean_mongo_document",
    "serialize_mongo_document",
    "serialize_mongo_documents",
    "save_mongo_document_to_file",
    "save_mongo_documents_to_file",
    "objectid_to_str",
    "str_to_objectid",
    # geometry
    "extract_geometry_from_mark",
    "geometry_to_wkt",
    "geometry_to_geojson",
    "calculate_geometry_area",
    "calculate_geometry_length",
    "calculate_geometry_bounds",
    "is_valid_geometry",
    "get_geometry_type",
    "safe_geometry_to_wkt",
    # file_utils
    "ensure_directory",
    "ensure_parent_directory",
    "generate_output_filename",
    "generate_batch_filename",
    "list_files_with_extension",
    "file_exists",
    "directory_exists",
    # rdf_utils
    "create_graph",
    "create_uri",
    "create_mark_uri",
    "create_analysis_uri",
    "create_image_uri",
    "create_execution_uri",
    "add_wkt_geometry",
    "add_provenance",
    "add_label",
    "add_type",
    "serialize_graph",
    "load_graph",
    "GEO",
    "PROV",
    "EX",
]
