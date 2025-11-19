"""Geometry processing utilities for MongoDB spatial data."""

import logging
from typing import Any, Dict, Optional

from shapely.geometry import LineString, Point, Polygon, shape
from shapely.geometry.base import BaseGeometry

logger = logging.getLogger(__name__)


def extract_geometry_from_mark(mark: Dict[str, Any]) -> Optional[BaseGeometry]:
    """Extract Shapely geometry from MongoDB mark document.

    Args:
        mark: MongoDB document containing geometries field

    Returns:
        Shapely geometry object or None if extraction fails

    Example:
        from utils.geometry import extract_geometry_from_mark

        mark = db.mark.find_one()
        geom = extract_geometry_from_mark(mark)
        if geom:
            print(f"Area: {geom.area}")
    """
    try:
        if "geometries" not in mark:
            logger.warning(f"No geometries field in mark {mark.get('_id')}")
            return None

        features = mark["geometries"].get("features", [])
        if not features:
            logger.warning(f"No features in mark {mark.get('_id')}")
            return None

        geometry_dict = features[0].get("geometry")
        if not geometry_dict:
            logger.warning(f"No geometry in first feature of mark {mark.get('_id')}")
            return None

        return shape(geometry_dict)

    except Exception as e:
        logger.warning(f"Geometry extraction error for mark {mark.get('_id')}: {e}")
        return None


def geometry_to_wkt(mark: Dict[str, Any]) -> Optional[str]:
    """Convert MongoDB mark geometry to WKT (Well-Known Text) format.

    Args:
        mark: MongoDB document containing geometries field

    Returns:
        WKT string representation or None if conversion fails

    Example:
        from utils.geometry import geometry_to_wkt

        mark = db.mark.find_one()
        wkt = geometry_to_wkt(mark)
        if wkt:
            print(f"WKT: {wkt}")
    """
    geom = extract_geometry_from_mark(mark)
    if geom:
        return geom.wkt
    return None


def geometry_to_geojson(mark: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert MongoDB mark geometry to GeoJSON format.

    Args:
        mark: MongoDB document containing geometries field

    Returns:
        GeoJSON dictionary or None if conversion fails

    Example:
        from utils.geometry import geometry_to_geojson

        mark = db.mark.find_one()
        geojson = geometry_to_geojson(mark)
    """
    geom = extract_geometry_from_mark(mark)
    if geom:
        return geom.__geo_interface__
    return None


def calculate_geometry_area(mark: Dict[str, Any]) -> Optional[float]:
    """Calculate area of geometry in mark document.

    Args:
        mark: MongoDB document containing geometries field

    Returns:
        Area value or None if calculation fails

    Example:
        area = calculate_geometry_area(mark)
        if area:
            print(f"Area: {area} square units")
    """
    geom = extract_geometry_from_mark(mark)
    if geom:
        return geom.area
    return None


def calculate_geometry_length(mark: Dict[str, Any]) -> Optional[float]:
    """Calculate length/perimeter of geometry in mark document.

    Args:
        mark: MongoDB document containing geometries field

    Returns:
        Length value or None if calculation fails

    Example:
        length = calculate_geometry_length(mark)
        if length:
            print(f"Length: {length} units")
    """
    geom = extract_geometry_from_mark(mark)
    if geom:
        return geom.length
    return None


def calculate_geometry_bounds(mark: Dict[str, Any]) -> Optional[tuple]:
    """Calculate bounding box of geometry in mark document.

    Args:
        mark: MongoDB document containing geometries field

    Returns:
        Tuple of (minx, miny, maxx, maxy) or None if calculation fails

    Example:
        bounds = calculate_geometry_bounds(mark)
        if bounds:
            minx, miny, maxx, maxy = bounds
            print(f"Bounds: {bounds}")
    """
    geom = extract_geometry_from_mark(mark)
    if geom:
        return geom.bounds
    return None


def is_valid_geometry(mark: Dict[str, Any]) -> bool:
    """Check if mark contains valid geometry.

    Args:
        mark: MongoDB document containing geometries field

    Returns:
        bool: True if geometry is valid, False otherwise

    Example:
        if is_valid_geometry(mark):
            print("Geometry is valid")
    """
    geom = extract_geometry_from_mark(mark)
    if geom:
        return geom.is_valid
    return False


def get_geometry_type(mark: Dict[str, Any]) -> Optional[str]:
    """Get geometry type from mark document.

    Args:
        mark: MongoDB document containing geometries field

    Returns:
        Geometry type string (e.g., "Polygon", "Point", "LineString") or None

    Example:
        geom_type = get_geometry_type(mark)
        print(f"Geometry type: {geom_type}")
    """
    geom = extract_geometry_from_mark(mark)
    if geom:
        return geom.geom_type
    return None


def create_point(x: float, y: float) -> Point:
    """Create a Shapely Point geometry.

    Args:
        x: X coordinate
        y: Y coordinate

    Returns:
        Point: Shapely Point object

    Example:
        point = create_point(10.5, 20.3)
        print(point.wkt)
    """
    return Point(x, y)


def create_polygon(coordinates: list) -> Polygon:
    """Create a Shapely Polygon geometry.

    Args:
        coordinates: List of (x, y) coordinate tuples

    Returns:
        Polygon: Shapely Polygon object

    Example:
        coords = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]
        polygon = create_polygon(coords)
        print(polygon.area)
    """
    return Polygon(coordinates)


def create_linestring(coordinates: list) -> LineString:
    """Create a Shapely LineString geometry.

    Args:
        coordinates: List of (x, y) coordinate tuples

    Returns:
        LineString: Shapely LineString object

    Example:
        coords = [(0, 0), (1, 1), (2, 0)]
        line = create_linestring(coords)
        print(line.length)
    """
    return LineString(coordinates)


def safe_geometry_to_wkt(mark: Dict[str, Any], default: str = "POINT EMPTY") -> str:
    """Safely convert geometry to WKT with fallback.

    Args:
        mark: MongoDB document containing geometries field
        default: Default WKT string if conversion fails

    Returns:
        WKT string (actual geometry or default)

    Example:
        wkt = safe_geometry_to_wkt(mark)
        # Always returns a valid WKT string
    """
    wkt = geometry_to_wkt(mark)
    return wkt if wkt else default
