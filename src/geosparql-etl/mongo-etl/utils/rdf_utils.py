"""RDF and graph utilities for MongoDB to RDF conversion."""

from typing import Any, Dict, Optional

from bson import ObjectId
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD

# Common namespace definitions
GEO = Namespace("http://www.opengis.net/ont/geosparql#")
PROV = Namespace("http://www.w3.org/ns/prov#")
EX = Namespace("http://example.org/")


def create_graph(namespaces: Optional[Dict[str, Namespace]] = None) -> Graph:
    """Create an RDF graph with common namespace bindings.

    Args:
        namespaces: Optional dictionary of custom namespaces to bind

    Returns:
        Graph: RDFLib Graph with bound namespaces

    Example:
        from utils.rdf_utils import create_graph, GEO, PROV, EX

        g = create_graph()
        # Graph already has geo, prov, ex namespaces bound
    """
    g = Graph()

    # Bind common namespaces
    g.bind("geo", GEO)
    g.bind("prov", PROV)
    g.bind("ex", EX)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)

    # Bind any additional custom namespaces
    if namespaces:
        for prefix, namespace in namespaces.items():
            g.bind(prefix, namespace)

    return g


def create_uri(namespace: Namespace, entity_type: str, entity_id: Any) -> URIRef:
    """Create a URI reference from namespace, type, and ID.

    Args:
        namespace: RDF namespace
        entity_type: Entity type (e.g., "mark", "analysis", "image")
        entity_id: Entity identifier (ObjectId or string)

    Returns:
        URIRef: URI reference

    Example:
        from utils.rdf_utils import create_uri, EX

        mark_uri = create_uri(EX, "mark", mark_doc["_id"])
        # Returns: http://example.org/mark/507f1f77bcf86cd799439011
    """
    if isinstance(entity_id, ObjectId):
        entity_id = str(entity_id)
    return namespace[f"{entity_type}/{entity_id}"]


def create_mark_uri(mark_id: Any, namespace: Namespace = EX) -> URIRef:
    """Create URI for a mark entity.

    Args:
        mark_id: Mark identifier
        namespace: RDF namespace (default: EX)

    Returns:
        URIRef: Mark URI

    Example:
        mark_uri = create_mark_uri(mark["_id"])
    """
    return create_uri(namespace, "mark", mark_id)


def create_analysis_uri(analysis_id: Any, namespace: Namespace = EX) -> URIRef:
    """Create URI for an analysis entity.

    Args:
        analysis_id: Analysis identifier
        namespace: RDF namespace (default: EX)

    Returns:
        URIRef: Analysis URI

    Example:
        analysis_uri = create_analysis_uri(analysis["_id"])
    """
    return create_uri(namespace, "analysis", analysis_id)


def create_image_uri(image_id: Any, namespace: Namespace = EX) -> URIRef:
    """Create URI for an image entity.

    Args:
        image_id: Image identifier
        namespace: RDF namespace (default: EX)

    Returns:
        URIRef: Image URI

    Example:
        image_uri = create_image_uri(image_id)
    """
    return create_uri(namespace, "image", image_id)


def create_execution_uri(execution_id: Any, namespace: Namespace = EX) -> URIRef:
    """Create URI for an execution entity.

    Args:
        execution_id: Execution identifier
        namespace: RDF namespace (default: EX)

    Returns:
        URIRef: Execution URI

    Example:
        exec_uri = create_execution_uri(exec_id)
    """
    return create_uri(namespace, "execution", execution_id)


def add_wkt_geometry(
    graph: Graph, subject: URIRef, wkt_string: str, predicate: URIRef = GEO.asWKT
) -> None:
    """Add WKT geometry to graph.

    Args:
        graph: RDF graph
        subject: Subject URI
        wkt_string: WKT geometry string
        predicate: Predicate to use (default: geo:asWKT)

    Example:
        from utils.rdf_utils import add_wkt_geometry

        g = create_graph()
        mark_uri = create_mark_uri(mark["_id"])
        add_wkt_geometry(g, mark_uri, "POINT(10 20)")
    """
    wkt_literal = Literal(wkt_string, datatype=GEO.wktLiteral)
    graph.add((subject, predicate, wkt_literal))


def add_provenance(
    graph: Graph,
    entity_uri: URIRef,
    agent_uri: Optional[URIRef] = None,
    activity_uri: Optional[URIRef] = None,
) -> None:
    """Add PROV-O provenance triples to graph.

    Args:
        graph: RDF graph
        entity_uri: Entity URI
        agent_uri: Optional agent URI (wasAttributedTo)
        activity_uri: Optional activity URI (wasGeneratedBy)

    Example:
        add_provenance(
            g,
            mark_uri,
            agent_uri=EX["user/123"],
            activity_uri=EX["activity/456"]
        )
    """
    if agent_uri:
        graph.add((entity_uri, PROV.wasAttributedTo, agent_uri))
    if activity_uri:
        graph.add((entity_uri, PROV.wasGeneratedBy, activity_uri))


def add_label(graph: Graph, subject: URIRef, label: str) -> None:
    """Add rdfs:label to subject.

    Args:
        graph: RDF graph
        subject: Subject URI
        label: Label text

    Example:
        add_label(g, mark_uri, "Tumor boundary annotation")
    """
    graph.add((subject, RDFS.label, Literal(label)))


def add_type(graph: Graph, subject: URIRef, rdf_type: URIRef) -> None:
    """Add rdf:type to subject.

    Args:
        graph: RDF graph
        subject: Subject URI
        rdf_type: Type URI

    Example:
        add_type(g, mark_uri, EX.Annotation)
    """
    graph.add((subject, RDF.type, rdf_type))


def add_literal_property(
    graph: Graph,
    subject: URIRef,
    predicate: URIRef,
    value: Any,
    datatype: Optional[URIRef] = None,
) -> None:
    """Add literal property to graph.

    Args:
        graph: RDF graph
        subject: Subject URI
        predicate: Predicate URI
        value: Literal value
        datatype: Optional datatype URI

    Example:
        add_literal_property(
            g,
            mark_uri,
            EX.confidence,
            0.95,
            datatype=XSD.float
        )
    """
    literal = Literal(value, datatype=datatype) if datatype else Literal(value)
    graph.add((subject, predicate, literal))


def add_object_property(
    graph: Graph, subject: URIRef, predicate: URIRef, obj: URIRef
) -> None:
    """Add object property to graph.

    Args:
        graph: RDF graph
        subject: Subject URI
        predicate: Predicate URI
        obj: Object URI

    Example:
        add_object_property(g, mark_uri, PROV.wasGeneratedBy, activity_uri)
    """
    graph.add((subject, predicate, obj))


def serialize_graph(
    graph: Graph, format: str = "turtle", destination: Optional[str] = None
) -> Optional[str]:
    """Serialize RDF graph to string or file.

    Args:
        graph: RDF graph to serialize
        format: Serialization format (turtle, xml, n3, nt, json-ld)
        destination: Optional file path to write to

    Returns:
        Serialized string if no destination, None if written to file

    Example:
        # Serialize to string
        ttl_string = serialize_graph(g, format="turtle")

        # Serialize to file
        serialize_graph(g, format="turtle", destination="output.ttl")
    """
    if destination:
        graph.serialize(destination=destination, format=format)
        return None
    else:
        return graph.serialize(format=format)


def load_graph(source: str, format: str = "turtle") -> Graph:
    """Load RDF graph from file.

    Args:
        source: File path to load from
        format: Serialization format (turtle, xml, n3, nt, json-ld)

    Returns:
        Graph: Loaded RDF graph

    Example:
        g = load_graph("data.ttl", format="turtle")
    """
    g = create_graph()
    g.parse(source, format=format)
    return g


def merge_graphs(*graphs: Graph) -> Graph:
    """Merge multiple RDF graphs into one.

    Args:
        *graphs: Variable number of Graph objects to merge

    Returns:
        Graph: Merged graph

    Example:
        merged = merge_graphs(graph1, graph2, graph3)
    """
    merged = create_graph()
    for g in graphs:
        for triple in g:
            merged.add(triple)
    return merged


def get_namespaces() -> Dict[str, Namespace]:
    """Get dictionary of common namespaces.

    Returns:
        Dict mapping prefix strings to Namespace objects

    Example:
        namespaces = get_namespaces()
        geo_ns = namespaces["geo"]
    """
    return {
        "geo": GEO,
        "prov": PROV,
        "ex": EX,
        "rdf": RDF,
        "rdfs": RDFS,
        "xsd": XSD,
    }
