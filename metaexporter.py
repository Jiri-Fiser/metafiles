from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import DC, RDF
from typing import Dict, List

def dict_to_rdf(data: Dict[str, List[str]], subject_uri: str = "http://example.org/resource") -> Graph:
    """
    Converts a dictionary with prefixed RDF terms into an RDF graph,
    skipping entries with prefix 'mfterms'.

    :param data: Dict with keys like 'dc:title' and values as lists of strings
    :param subject_uri: URI of the RDF subject
    :return: RDFLib Graph
    """
    g = Graph()
    subject = URIRef(subject_uri)

    # Define known namespace prefixes
    prefix_map = {
        'dc': DC,
        'rdf': RDF
        # Add more known prefixes here if needed
    }

    for full_key, values in data.items():
        if full_key.startswith("mfterms:"):
            continue  # Skip private/internal metadata

        if ":" not in full_key:
            continue  # Skip invalid key

        prefix, local = full_key.split(":", 1)

        if prefix not in prefix_map:
            continue  # Unknown prefix, skip or log warning

        predicate_ns = prefix_map[prefix]
        predicate = predicate_ns[local]

        for value in values:
            g.add((subject, predicate, Literal(value)))

    return g
