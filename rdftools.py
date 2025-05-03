from rdflib import Graph, URIRef, Literal, BNode
from rdflib.namespace import DC, RDF, DCTERMS, DCAT, Namespace
from typing import Dict, List
from metaparser import LinkInfo
from xml.etree.ElementTree import Element
import xml.etree.ElementTree as ET

def extract_nsmap(xml_string: str) -> dict:
    """
    Získá mapu prefix → namespace URI z XML textu.
    (Používá SAX parser kvůli přístupu k xmlns definicím.)
    """
    from xml.sax.handler import ContentHandler
    from xml.sax import make_parser
    from io import StringIO

    ns_map = {}

    class NamespaceHandler(ContentHandler):
        def startPrefixMapping(self, prefix, uri):
            ns_map[prefix] = uri

    parser = make_parser()
    handler = NamespaceHandler()
    parser.setContentHandler(handler)
    parser.setFeature("http://xml.org/sax/features/namespaces", True)
    parser.parse(StringIO(xml_string))

    return ns_map

def etree_to_rdf(graph: Graph, parent_subject, element: Element, ns_map: dict) -> URIRef:
    """
    Převede jeden XML element (etree) do RDF a připojí k parent_subject pomocí názvu tagu.
    Vrací vytvořený uzel (např. blank node), ke kterému jsou připojeny další predikáty.
    """
    tag_uri = tag_to_uri(element.tag, ns_map)
    this_node = BNode()
    graph.add((parent_subject, tag_uri, this_node))

    # Typ, pokud je tag třídou (volitelné, ale často smysluplné)
    graph.add((this_node, RDF.type, tag_uri))

    for child in element:
        pred = tag_to_uri(child.tag, ns_map)

        # Pokud má atribut rdf:resource → je to URI
        rdf_resource = child.attrib.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
        if rdf_resource:
            graph.add((this_node, pred, URIRef(rdf_resource)))
        elif child.text and child.text.strip():
            graph.add((this_node, pred, Literal(child.text.strip())))
        elif len(child):  # má podřízené prvky
            etree_to_rdf(graph, this_node, child, ns_map)

    return this_node

def tag_to_uri(tag: str, ns_map: dict) -> URIRef:
    """Převede tag ve formě '{namespace}localname' na URIRef."""
    if tag.startswith('{'):
        uri, local = tag[1:].split('}')
        return URIRef(uri + local)
    else:
        # fallback – namespace podle předpony
        prefix, local = tag.split(':', 1)
        return URIRef(ns_map[prefix] + local)


def addSubnode(g: Graph, subject, fragment:str):
    ns_map = extract_nsmap(fragment)
    root = ET.fromstring(fragment)

    for prefix, uri in ns_map.items():
        g.bind(prefix, uri)

    etree_to_rdf(g, subject, root, ns_map)

def meta_to_rdf(data: Dict[str, List[str]],
                links: List[LinkInfo],
                subject_uri: str) -> Graph:
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
        'dcterms': DCTERMS,
        'rdf': RDF,
        'spdx': Namespace("http://spdx.org/rdf/terms#"),
        'dcat': DCAT,
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
            if not value.startswith("__xml__:"):
                g.add((subject, predicate, Literal(value)))
            else:
                addSubnode(g, subject, value[8:])

    return g