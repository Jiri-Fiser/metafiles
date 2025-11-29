from pathlib import Path

from lxml import etree as ET
import fnmatch
from collections import ChainMap
from typing import Dict, List, Mapping, Optional, Tuple, Any
import re
from copy import deepcopy
from dataclasses import dataclass
import logging
logger = logging.getLogger(__name__)

ns = {"ns": "http://ki.ujep.cz/metafiles"}

uri_ns = {"http://purl.org/dc/elements/1.1/" : "dc",
          "http://purl.org/dc/terms/": "dcterms",
          "http://spdx.org/rdf/terms#" : "spdx",
          "http://www.w3.org/ns/dcat#" : "dcat"}

def clark_to_qname(tag: str, mapping: Dict[str, str]) -> str:
    """
    '{uri}local'  →  'prefix:local'
    """
    if not tag.startswith('{'):
        return tag                                # žádný namespace
    q = ET.QName(tag)                             # rozparsuje na uri/local
    prefix = mapping.get(q.namespace)
    if prefix is None:                            # URI neznáme
        return tag                                # vrátíme beze změny
    return f"{prefix}:{q.localname}" if prefix else q.localname

@dataclass
class LinkInfo:
    type: str
    path: str
    metadata: Dict[str, List[str]]

    def to_dict(self) -> Dict[str, Any]:
        return dict(type=self.type, path=self.path, metadata=self.metadata)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "LinkInfo":
        return LinkInfo(**d)

def transform_dict_values(
        data: Mapping[str, List[str]],
        joiners: Dict[str, str],
        splitters: Dict[str, str]
) -> Dict[str, List[str]]:
    """
    Transforms a dictionary {key: [str]} based on key-specific join and split rules.

    - If key is in `splitters`, each string is split by the splitter and flattened.
    - If key is in `joiners`, values are joined into a single string, wrapped in a one-item list.
    - If key is in neither, the original list is returned.

    :param data: Input dictionary {key: [str]}
    :param joiners: Keys whose values should be joined using a delimiter.
    :param splitters: Keys whose values should be split and flattened.
    :return: Dictionary {key: [str]} — all values are lists.
    """
    result: Dict[str, List[str]] = {}

    for key, values in data.items():
        if key in splitters:
            splitter = splitters[key]
            flattened = []
            for value in values:
                flattened.extend(part.strip() for part in value.split(splitter) if part.strip())
            result[key] = flattened
        elif key in joiners:
            joiner = joiners[key]
            result[key] = [joiner.join(values)]
        else:
            result[key] = values

    return result


def is_prefix(shorter: list, longer: list) -> bool:
    return longer[:len(shorter)] == shorter


def set_attribute(metadata: Dict[str, List[str]],
                  element: ET.Element, attribute_name: Optional[str], meta_name: str) -> None:
    if attribute_name is None:
        value = element.text
    elif attribute_name in element.attrib:
        value = element.attrib[attribute_name]
    else:
        return

    metadata[meta_name] = [value]

def extra_key_value(element: ET.Element) -> Tuple[str, str]:
    element = element[0]
    meta_name = clark_to_qname(element.tag, uri_ns)
    frag = deepcopy(element)
    ET.cleanup_namespaces(frag, top_nsmap={})
    value = ET.tostring(frag, encoding='unicode',
                        method="xml",
                        xml_declaration=False,
                        pretty_print=False)
    return meta_name, value

def set_extra_attribute(metadata: Dict[str, List[str]], element: ET.Element) -> None:
    meta_name, value = extra_key_value(element)
    metadata[meta_name] = ["__xml__:" + value]

def append_attribute(metadata: Dict[str, List[str]],
                     element: ET.Element, attribute_name: Optional[str], meta_name: str):
    if attribute_name is None:
        value = element.text.strip()
    elif attribute_name in element.attrib:
        value = element.attrib[attribute_name]
    else:
        return

    if meta_name in metadata:
        metadata[meta_name].append(value)
    else:
        metadata[meta_name] = [value]

def append_extra_attribute(metadata: Dict[str, List[str]],
                           element: ET.Element) -> None:
    meta_name, value = extra_key_value(element)
    if meta_name in metadata:
        metadata[meta_name].append("__xml__:" + value)
    else:
        metadata[meta_name] = [value]


def process_metadata(metadata: Dict[str, List[str]], meta_element: ET.Element):
    metadata = deepcopy(metadata)
    set_attribute(metadata, meta_element, "creator", "dc:creator")
    set_attribute(metadata, meta_element, "date", "dc:date")
    set_attribute(metadata, meta_element, "description", "dc:description")
    set_attribute(metadata, meta_element, "title", "dc:title")
    set_attribute(metadata, meta_element, "prefix", "mfterms:prefix")
    set_attribute(metadata, meta_element, "meta-manager", "mfterms:meta-manager")
    set_attribute(metadata, meta_element, "data-policy", "mfterms:data-policy")

    append_attribute(metadata, meta_element, "creator.add", "dc:creator")
    append_attribute(metadata, meta_element, "date.add", "dc:date")
    append_attribute(metadata, meta_element, "description.add", "dc:description")
    append_attribute(metadata, meta_element, "title.add", "dc:title")
    append_attribute(metadata, meta_element, "prefix.add", "mfterms:prefix")
    append_attribute(metadata, meta_element, "meta-manager.add", "mfterms:meta-manager")

    if extra_meta := meta_element.xpath("ns:metadata", namespaces=ns):
        for child in extra_meta[0]:
            if child.tag == f"{{{ns['ns']}}}set":
                if len(child) == 0: # nemá dětské elementy
                    set_attribute(metadata, child, None, child.get("type"))
                else:
                    set_extra_attribute(metadata, child)
            elif child.tag == f"{{{ns['ns']}}}add":
                if len(child) == 0:
                    append_attribute(metadata, child, None, child.get("type"))
                else:
                    append_extra_attribute(metadata, child)
            else:
                raise ValueError(f"Unsupported element in metadata section {child.tag}")

    return metadata

def process_links(link_collector: List[LinkInfo], meta_element: ET.Element):
    if links := meta_element.xpath("ns:links", namespaces=ns):
        for child in links[0]:
            if child.tag == f"{{{ns['ns']}}}link":
                type = child.get("type")
                path = child.get("path")
                metadata = process_metadata({}, child)
                link_collector.append(LinkInfo(type, path, metadata))


def collect_dir(collector: List[Mapping[str, List[str]]],
                link_collector: List[LinkInfo],
                file_path: Path,
                root_path: Path,
                meta_path: List[str],
                metadata: Dict[str, List[str]],
                meta_element: ET.Element) -> None:

    if "path" in meta_element.attrib:
        meta_path.append(meta_element.attrib["path"])

    real_path = list(file_path.relative_to(root_path).parent.parts)
    if not is_prefix(meta_path, real_path):
        return

    metadata = process_metadata(metadata, meta_element)

    for element in meta_element:
        if element.tag == f"{{{ns['ns']}}}dir":
            collect_dir(collector, link_collector, file_path, root_path, meta_path,
                        metadata, element)
        elif element.tag == f"{{{ns['ns']}}}files":
            collect_files(collector, link_collector, file_path, root_path, meta_path,
                          metadata, element)


def log(meta_element, meta_path, file_path, meta_name, metadata, link_collector):
    fp = str(file_path)
    mp = "/".join(meta_path + [meta_name])
    logger.info(f"file {fp} match pattern {mp} on row {meta_element.sourceline}, "
                f"metadata: {metadata}, links: {link_collector}")


def collect_files(collector: List[Mapping[str, List[str]]],
                  link_collector: List[LinkInfo],
                  file_path: Path,
                  root_path: Path,
                  meta_path: List[str],
                  metadata: Dict[str, List[str]],
                  meta_element: ET.Element) -> None:

    recursive = meta_element.get('recursive', False)
    real_path = list(file_path.relative_to(root_path).parent.parts)
    file_name = file_path.name

    if not (real_path == meta_path or recursive):
        return

    if ((meta_name := meta_element.get('filename', None)) == file_name
            or
            "filename" not in meta_element.attrib and
            fnmatch.fnmatch(file_name, meta_name := meta_element.get('pattern', "*"))):
        metadata = process_metadata(metadata, meta_element)
        collector.append(metadata)
        process_links(link_collector, meta_element)
        #print(f"*** {file_name} {meta_name} {link_collector} {recursive} ***")
        log(meta_element, meta_path, file_path, meta_name, metadata, link_collector)

def parse_metadata(xml_file: Path, path: Path, root_path:Path) -> Tuple[List[LinkInfo], Mapping[str, List[str]]]:
    tree = ET.parse(xml_file)
    tree.xinclude()
    root = tree.getroot()
    collector = []
    link_collector = []
    collect_dir(collector, link_collector, path, root_path, [], {}, root)
    return (link_collector, transform_dict_values(ChainMap(*reversed(collector)),
                            {"mfterms:prefix": "", "mfterms:meta-manager": "+",
                                    "dc:description": "\n"},
                           {"dc:creator": ",", "dc:contributor": ","}))
