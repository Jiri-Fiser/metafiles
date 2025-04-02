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


def join_path(dir_path: str, file_name: str) -> str:
    """
    Joins a directory path and a file name using string operations only.

    Handles edge cases such as:
      - dir_path ending with or without a slash,
      - dir_path being empty,
      - dir_path being "/".

    :param dir_path: Path to the directory as a string.
    :param file_name: Name of the file.
    :return: Full path as a string.
    """
    if dir_path in ("", "."):
        return file_name
    if dir_path == "/":
        return "/" + file_name
    if dir_path.endswith("/"):
        return dir_path + file_name
    return dir_path + "/" + file_name


def normalize_path(path: str) -> str:
    """
    Normalize a file path by replacing multiple '/' with a single '/'
    and removing a trailing '/' unless the path is just '/'.

    Args:
        path (str): The input path string.

    Returns:
        str: The normalized path.
    """
    path = re.sub(r'/+', '/', path)  # Nahradí vícenásobná lomítka jedním
    return path.rstrip('/') if path != '/' else path  #

def set_attribute(metadata: Dict[str, List[str]],
                  element: ET.Element, attribute_name: Optional[str], meta_name: str) -> None:
    if attribute_name is None:
        value = element.text
    elif attribute_name in element.attrib:
        value = element.attrib[attribute_name]
    else:
        return

    metadata[meta_name] = [value]

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


def process_metadata(metadata: Dict[str, List[str]], meta_element: ET.Element):
    metadata = deepcopy(metadata)
    set_attribute(metadata, meta_element, "creator", "dc:creator")
    set_attribute(metadata, meta_element, "date", "dc:date")
    set_attribute(metadata, meta_element, "description", "dc:description")
    set_attribute(metadata, meta_element, "title", "dc:title")
    set_attribute(metadata, meta_element, "prefix", "mfterms:prefix")
    set_attribute(metadata, meta_element, "meta-manager", "mfterms:meta-manager")

    append_attribute(metadata, meta_element, "creator.add", "dc:creator")
    append_attribute(metadata, meta_element, "date.add", "dc:date")
    append_attribute(metadata, meta_element, "description.add", "dc:description")
    append_attribute(metadata, meta_element, "title.add", "dc:title")
    append_attribute(metadata, meta_element, "prefix.add", "mfterms:prefix")
    append_attribute(metadata, meta_element, "meta-manager.add", "mfterms:meta-manager")

    if extra_meta := meta_element.xpath("ns:metadata", namespaces=ns):
        for child in extra_meta[0]:
            if child.tag == f"{{{ns['ns']}}}set":
                set_attribute(metadata, child, None, child.get("type"))
            elif child.tag == f"{{{ns['ns']}}}add":
                append_attribute(metadata, child, None, child.get("type"))

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
                file_path: str, file_name: str,
                meta_path: str,
                metadata: Dict[str, List[str]],
                meta_element: ET.Element) -> None:

    meta_path = normalize_path(meta_path + "/" + meta_element.get('path', ""))
    if not file_path.startswith(meta_path):
        return

    metadata = process_metadata(metadata, meta_element)

    for element in meta_element:
        if element.tag == f"{{{ns['ns']}}}dir":
            collect_dir(collector, link_collector, file_path, file_name, meta_path,
                        metadata, element)
        elif element.tag == f"{{{ns['ns']}}}files":
            collect_files(collector, link_collector, file_path, file_name, meta_path,
                          metadata, element)


def log(meta_element, meta_path, file_path, file_name, meta_name, metadata, link_collector):
    fp = join_path(file_path, file_name)
    mp = join_path(meta_path, meta_name)
    logger.info(f"file {fp} match pattern {mp} on row {meta_element.sourceline}, "
                f"metadata: {metadata}, links: {link_collector}")


def collect_files(collector: List[Mapping[str, List[str]]],
                  link_collector: List[LinkInfo],
                  file_path: str, file_name: str,
                  meta_path: str,
                  metadata: Dict[str, List[str]],
                  meta_element: ET.Element) -> None:

    recursive = meta_element.get('recursive', False)
    if not (file_path == meta_path or recursive):
        return

    if ((meta_name := meta_element.get('filename', None)) == file_name
            or fnmatch.fnmatch(file_name, meta_name := meta_element.get('pattern', "*"))):
        metadata = process_metadata(metadata, meta_element)
        collector.append(metadata)
        process_links(link_collector, meta_element)
        log(meta_element, meta_path, file_path, file_name, meta_name, metadata, link_collector)

def parse_metadata(xml_file: Path, path: str, filename: str) -> Tuple[List[LinkInfo], Mapping[str, List[str]]]:
    tree = ET.parse(xml_file)
    root = tree.getroot()
    collector = []
    link_collector = []
    path = normalize_path(path)
    collect_dir(collector, link_collector, path, filename, "/", {}, root)
    return (link_collector, transform_dict_values(ChainMap(*reversed(collector)),
                            {"mfterms:prefix": "", "mfterms:meta-manager": "+",
                                    "dc:description": "\n"},
                           {"dc:creator": ","}))

if __name__ == "__main__":
    links,result = parse_metadata("files.xml", "/doc/", "readme2.txt")
    print(result)
    print(links)