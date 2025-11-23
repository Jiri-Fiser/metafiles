import datetime
from pathlib import Path
from typing import Iterator, Union, ChainMap

from data_policy import ConflictAction, parse_policy, get_localname, NameStrategy
from filehash import hash_file
from metaparser import parse_metadata
import logging
from ark import ArkIdentifier
from database import FileRecord

import re
from typing import Dict
from configparser import ConfigParser


def substitute_placeholders(template: str, substitutions: Dict[str, str]) -> str:
    """
    Replaces occurrences of {key} in the input string with values from the substitutions dictionary.
    Double braces {{ and }} are treated as literal curly braces.
    Raises KeyError if any key in the template is not found in the substitutions dictionary.

    Args:
        template: The input string containing placeholders (e.g., "Hello, {name}!")
        substitutions: A dictionary mapping keys to replacement values (e.g., {"name": "John"})

    Returns:
        A string with placeholders replaced and literal braces preserved.

    Raises:
        KeyError: If a placeholder key is not found in the substitutions dictionary.
    """
    # Temporarily replace double braces with placeholders
    temp_template = template.replace('{{%', '\x01').replace('%}}', '\x02')

    pattern = re.compile(r'\{%([^{}]+)%}')

    def replace_match(match: re.Match) -> str:
        key = match.group(1)
        if key not in substitutions:
            raise KeyError(f"Key '{key}' not found in substitutions.")
        return substitutions[key]

    substituted = pattern.sub(replace_match, temp_template)

    # Restore literal braces
    return substituted.replace('\x01', '{%').replace('\x02', '%}')


def list_files(base_dir: Union[str, Path]) -> Iterator[Path]:
    """
    Recursively finds all files in the given base directory and its subdirectories.

    Returns an iterator over pairs (relative_dir, file_name), where:
      - relative_dir is a string representing the path to the directory
        relative to `base_dir` (excluding the file name),
      - file_name is the name of the file.

    If a file is located directly in `base_dir`, relative_dir is an empty string.

    :param base_dir: The base directory to search, as a string or Path object.
    :return: An iterator over (relative directory, file name) tuples.
    """
    base_path = Path(base_dir).resolve()
    for path in base_path.rglob('*'):
        if path.is_file():
            yield path


def update(naan:str, data_path:Path, metafile:Path, database_uri: str):
    session = FileRecord.initialize_database(database_uri)()

    for path in list_files(data_path):
        if path.name == "metafile.xml":
            continue
        links, meta = parse_metadata(metafile, path, data_path)
        local_path = str(path.relative_to(data_path))
        print(local_path)
        #print(meta)
        #print(links)
        shoulder = meta["mfterms:prefix"]
        policy = parse_policy(meta["mfterms:data-policy"][0])
        name_strategy= policy.get("local_name_strategy", NameStrategy.FILENAME_HASH_12)
        local = get_localname(path, data_path, name_strategy)
        ark = ArkIdentifier(naan, shoulder[0], local)
        print(ark)
        hash = hash_file(path, "sha256")
        substitutions = {"hash": hash.hex(), "ark": str(ark)}
        # substitution
        meta = {key: [substitute_placeholders(s, substitutions) for s in string_list]
            for key, string_list in meta.items()}
        # insertion/update
        r = FileRecord(ark_base_name=repr(ark), local_path=local_path,
                       digest=hash, meta=meta, links={"files": [link.to_dict() for link in links]},
                       created=datetime.datetime.now(), updated=datetime.datetime.now())

        strictness_policy = dict(ChainMap({"created": ConflictAction.IGNORE, "updated": ConflictAction.UPDATE},
                          policy["strictness"]))
        with session.begin():
            r.insert(session, policy=strictness_policy)


if __name__ == "__main__":
    config = ConfigParser()
    config.read("config.ini")
    ki_naan = config["Storage"]["Naan"]
    location = config[config["Storage"]["Location"]]
    path = Path(location["Path"])
    metafiles_db = location["Database"]
    log = location["Log"]
    logging.basicConfig(filename=log, filemode="w", level=logging.INFO)
    update(ki_naan, path, path/"metafile.xml",metafiles_db)
