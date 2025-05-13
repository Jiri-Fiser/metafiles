from pathlib import Path
from typing import Iterator, Tuple, Union

from sqlalchemy.orm import Session

from filehash import hash_file
from metaparser import parse_metadata
import logging
from ark import ArkIdentifier
from fntrans import bcode, bdecode
from database import FileRecord, initialize_database, ConflictError, LogRecord, LogLevel

import re
from typing import Dict

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
    temp_template = template.replace('{{', '\x01').replace('}}', '\x02')

    pattern = re.compile(r'\{([^{}]+)\}')

    def replace_match(match: re.Match) -> str:
        key = match.group(1)
        if key not in substitutions:
            raise KeyError(f"Key '{key}' not found in substitutions.")
        return substitutions[key]

    substituted = pattern.sub(replace_match, temp_template)

    # Restore literal braces
    return substituted.replace('\x01', '{').replace('\x02', '}')


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


def resolveConflict(e: ConflictError, session: Session, strict:bool = False):
    with session.begin():
        logging.info(f"updating conflict. old:{e.existing_record} new:{e.new_record}")
        level = LogLevel.Error if strict else LogLevel.Warning
        if e.existing_record.digest != e.new_record.digest:
            LogRecord.write_log(session, level, "failed attempt to change file" if strict else "file changed",
                                e.existing_record.local_path,
                                old_value=e.existing_record.digest.hex().upper(),
                                new_value=e.new_record.digest.hex().upper())
        if e.existing_record.metadata_data != e.new_record.metadata_data:
            LogRecord.write_log(session, level, "failed attempt to change metadata" if strict else "metadata changed",
                                e.existing_record.local_path,
                                old_value=str(e.existing_record.metadata_data),
                                new_value=str(e.new_record.metadata_data))
        if e.existing_record.linkdata != e.new_record.linkdata:
            LogRecord.write_log(session, level, "failed attempt to change link metadata" if strict else "link metadata changed",
                                e.existing_record.local_path,
                                old_value=str(e.existing_record.linkdata),
                                new_value=str(e.new_record.linkdata))
        if not strict:
            FileRecord.update(session, e.new_record)

def update(naan:str, data_path:Path, metafile:Path, database_uri: str):
    session = initialize_database(database_uri)

    for path in list_files(data_path):
        print(path)
        links, meta = parse_metadata(metafile, path, data_path)
        print(meta)
        print(links)
        shoulder = meta["mfterms:prefix"]
        local = bcode(path.name)
        ark = ArkIdentifier(naan, shoulder[0], local)
        hash = hash_file(path, "sha256")
        substitutions = {"hash": hash.hex(), "ark": str(ark)}
        meta = {key: [substitute_placeholders(s, substitutions) for s in string_list]
            for key, string_list in meta.items()}
        local_path = str(path.relative_to(data_path))
        print(local_path)
        print()
        r = FileRecord(ark_base_name=repr(ark), local_path=local_path,
                       digest=hash, metadata_data=meta, linkdata={"files": [link.to_dict() for link in links]})
        try:
            FileRecord.insert_if_new_or_identical(session, r)
        except ConflictError as e:
            resolveConflict(e, session)


if __name__ == "__main__":
    ki_naan = "77298"
    logging.basicConfig(filename='test.log', filemode="w", level=logging.INFO)
    update(ki_naan, Path("/home/jfiser/metafiles/test"), Path("/home/jfiser/metafiles/test")/"metafile.xml",
           "sqlite:///databases/metafiles.db")
