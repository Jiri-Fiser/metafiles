from pathlib import Path
from typing import Iterator, Tuple, Union

from sqlalchemy.orm import Session

from filehash import hash_file
from metaparser import parse_metadata, normalize_path
import logging
from ark import ArkIdentifier
from fntrans import bcode, bdecode
from database import FileRecord, initialize_database, ConflictError, LogRecord, LogLevel


def list_files(base_dir: Union[str, Path]) -> Iterator[Tuple[str, str]]:
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
            rel_dir_path = path.parent.relative_to(base_path)
            rel_dir = '' if rel_dir_path == Path('.') else str(rel_dir_path)
            yield rel_dir, path.name


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

    for rel_dir, file_name in list_files(data_path):
        links, meta = parse_metadata(metafile, "/" + rel_dir, file_name)
        shoulder = meta["mfterms:prefix"]
        local = bcode(file_name)
        ark = ArkIdentifier(naan, shoulder, local)
        hash = hash_file(data_path / rel_dir / file_name, "sha256")
        r = FileRecord(ark_base_name=ark.locid, local_path=normalize_path(rel_dir + "/" + file_name),
                       digest=hash, metadata_data=meta, linkdata={"files": [link.to_dict() for link in links]})
        try:
            FileRecord.insert_if_new_or_identical(session, r)
        except ConflictError as e:
            resolveConflict(e, session)


if __name__ == "__main__":
    ki_naan = "77298"
    logging.basicConfig(filename='test.log', filemode="w", level=logging.INFO)
    update(ki_naan, Path("test"), Path("test")/"metafile.xml",
           "sqlite:///databases/metafiles.db")
