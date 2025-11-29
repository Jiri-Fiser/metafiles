from pathlib import Path
from re import match
from typing import Dict
from urllib.parse import urlunparse, urlencode

from sqlalchemy import create_engine, Column, Text
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from database import FileRecord, get_session
from rdftools import meta_to_rdf
from sqlite3_exporter import sqlite_table_to_json, sqlite_url_to_path

from configparser import ConfigParser

# Vytvoření základní třídy pro ORM modely
Base = declarative_base()


class FileCache(Base):
    __tablename__ = 'file_cache'

    # Definice sloupců
    ark_id = Column(Text, primary_key=True)
    url = Column(Text)
    metadata_rdf = Column(Text)

    @classmethod
    def init_db(cls, db_url: str) -> Session:
        """
        Inicializuje databázi podle zadaného URL, vytvoří tabulky a vrátí session objekt.

        :param db_url: Řetězec s adresou databáze, např. 'sqlite:///filecache.db'
        :return: Objekt session pro práci s databází
        """
        # Vytvoření enginu s SQLAlchemy 2.0 API (future=True)
        engine = create_engine(db_url, echo=False, future=True)

        FileCache.__table__.drop(engine, checkfirst=True)
        # Vytvoření všech tabulek definovaných v Base (včetně tabulky file_cache)
        Base.metadata.create_all(engine)

        # Vytvoření session makeru s vypnutým automatickým commitováním změn
        SessionLocal = sessionmaker(engine, expire_on_commit=False, future=True)

        # Vrácení nové session
        return SessionLocal()

def substitute_links(linkdata: Dict, root: Path, ark_dict: Dict[str, str], local_path: str):
    new_records = []
    for record in linkdata["files"]:
        link_pattern = record["path"]
        print(link_pattern, local_path, root)
        path = Path(local_path).parent / link_pattern
        for file in root.glob(str(path)):
            link_path = file.resolve().relative_to(root)
            new_record = dict(record)
            new_record["ark"] = ark_dict[str(link_path)]
            new_record["filename"] = str(link_path)
            new_records.append(new_record)
            for term, values in record["metadata"].items():
                for value in values:
                    if m:= match(r"#path\((.*)\)\s*", value):
                        pass
                        #print(m.group(1))

    return {"files:": new_records}


def substitute_paths(record: FileRecord, root: Path, ark_dict: Dict[str, str]):
    new_linkdata = substitute_links(record.links, root, ark_dict, record.local_path)
    record.linkdata = new_linkdata



def update_cache(cache_session, metafile_session, location):
    ark_dict = {record.local_path: record.ark_base_name
                for record in metafile_session.query(FileRecord).all()}

    path = Path(location["Path"])
    for record in metafile_session.query(FileRecord).all():
        substitute_paths(record, path, ark_dict)
        rdf = meta_to_rdf(record.meta, record.linkdata, record.ark_base_name)
        rdf_xml = rdf.serialize(format="pretty-xml")
        #print(record.linkdata)
        #print(rdf_xml)
        query = urlencode({"path" : record.local_path})
        url_protocol = location["Url_protocol"]
        url_authority = location["Url_authority"]
        url_path = location["Url_path"]
        url = urlunparse((url_protocol, url_authority, url_path, "", query, ""))
        cache_row = FileCache(ark_id=record.ark_base_name, url=url, metadata_rdf=rdf_xml)
        cache_session.add(cache_row)
        cache_session.commit()


if __name__ == "__main__":
    config = ConfigParser()
    config.read("config.ini")
    location = config[config["Storage"]["Location"]]
    metafiles_db = location["Database"]
    cache_db = location["Cache"]
    update_cache(FileCache.init_db(cache_db), get_session(metafiles_db), location)
    contents_file = location["Contents"]
    sqlite_table_to_json(db_path=sqlite_url_to_path(cache_db),
                         table_name="file_cache",
                         output_path=contents_file)
