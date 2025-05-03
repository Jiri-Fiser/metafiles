from urllib.parse import urlunparse, urlencode

from sqlalchemy import create_engine, Column, Text
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from database import FileRecord, get_session
from rdftools import meta_to_rdf

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

def update_cache(cache_session, metafile_session):
    for record in metafile_session.query(FileRecord).all():
        rdf = meta_to_rdf(record.metadata_data, record.linkdata, record.ark_base_name)
        rdf_xml = rdf.serialize(format="pretty-xml")
        print(rdf_xml)
        query = urlencode({"path" : record.local_path})
        url = urlunparse(("https", "example.com", "sourcer", "", query, ""))
        cache_row = FileCache(ark_id=record.ark_base_name, url=url, metadata_rdf=rdf_xml)
        cache_session.add(cache_row)
        cache_session.commit()


# Příklad použití:
if __name__ == "__main__":
    update_cache(FileCache.init_db("sqlite:///databases/filecache.db"),
                 get_session("sqlite:///databases/metafiles.db"))
