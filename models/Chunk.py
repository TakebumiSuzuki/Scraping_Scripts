from sqlalchemy import create_engine, Column, String, Text, DateTime # ★変更点: DateTimeを追加
from sqlalchemy.orm import sessionmaker, declarative_base


Base = declarative_base()

class Chunk(Base):
    """
    データベースの'chunks'テーブルに対応するORMモデル。
    """
    __tablename__ = 'chunks'
    id = Column(String(36), primary_key=True)
    content = Column(Text, nullable=False)
    # ★変更点: StringからDateTime(timezone=True)へ変更
    # timezone=Trueとすることで、DB側でタイムゾーン付きのタイムスタンプとして扱われます。
    scraped_at = Column(DateTime(timezone=True), nullable=False)

    def __repr__(self):
        return f"<Chunk(id='{self.id}', scraped_at='{self.scraped_at}')>"