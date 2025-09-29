from database import Base
from sqlalchemy import Column, String, Text, DateTime
from sqlalchemy.dialects.postgresql import UUID

class Chunk(Base):
    __tablename__ = 'chunks'

    id = Column(String(36), primary_key=True)
    # id = Column(UUID(as_uuid=True), primary_key=True)
    content = Column(Text, nullable=False)
    scraped_at = Column(DateTime(timezone=True), nullable=False)

    def __repr__(self):
        return f"<Chunk id:'{self.id}' scraped_at:'{self.scraped_at}')>"