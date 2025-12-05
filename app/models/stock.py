from sqlalchemy import Column, String, Float, Date, DateTime, Integer, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db.base_class import Base

class Stock(Base):
    code = Column(String, primary_key=True, index=True)
    symbol = Column(String, unique=True, index=True, nullable=False) # e.g., sh600000, usAAPL
    name = Column(String, index=True)
    market = Column(String, index=True) # CN, US, HK
    type = Column(String) # stock, etf, etc.
    update_time = Column(DateTime, default=datetime.utcnow)

    prices = relationship("PriceHistory", back_populates="stock")

class PriceHistory(Base):
    id = Column(Integer, primary_key=True, index=True)
    stock_code = Column(String, ForeignKey("stock.code"), index=True)
    date = Column(Date, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

    stock = relationship("Stock", back_populates="prices")

    __table_args__ = (
        Index('idx_stock_date', 'stock_code', 'date', unique=True),
    )
