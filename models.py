from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, create_engine
from sqlalchemy.orm import declarative_base, relationship
import dotenv
import os

dotenv.load_dotenv()
connect_str = os.getenv("CONNECT_STR")

Base = declarative_base()


class MemberData(Base):
    __tablename__ = 'MemberData'

    # Define columns
    ShopMemberId = Column(String, primary_key=True)
    RegisterSourceTypeDef = Column(String)
    RegisterDateTime = Column(DateTime)
    Gender = Column(String)
    Birthday = Column(DateTime)
    IsAppInstalled = Column(Boolean)
    IsEnableEmail = Column(Boolean)
    IsEnablePushNotification = Column(Boolean)
    IsEnableShortMessage = Column(Boolean)
    MemberCardLevel = Column(Integer)
    CountryAliasCode = Column(String)


class BehaviorData(Base):
    __tablename__ = 'BehaviorData'

    # Define columns
    id = Column(Integer, primary_key=True)
    Tunnel = Column(String)
    Device = Column(String)
    FullvisitorId = Column(String)
    DeviceId = Column(String)
    ShopMemberId = Column(String, ForeignKey('MemberData.ShopMemberId'))  # Foreign key relationship
    HitTime = Column(DateTime)
    Language = Column(String)
    CountryAliasCode = Column(String)
    Version = Column(String)
    UTMSource = Column(String)
    UTMMedium = Column(String)
    UTMName = Column(String)
    Behavior = Column(String)
    RegisterTunnel = Column(String)
    CategoryId = Column(String)
    SalePageId = Column(String)
    UnitPrice = Column(Float)
    Qty = Column(Float)
    TotalSalesAmount = Column(Float)
    CurrencyCode = Column(String)
    TradesGroupCode = Column(String)
    SearchTerm = Column(String)
    ContentType = Column(String)
    ContentName = Column(String)
    ContentId = Column(String)
    PageType = Column(String)
    EventTime = Column(DateTime)

    # Define the relationship to MemberData
    MemberData = relationship('MemberData', foreign_keys='BehaviorData.ShopMemberId')


# create tables
engine = create_engine(connect_str)
Base.metadata.create_all(engine)
