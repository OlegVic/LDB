from sqlalchemy import (
    Column, Integer, String, ForeignKey, UniqueConstraint, Index, Text, Float
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import TSVECTOR

Base = declarative_base()

class ClassClarify(Base):
    __tablename__ = 'classes_clarify'
    id = Column(Integer, primary_key=True)
    class_rusname = Column(String(1000), unique=True, nullable=False)
    group_name = Column(String(1000))
    purpose = Column(String(2000))

class CharacteristicClarify(Base):
    __tablename__ = 'characteristics_clarify'
    id = Column(Integer, primary_key=True)
    characteristic = Column(String(255), unique=True, nullable=False)
    characteristic_good = Column(String(255), nullable=True)
    priority = Column(Integer)

class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True)
    article = Column(String(64), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    class_id = Column(Integer, ForeignKey('classes_clarify.id'), index=True)
    search_vector = Column(TSVECTOR)
    total_stock = Column(Integer, default=0)  # Total stock across all warehouses minus reserve

    characteristics = relationship('ProductCharacteristic', back_populates='product')
    class_info = relationship('ClassClarify', foreign_keys=[class_id])
    analogs = relationship('ProductAnalog', back_populates='product')
    barcodes = relationship('ProductBarcode', back_populates='product')
    certificates = relationship('ProductCertificate', back_populates='product')
    instructions = relationship('ProductInstruction', back_populates='product')
    photos = relationship('ProductPhoto', back_populates='product')
    prices = relationship('ProductPrice', back_populates='product')

    __table_args__ = (
        Index('idx_product_name', name),
        Index('idx_product_search_vector', search_vector, postgresql_using='gin'),
    )

class ProductCharacteristic(Base):
    __tablename__ = 'product_characteristics'
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id', ondelete="CASCADE"), nullable=False)
    characteristic_id = Column(Integer, ForeignKey('characteristics_clarify.id', ondelete="CASCADE"), nullable=False)
    value = Column(String(255))
    extra_value = Column(String(255))

    product = relationship('Product', back_populates='characteristics')
    __table_args__ = (
        UniqueConstraint('product_id', 'characteristic_id', name='uniq_product_char'),
        Index('idx_product_characteristics_product_id', 'product_id'),
        Index('idx_product_characteristics_characteristic_id', 'characteristic_id'),
        Index('idx_product_characteristics_value', 'value'),
        Index('idx_product_characteristics_extra_value', 'extra_value'),
    )

class ProductAnalog(Base):
    __tablename__ = 'product_analogs'
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id', ondelete="CASCADE"), nullable=False)
    article = Column(String(64), nullable=False)

    product = relationship('Product', back_populates='analogs')
    __table_args__ = (
        UniqueConstraint('product_id', 'article', name='uniq_product_analog'),
        Index('idx_product_analogs_product_id', 'product_id'),
        Index('idx_product_analogs_article', 'article'),
    )

class ProductBarcode(Base):
    __tablename__ = 'product_barcodes'
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id', ondelete="CASCADE"), nullable=False)
    barcode = Column(String(255), nullable=False)

    product = relationship('Product', back_populates='barcodes')
    __table_args__ = (
        UniqueConstraint('product_id', 'barcode', name='uniq_product_barcode'),
        Index('idx_product_barcodes_product_id', 'product_id'),
        Index('idx_product_barcodes_barcode', 'barcode'),
    )

class ProductCertificate(Base):
    __tablename__ = 'product_certificates'
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id', ondelete="CASCADE"), nullable=False)
    certificate_link = Column(String(1024), nullable=False)

    product = relationship('Product', back_populates='certificates')
    __table_args__ = (
        UniqueConstraint('product_id', 'certificate_link', name='uniq_product_certificate'),
        Index('idx_product_certificates_product_id', 'product_id'),
    )

class ProductInstruction(Base):
    __tablename__ = 'product_instructions'
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id', ondelete="CASCADE"), nullable=False)
    instruction_link = Column(String(1024), nullable=False)

    product = relationship('Product', back_populates='instructions')
    __table_args__ = (
        UniqueConstraint('product_id', 'instruction_link', name='uniq_product_instruction'),
        Index('idx_product_instructions_product_id', 'product_id'),
    )

class ProductPhoto(Base):
    __tablename__ = 'product_photos'
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id', ondelete="CASCADE"), nullable=False)
    photo_link = Column(String(1024), nullable=False)

    product = relationship('Product', back_populates='photos')
    __table_args__ = (
        UniqueConstraint('product_id', 'photo_link', name='uniq_product_photo'),
        Index('idx_product_photos_product_id', 'product_id'),
    )

class ProductPrice(Base):
    __tablename__ = 'product_prices'
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id', ondelete="CASCADE"), nullable=False)
    price_type = Column(String(255), nullable=False)
    price = Column(Float, nullable=False)

    product = relationship('Product', back_populates='prices')
    __table_args__ = (
        UniqueConstraint('product_id', 'price_type', name='uniq_product_price'),
        Index('idx_product_prices_product_id', 'product_id'),
        Index('idx_product_prices_price_type', 'price_type'),
    )
