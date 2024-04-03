from datetime import date, datetime
from decimal import Decimal
from typing import List
from pydantic import BaseModel, ConfigDict, TypeAdapter, parse_obj_as

from sqlalchemy import ForeignKey, create_engine, func, select
from sqlalchemy.orm import (
    DeclarativeBase,
    backref,
    Mapped,
    lazyload,
    mapped_column,
    relationship,
    sessionmaker,
)

class Base(DeclarativeBase):
    pass


class CategoryDB(Base):
    __tablename__ = 'category'

    category_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]


class ProductDB(Base):
    __tablename__ = 'product'

    product_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    price: Mapped[Decimal]
    category_id: Mapped[int] = mapped_column(
        ForeignKey('category.category_id'),
    )
    category: Mapped['CategoryDB'] = relationship(
        foreign_keys=[category_id],
        backref='ProductDB',
        cascade='all,delete',
        uselist=False,
        lazy='joined',
    )
    inventory = relationship(
        'InventoryDB',
        backref=backref('ProductDB', uselist=True),
        cascade='all,delete',
        foreign_keys=[product_id],
        primaryjoin='ProductDB.product_id == InventoryDB.product_id',
    )


class InventoryDB(Base):
    __tablename__ = 'inventory'

    inventory_id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey('product.product_id'))
    quantity: Mapped[int]


class ProductInDB(BaseModel):
    name: str
    price: Decimal
    category_id: int
    available_quantity: int

    model_config = ConfigDict(from_attributes=True)


def get_session():
    sqlalchemy_database_url = 'postgresql://postgres_user:pass123@localhost/dummy'

    engine = create_engine(
        sqlalchemy_database_url,
    )
    return sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )


if __name__ == "__main__":

    quantity_query = (
        select(
            ProductDB,
            ProductDB.name,
            ProductDB.price,
            func.coalesce(func.sum(InventoryDB.quantity), 0).label(
                'available_quantity',
            ),
        )
        .options(lazyload('*'))
        .outerjoin(
            InventoryDB,
            ProductDB.product_id == InventoryDB.product_id,
        )
        .group_by(ProductDB.product_id)
    )
    db = get_session()
    with db() as session:
        products_db = session.scalars(quantity_query)
        import ipdb; ipdb.set_trace()
        adapter = TypeAdapter(List[ProductInDB])

        pydantic_object =  adapter.validate_python(products_db.all())
        print(pydantic_object)
