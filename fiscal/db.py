from datetime import datetime, date
from enum import Enum
import os
from typing import TypeVar

from pandas.core.common import contextlib
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlmodel import Field, SQLModel, Session, create_engine, select

DB_PATH = "/home/julianonegri/Documents/github/fiscal/fiscal.db"

DATE_FORMAT = "%Y-%m-%d"


class EntryType(str, Enum):
    ENTRADA = "ENTRADA"
    SAIDA = "SAIDA"


class Category(str, Enum):
    BANCOS = "Bancos"
    CONTADOR = "Contador"
    COMPRAS = "Compras"
    ENTRADA = "Entrada"
    FRETE = "Frete"
    INSUMOS = "Insumos"
    IGNORAR = "Ignorar"
    IMPOSTO = "Imposto"
    MARKETING = "Marketing"
    SALÁRIOS = "Salários"
    SEGURANÇA = "Segurança"
    SERVIÇOS_3 = "Serviços 3º"
    SISTEMAS = "Sistemas"


class Categories(SQLModel, table=True):
    category: str = Field(default=None, primary_key=True)

    class Config:
        anystr_lower = True


class Transactions(SQLModel, table=True):
    """Transactions"""

    id: int | None = Field(default=None, primary_key=True)
    bank: str
    date: datetime
    entry_type: EntryType
    transaction_type: str
    category: str | None
    description: str
    value: str
    counterpart_name: str | None
    validated: bool
    external_id: str

    class Config:
        anystr_lower = True


class Companies(SQLModel, table=True):
    """Companies"""

    name: str = Field(default=None, primary_key=True)
    cnpj: str
    default_category: str | None = Field(default=None)

    class Config:
        anystr_lower = True


class Company_Naming(SQLModel, table=True):
    nickname: str = Field(default=None, primary_key=True)
    name: str

    class Config:
        anystr_lower = True


class Banks(SQLModel, table=True):
    bank: str = Field(default=None, primary_key=True)
    description: str

    class Config:
        anystr_lower = True


Model = TypeVar("Model", bound=SQLModel)


class NFEs(SQLModel, table=True):
    codigo_acesso: str = Field(default=None, primary_key=True)
    emissor: str
    dt_emissao: datetime
    valor_liquido: str
    valor_total: str
    validated: bool = Field(default=False)

    class Config:
        anystr_lower = True


class Validations(SQLModel, table=True):
    transacao: int = Field(default=None, primary_key=True, foreign_key=Transactions.id)
    codigo_acesso: str = Field(
        default=None, primary_key=True, foreign_key=NFEs.codigo_acesso
    )

    class Config:
        anystr_lower = True


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, _):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class Database:
    _session: Session | None = None
    steps = 0

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @classmethod
    def from_default(cls):
        path = os.environ.get("DB_PATH", None)
        engine = create_engine(f"sqlite:///{path or DB_PATH}", echo=False)
        return cls(engine=engine)

    def __enter__(self) -> Session:
        if self._session is None:
            self._session = Session(self.engine)
            self._session.begin()
            self._session.__enter__()
        self.steps += 1

        return self._session

    def __exit__(self, *_):
        self.steps -= 1
        if self._session and self.steps == 0:
            self._session.commit()
            self._session = None

    def delete(self, model: SQLModel) -> None:
        if self._session is None:
            raise ValueError("Not within a session")
        self._session.delete(model)

    def add(self, model: SQLModel) -> None:
        with self as session:
            session.add(model)

    def _get_all(self, model: type[Model]) -> list[Model]:
        with self as session:
            statement = select(model)
            return session.exec(statement=statement).all()

    def get_company_names(self) -> list[Company_Naming]:
        return self._get_all(Company_Naming)

    def get_nfes(self) -> list[NFEs]:
        return self._get_all(NFEs)

    def get_companies(self) -> list[Companies]:
        return self._get_all(Companies)

    def get_transactions(self, bank: str):
        with self as session:
            statement = select(Transactions).where(Transactions.bank.ilike(bank))
            return session.exec(statement=statement).all()

    def get_validation_by_id(self, transacao: int, codigo_acesso: str):
        if self._session is None:
            raise ValueError("Not within a session")

        statement = select(Validations).where(
            Validations.codigo_acesso.ilike(codigo_acesso),
            Validations.transacao == transacao,
        )
        return self._session.exec(statement=statement).first()

    def get_latest_transaction(self, bank: str) -> datetime | None:
        result = self.execute(
            f"SELECT max(TRA.date) FROM transactions as TRA WHERE TRA.bank == '{bank}'"
        ).scalar()

        if result:
            return datetime.strptime(result[:10], DATE_FORMAT)

        return None

    def execute(self, statement: str):
        if self._session is None:
            raise ValueError("Not within a session")
        return self._session.execute(statement=text(statement))
