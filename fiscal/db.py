from datetime import datetime
from typing import TypeVar
from pandas.core.common import contextlib
from sqlalchemy.engine import Engine
from sqlmodel import Field, SQLModel, Session, create_engine, select


DB_PATH = "/home/julianonegri/Documents/github/fiscal/fiscal.db"


class Transactions(SQLModel, table=True):
    """Transactions"""

    id: int | None = Field(default=None, primary_key=True)
    bank: str
    date: datetime
    entry_type: str
    transaction_type: str
    category: str | None
    description: str
    value: str
    counterpart_name: str
    validated: bool


class Companies(SQLModel, table=True):
    """Companies"""

    name: str = Field(default=None, primary_key=True)
    cnpj: str
    default_category: str | None = Field(default=None)


class Company_Naming(SQLModel, table=True):
    nickname: str = Field(default=None, primary_key=True)
    name: str


class Banks(SQLModel, table=True):
    bank: str = Field(default=None, primary_key=True)
    description: str


Model = TypeVar("Model", bound=SQLModel)


class NFEs(SQLModel, table=True):
    codigo_acesso: str = Field(default=None, primary_key=True)
    emissor: str
    dt_emissao: datetime
    valor_liquido: str
    valor_total: str
    validated: bool = Field(default=False)


class Database:
    _session: Session | None = None

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @classmethod
    def from_default(cls):
        engine = create_engine(f"sqlite:///{DB_PATH}", echo=True)
        return cls(engine=engine)

    @contextlib.contextmanager
    def start(self):
        self._session = Session(self.engine)
        self._session.begin()

        try:
            with self._session:
                yield self
                self._session.commit()
        finally:
            self._session = None

    def add(self, model: SQLModel) -> None:
        if self._session is None:
            raise ValueError("Not within a session")
        self._session.add(model)

    def _get_all(self, model: type[Model]) -> list[Model]:
        if self._session is None:
            raise ValueError("Not within a session")

        statement = select(model)
        return self._session.exec(statement=statement).all()

    def get_company_names(self) -> list[Company_Naming]:
        return self._get_all(Company_Naming)

    def get_nfes(self) -> list[NFEs]:
        return self._get_all(NFEs)

    def get_companies(self) -> list[Companies]:
        return self._get_all(Companies)

    def get_transactions(self, bank: str):
        if self._session is None:
            raise ValueError("Not within a session")

        statement = select(Transactions).where(Transactions.bank.ilike(bank))
        return self._session.exec(statement=statement).all()
