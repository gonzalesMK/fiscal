from datetime import datetime
from functools import partial
from os import stat
from typing import Any
from typing_extensions import Self
from pydantic import BaseModel
import typer

from fiscal.db import Database, Validations

# TODO - Include matching salarios given the accounting email or some other form (or manual input )
# TODO - Same for agua e luz
# TODO - or instead just ask if the value is ok
# TODO - Pagamentos recorrentes

# Match NFE with Transaction on same Company and Same value

UNDO_TRANSACTION = """
UPDATE transactions
SET validated = 0
WHERE id = {};
"""
UNDO_NFE = """
UPDATE nfes
SET validated = 0
WHERE codigo_acesso = '{}';
"""

UPDATE_NFE = """
UPDATE nfes
SET validated = 1
WHERE codigo_acesso = '{}';
"""

UPDATE_TRANSACTION = """
UPDATE transactions
SET validated = 1
WHERE id = {};
"""


# This is expected for market places

DATE_FORMAT = "%Y-%m-%d"


class BaseMatch(BaseModel):
    codigo_acesso: str
    id: int

    @staticmethod
    def query() -> str:
        return ""

    def format(self) -> None:
        ...

    def split_marker(self) -> str:
        return ""

    def act(self, db: Database):
        transacao = self.id
        codigo_acesso = self.codigo_acesso

        db.add(Validations(transacao=transacao, codigo_acesso=codigo_acesso))
        db.execute(UPDATE_NFE.format(codigo_acesso))
        db.execute(UPDATE_TRANSACTION.format(transacao))


class Undo(BaseMatch):
    emissor: str
    counterpart: str
    dt_emissao: datetime
    dt_transaction: datetime
    value: float

    def format(self):
        return (
            f"Emissão: {self.dt_emissao.strftime(DATE_FORMAT)}\t"
            f"Transação: {self.dt_transaction.strftime(DATE_FORMAT)}\t"
            f"Valor: {self.value}\t"
            f"Emissor: {self.emissor}\t"
            f"Counter: {self.counterpart}\t"
        )

    @staticmethod
    def query():
        return """
        SELECT   NFE.codigo_acesso
                ,TRA.id
                ,NFE.emissor
                ,TRA.counterpart_name
                ,NFE.dt_emissao
                ,TRA.date
                ,TRA.value
        FROM "main"."validations" as VAL
            JOIN "main"."nfes" as NFE  on NFE.codigo_acesso == VAL.codigo_acesso
            JOIN "main"."transactions" as TRA ON TRA.id == VAL.transacao
        ORDER BY VAL.created_at DESC
        LIMIT 10
    """

    def act(self, db: Database):
        transacao = self.id
        codigo_acesso = self.codigo_acesso

        val = db.get_validation_by_id(transacao=transacao, codigo_acesso=codigo_acesso)
        assert val
        db.delete(val)
        db.execute(UNDO_NFE.format(codigo_acesso))
        db.execute(UNDO_TRANSACTION.format(transacao))


class MarketPlace(BaseMatch):
    emissor: str
    counterpart: str
    dt_emissao: datetime
    dt_transaction: datetime
    day_diff: int
    value: float

    def format(self):
        return (
            f"Diff: {str(self.day_diff).rjust(3, ' ')}\t"
            f"Emissão: {self.dt_emissao.strftime(DATE_FORMAT)}\t"
            f"Transação: {self.dt_transaction.strftime(DATE_FORMAT)}\t"
            f"Valor: {self.value}\t"
            f"Emissor: {self.emissor}\t"
            f"Counter: {self.counterpart}\t"
            # f"Codigo: {self.codigo_acesso}\t"
        )

    @staticmethod
    def query():
        return """
SELECT   NFE.codigo_acesso
		,TRA.id
		,NFE.emissor
		, TRA.counterpart_name
		,NFE.dt_emissao as "Data NF"
		,TRA.date as "Data Transação"
		,julianday(NFE.dt_emissao) - julianday(TRA.date) AS days_difference
		,TRA.value

FROM "main"."nfes" as NFE
	JOIN "main"."transactions" as TRA ON NFE.valor_total == TRA.value
WHERE
	NFE.validated == 0 AND TRA.validated == 0 AND (counterpart_name LIKE "Pix Marketplace" or counterpart_name LIKE "Magalu Pagamentos Ltda")
ORDER by NFE.emissor, ABS(days_difference)
"""


class BestMatch(BaseMatch):
    name: str
    dt_emissao: datetime
    dt_transaction: datetime
    day_diff: int
    value: float

    def format(self):
        return (
            f"Diff: {str(self.day_diff).rjust(3, ' ')}\t"
            f"Emissão: {self.dt_emissao.strftime(DATE_FORMAT)}\t"
            f"Transação: {self.dt_transaction.strftime(DATE_FORMAT)}\t"
            f"Valor: {self.value}\t"
            f"Emissor: {self.name}\t"
            f"Codigo: {self.codigo_acesso}\t"
            f"Id: {self.id}"
        )

    def split_marker(self):
        return self.name

    @staticmethod
    def query():
        return """
SELECT   NFE.codigo_acesso
		,TRA.id
		,EMISSOR.name
		,NFE.dt_emissao as "Data NF"
		,TRA.date as "Data Transação"
		,julianday(NFE.dt_emissao) - julianday(TRA.date) AS days_difference
		,TRA.value

FROM "main"."nfes" as NFE
	JOIN "main"."transactions" as TRA ON NFE.valor_total == TRA.value
	JOIN "company_naming" as EMISSOR ON EMISSOR.nickname == NFE.emissor
	JOIN "company_naming" as CPART ON CPART.nickname == TRA.counterpart_name
WHERE
	NFE.validated == 0 AND TRA.validated == 0 AND EMISSOR.name == CPART.name
ORDER by NFE.emissor, ABS(days_difference)
"""


def match():
    db = Database.from_default()

    print("MATCH NFES")
    save = "1"
    while save:
        with db.__start__():
            save = iterate_matching(db, cls=BestMatch)

    print("MATCH MARKETPLACES")
    save = "1"
    while save:
        with db.__start__():
            save = iterate_matching(db, cls=MarketPlace)

    print("MATCH MISSING")

    print("UNDO")
    save = "1"
    while save:
        with db.__start__():
            save = iterate_matching(db, cls=Undo)


def row_to_model(row: list[Any], cls: type[BaseMatch]):
    return cls(**{field: value for field, value in zip(cls.__fields__, row)})


def iterate_matching(db: Database, cls: type[BaseMatch]) -> bool:
    results = list(map(partial(row_to_model, cls=cls), db.execute(cls.query()).all()))

    if not results:
        print("No results. Done.")
        return False

    _print_rows(results)

    save = input(f"Would you like to accept a match (0 to {len(results)}): ")

    if save:
        results[int(save)].act(db=db)
        return True

    return False


def _print_rows(results: list[BaseMatch]):
    marker = results[0].split_marker()
    for id, row in enumerate(results):
        if row.split_marker() != marker:
            print("----------")
            marker = row.split_marker()
        print(f"{str(id).rjust(2, ' ')} - {row.format()}")


if __name__ == "__main__":
    typer.run(match)
