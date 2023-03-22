from datetime import datetime
from typing import Any
from pydantic import BaseModel
from sqlalchemy import text
import typer

from fiscal.db import Database, Validations

# TODO - Include matching salarios given the accounting email or some other form (or manual input )
# TODO - Same for agua e luz
# TODO - or instead just ask if the value is ok
# TODO - Pagamentos recorrentes

# Match NFE with Transaction on same Company and Same value
BEST_MATCH = """
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
DATE_FORMAT = "%Y-%m-%d"


class BestMatch(BaseModel):
    codigo_acesso: str
    id: int
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


def row_to_model(row: list[Any]):
    return BestMatch(
        **{field: value for field, value in zip(BestMatch.__fields__, row)}
    )


def match():
    db = Database.from_default()

    save = "1"
    while save:
        with db.start():
            if not db._session:
                return

            results = list(
                map(row_to_model, db._session.execute(text(BEST_MATCH)).all())
            )
            if not results:
                print("No results. Done.")
                break

            last_emissor = results[0].name
            for id, row in enumerate(results):
                if row.name != last_emissor:
                    print("----------")
                    last_emissor = row.name
                print(f"{str(id).rjust(2, ' ')} - {row.format()}")

            save = input(f"Would you like to accept a match (0 to {len(results)}): ")

            if save:
                transacao = results[int(save)].id
                codigo_acesso = results[int(save)].codigo_acesso

                db.add(Validations(transacao=transacao, codigo_acesso=codigo_acesso))
                db._session.execute(text(UPDATE_NFE.format(codigo_acesso)))
                db._session.execute(text(UPDATE_TRANSACTION.format(transacao)))


if __name__ == "__main__":
    typer.run(match)
