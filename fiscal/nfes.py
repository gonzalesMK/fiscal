from datetime import datetime
from enum import Enum
import typer
import pandas as pd

from fiscal.db import Companies, Company_Naming, Database, NFEs


class Columns(str, Enum):
    CNPJ = "CNPJ Emitente"
    DATA = "Data Emissão"
    VALOR_TOTAL = "Valor Total da Nota"
    VALOR_LIQUIDO = "Valor Total Produtos"


def update_nfes(
    path="resources/relatorio_avancado_nfe_28-03-2023_22-36-56.csv",
) -> None:
    d_f = pd.read_csv(path)
    db = Database.from_default()

    d_f[Columns.CNPJ] = d_f[Columns.CNPJ].astype(str).str.pad(14, fillchar="0")
    d_f[Columns.DATA] = pd.to_datetime(d_f[Columns.DATA], format="%d/%m/%Y")
    d_f[Columns.VALOR_TOTAL] = (
        d_f[Columns.VALOR_TOTAL]
        .str.replace("R$", "", regex=False)
        .str.strip()
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    d_f[Columns.VALOR_LIQUIDO] = (
        d_f[Columns.VALOR_LIQUIDO]
        .str.replace("R$", "", regex=False)
        .str.strip()
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    with db:
        companies = db.get_companies()
        by_name = {company.name: company.cnpj for company in companies}
        by_cnpj = {company.cnpj: company.name for company in companies}
        codigos = {nfe.codigo_acesso for nfe in db.get_nfes()}

        for _, row in d_f.iterrows():
            codigo_acesso = str(row["Chave de Acesso"])
            cnpj = str(row["CNPJ Emitente"])
            name = str(row["Nome PJ Emitente"])
            date: datetime = row[Columns.DATA]

            if codigo_acesso in codigos:
                continue

            if not cnpj in by_cnpj:
                if name in by_name:
                    match_cnpj = by_name[name]
                    print(f"{name} has same cnpj as {cnpj} but like {match_cnpj}")
                    raise ValueError(
                        f"{name} has same cnpj as {cnpj} but like {match_cnpj}"
                    )
                # category = input(f"Category for {name} and {cnpj}: ")
                db.add(Companies(name=name, cnpj=cnpj, default_category=""))
                db.add(Company_Naming(nickname=name, name=name))
                by_cnpj[cnpj] = name
                by_name[name] = cnpj

            db.add(
                NFEs(
                    codigo_acesso=codigo_acesso,
                    dt_emissao=date,
                    valor_total=str(row[Columns.VALOR_TOTAL]),
                    valor_liquido=str(row[Columns.VALOR_LIQUIDO]),
                    emissor=name,
                )
            )


if __name__ == "__main__":
    typer.run(update_nfes)
