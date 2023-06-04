from datetime import datetime
from enum import Enum

import pandas as pd
import typer

from fiscal.db import Companies, Company_Naming, Database, NFEs


class Columns(str, Enum):
    CNPJ = "CNPJ Emitente"
    DATA = "Data Emissão"
    VALOR_TOTAL = "Valor Total da Nota"
    VALOR_LIQUIDO = "Valor Total Produtos"


def update_nfes(
    path="resources/relatorio_avancado_nfe_maio.csv",
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
        company_by_name = {company.name: company for company in db.get_companies()}
        company_by_cnpj = {
            company.cnpj: company for company in company_by_name.values()
        }
        company_by_nickname = {
            company.nickname: company_by_name[company.name]
            for company in db.get_company_names()
        }
        codigos = {nfe.codigo_acesso for nfe in db.get_nfes()}

        for _, row in d_f.iterrows():
            codigo_acesso = str(row["Chave de Acesso"])
            cnpj = str(row["CNPJ Emitente"])
            name = str(row["Nome PJ Emitente"]).lower()
            date: datetime = row[Columns.DATA]

            if codigo_acesso in codigos:
                continue

            print(f"{name} - {cnpj}")
            if name in company_by_nickname:
                if cnpj not in company_by_cnpj:
                    match_cnpj = company_by_nickname[name]
                    print(f"{name} has same cnpj as {cnpj} but not like {match_cnpj}")
                    raise ValueError(
                        f"{name} has same cnpj as {cnpj} but like {match_cnpj}"
                    )
            elif cnpj in company_by_cnpj:
                print("!!!!!")
                db.add(Company_Naming(nickname=name, name=company_by_cnpj[cnpj].name))
                company_by_nickname[name] = company_by_cnpj[cnpj]
            else:
                print("??????????")
                company = db.add(Companies(name=name, cnpj=cnpj, default_category=""))
                db.add(Company_Naming(nickname=name, name=name))
                company_by_cnpj[cnpj] = company
                company_by_nickname[name] = company

            db.add(
                NFEs(
                    codigo_acesso=codigo_acesso,
                    dt_emissao=date,
                    valor_total=str(row[Columns.VALOR_TOTAL]),
                    valor_liquido=str(row[Columns.VALOR_LIQUIDO]),
                    emissor=name,
                )
            )

            db.commit()


if __name__ == "__main__":
    typer.run(update_nfes)
