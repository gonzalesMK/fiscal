from datetime import datetime
import uuid
from enum import Enum
from pathlib import Path
from thefuzz import process
import pandas as pd
from sqlmodel import Field, SQLModel, desc
import typer

from fiscal.db import Companies, Company_Naming, Database, Transactions

root = Path(__file__).parent

BB_BANK = "bb"


class Columns(str, Enum):
    CNPJ = "cnpj"
    NAME = "nome"
    DESCRIPTION = "Historico"


class TransactionType(str, Enum):
    BACEN = "BACEN-Res.An.Céd.Encam."
    TARIFA = "Tarifa Pacote de Serviços"
    IMPOSTO = "Impostos"
    PIX_REJEITADO = "Pix - Rejeitado"
    TARIFA_DOC = "Tar DOC/TED Eletrônico"
    DEPOSITO = "Depósito Online"


NO_COUNTERPARTY = [
    TransactionType.BACEN,
    TransactionType.TARIFA,
    TransactionType.IMPOSTO,
    TransactionType.PIX_REJEITADO,
    TransactionType.TARIFA_DOC,
    TransactionType.DEPOSITO,
]


class EntryTipe(str, Enum):
    ENTRADA = "ENTRADA"
    SAIDA = "SAIDA"


# TODO - add naming table and download it on a dictionary
# TODO - add transactions
# TODO - accepts banco inter
# TODO - create matching for same ame company - same price - print and ask if should match
# TODO - create matching for multiple prices on same date for mercadolivre
# TODO - for mercado livre ask same price same date
# TODO - Should not accept files with same name

# Should ask one by one iteractively (Y/N)
# SAME PRICE - SAME DATE
# SUM SAME PRICE - SAME DATE for ML
# SAME PRICE - SAME COMPANY


def _compose_cnpj_in_names(name: str) -> tuple[str, str]:
    if len(name) < 25:
        return "", name

    possible_cnpj = name[9:23]

    try:
        int(possible_cnpj)
    except:
        return "", name

    return possible_cnpj, name[23:]


def _cnpj_in_names(name: str) -> tuple[str, str]:
    if len(name) < 15:
        return "", name

    possible_cnpj = name[:15]

    try:
        int(possible_cnpj)
    except:
        return _compose_cnpj_in_names(name)

    return possible_cnpj, name[15:]


def _get_info(description: str) -> tuple[str, str]:
    if len(description) < 12:
        return "", description

    possible_date = description[:11]
    try:
        datetime.strptime(possible_date, "%d/%m %H:%M")
    except:
        pass
    else:
        description = description[12:].strip()

    return _cnpj_in_names(description)


def _create_company(
    nickname: str,
    cnpj: str,
    by_cnpj: dict[str, str],
    by_name: dict[str, str],
    db: Database,
) -> None:
    """
    If there is no nickname nor company, try to get one
    """
    print(f"Creating {nickname} with {cnpj}.")

    # 1 - Look for a CNPJ
    if not cnpj:
        values = list(by_name.keys())
        chosen: str = process.extractOne(nickname, values)[0]
        found_cnpj = {value: key for key, value in by_cnpj.items()}[by_name[chosen]]
        print(f"Similar named {chosen} with cnpj {found_cnpj}")
        cnpj = input("Which CNPJ to use: ")

        print(f"Given cnpj {cnpj}")
        if not cnpj:
            cnpj = str(uuid.uuid4())
        # 2 - Given a new CNPJ, try to find a company
        elif cnpj in by_cnpj:
            name = by_cnpj.get(cnpj)
            assert name
            by_name[nickname] = name
            db.add(Company_Naming(nickname=nickname, name=name))
            return

    # 3 - If no company - ask for a name
    name = input("Which Name to use: ") or nickname
    category = input("Which category to use: ")
    by_cnpj[cnpj] = name
    by_name[nickname] = name
    by_name[name] = name
    db.add(Companies(name=name, cnpj=cnpj, default_category=category))
    db.add(Company_Naming(nickname=nickname, name=name))


def _currency_to_float(series: pd.Series):
    return series.str.replace(".", "").str.replace(",", ".").astype(float)


def _get_dataframe(xlsx_path: str) -> pd.DataFrame:
    d_f = pd.read_excel(xlsx_path, skiprows=2)

    d_f["Historico"] = d_f["Historico"].str.strip()

    # Filter start and end
    start_row: int = d_f[d_f["Historico"] == "Saldo Anterior"].index[0] + 1
    end_row: int = d_f[d_f["Historico"] == "S A L D O"].index[0]
    d_f = d_f.iloc[start_row:end_row]

    # Clean Data
    d_f["Data"] = pd.to_datetime(d_f["Data"], format="%d/%m/%Y")

    d_f["Valor R$ "] = _currency_to_float(d_f["Valor R$ "])

    d_f["Detalhamento Hist."] = d_f["Detalhamento Hist."].str.strip()

    # Extract name and cnpj
    d_f["cnpj"], d_f["nome"] = d_f["Detalhamento Hist."].apply(_get_info).str

    return d_f


def update_bb(xlsx_path: str = "resources/bb_fevereiro.xlsx"):
    """Update banco do brasil"""

    d_f = _get_dataframe(xlsx_path=xlsx_path)

    db = Database.from_default()

    with db.start():
        _assert_new_entries_only(db, d_f)

        by_name = {naming.nickname: naming.name for naming in db.get_company_names()}
        by_cnpj = {company.cnpj: company.name for company in db.get_companies()}

        for _, row in d_f.iterrows():
            naming = by_name.get(row["nome"], None)
            company = by_cnpj.get(row["cnpj"], None)

            exists = company or naming
            has_counterparty = row["Historico"] not in NO_COUNTERPARTY
            is_saida = row["Inf."] != "C"
            if (not exists) and (has_counterparty) and is_saida:
                _create_company(
                    nickname=row["nome"],
                    cnpj=row["cnpj"],
                    by_name=by_name,
                    by_cnpj=by_cnpj,
                    db=db,
                )

            db.add(
                Transactions(
                    id=None,
                    bank=BB_BANK,
                    date=row["Data"],
                    entry_type=EntryTipe.ENTRADA
                    if row["Inf."] == "C"
                    else EntryTipe.SAIDA,
                    transaction_type=row["Historico"],
                    category=None,
                    description=row["Detalhamento Hist."],
                    value=row["Valor R$ "],
                    counterpart_name=row["nome"],
                    validated=False,
                )
            )


def _assert_new_entries_only(db: Database, d_f):
    transactions = db.get_transactions(bank=BB_BANK)
    dates = {tran.date for tran in transactions}
    start: datetime = d_f["Data"].min()
    end: datetime = d_f["Data"].max()
    print(dates)
    print(start)
    print(end)

    is_less = any([date > start for date in dates])
    is_more = any([date < end for date in dates])
    if is_less and is_more:
        raise ValueError("Some dates are in between")


if __name__ == "__main__":
    typer.run(update_bb)
