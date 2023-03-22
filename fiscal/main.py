from datetime import datetime
import uuid
from enum import Enum
from pathlib import Path
from thefuzz import process
import pandas as pd
import typer

from fiscal.db import Categories, Companies, Company_Naming, Database, Transactions

root = Path(__file__).parent

BB_BANK = "bb"


class Columns(str, Enum):
    CNPJ = "cnpj"
    NAME = "nome"
    TRANSACTION = "Historico"
    DESCRIPTION = "Detalhamento Hist."
    VALOR = "Valor R$ "
    DATE = "Data"


class TransactionType(str, Enum):
    BACEN = "BACEN-Res.An.Céd.Encam."
    TARIFA = "Tarifa Pacote de Serviços"
    IMPOSTO = "Impostos"
    PIX_REJEITADO = "Pix - Rejeitado"
    TARIFA_DOC = "Tar DOC/TED Eletrônico"
    DEPOSITO = "Depósito Online"


NO_COUNTERPARTY = [
    TransactionType.IMPOSTO,
    TransactionType.BACEN,
    TransactionType.TARIFA,
    TransactionType.TARIFA_DOC,
    TransactionType.PIX_REJEITADO,
    TransactionType.DEPOSITO,
]

IGNORE_TYPE = [TransactionType.PIX_REJEITADO, TransactionType.DEPOSITO]
TARIFAS = [TransactionType.BACEN, TransactionType.TARIFA, TransactionType.TARIFA_DOC]


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


def _ask_for_cnpj(companies: dict[str, Companies], nickname: str) -> str:
    _print_company_suggestion(companies, nickname)

    cnpj = input("Which CNPJ to use: ")
    print(f"Given cnpj {cnpj}")

    if not cnpj:
        cnpj = str(uuid.uuid4())
    return cnpj


def _create_company(nickname: str, cnpj: str, db: Database) -> Companies:
    print(f"Creating {nickname} with {cnpj}.")

    name = input("Which Name to use: ") or nickname
    category = input("Which category to use: ")

    company = Companies(name=name, cnpj=cnpj, default_category=category)

    db.add(company)
    db.add(Company_Naming(nickname=nickname, name=name))

    return company


def update_bb(xlsx_path: str = "resources/bb_fevereiro.xlsx"):
    """Update banco do brasil"""

    d_f = _get_dataframe(xlsx_path=xlsx_path)

    db = Database.from_default()

    with db.start():
        _assert_new_entries_only(db, d_f)
        companies = _get_companies_mapping(db)

        for _, row in d_f.iterrows():
            transaction, cnpj = _parse_row(row)
            counterpart = transaction.counterpart_name or ""

            print(
                f"{transaction.counterpart_name}\t|{transaction.entry_type}\t|{transaction.transaction_type}\t|{transaction.date}"
            )
            if _has_counterpart(transaction):
                company = _get_company(companies, counterpart, cnpj, db)

                companies[company.cnpj] = company
                companies[company.name] = company
                companies[counterpart] = company
                print(companies[counterpart])
            else:
                company = None
                transaction.counterpart_name = None

            category = _default_cat_for_transaction(transaction, company)

            transaction.category = category

            db.add(transaction)


def _get_dataframe(xlsx_path: str) -> pd.DataFrame:
    d_f = pd.read_excel(xlsx_path, skiprows=2)

    d_f[Columns.TRANSACTION] = d_f[Columns.TRANSACTION].str.strip()

    # Filter start and end
    start_row: int = d_f[d_f[Columns.TRANSACTION] == "Saldo Anterior"].index[0] + 1
    end_row: int = d_f[d_f[Columns.TRANSACTION] == "S A L D O"].index[0]
    d_f = d_f.iloc[start_row:end_row]

    # Clean Data
    d_f[Columns.DATE] = pd.to_datetime(d_f[Columns.DATE], format="%d/%m/%Y")

    d_f[Columns.VALOR] = _currency_to_float(d_f[Columns.VALOR])

    d_f["Detalhamento Hist."] = d_f["Detalhamento Hist."].str.strip()

    # Extract name and cnpj
    d_f[Columns.CNPJ], d_f[Columns.NAME] = (
        d_f["Detalhamento Hist."].apply(_get_info).str
    )

    return d_f


def _currency_to_float(series: pd.Series):
    return (
        series.str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )


def _assert_new_entries_only(db: Database, d_f):
    transactions = db.get_transactions(bank=BB_BANK)
    dates = {tran.date for tran in transactions}
    start: datetime = d_f[Columns.DATE].min()
    end: datetime = d_f[Columns.DATE].max()

    is_less = any([date > start for date in dates])
    is_more = any([date < end for date in dates])
    if is_less and is_more:
        raise ValueError("Some dates are in between")


def _get_companies_mapping(db: Database) -> dict[str, Companies]:
    company_listing = db.get_companies()
    names = db.get_company_names()
    companies = {company.cnpj: company for company in company_listing}
    companies |= {company.name: company for company in company_listing}
    companies |= {name.nickname: companies[name.name] for name in names}
    return companies


def _parse_row(row: pd.Series) -> tuple[Transactions, str]:
    cnpj = str(row[Columns.CNPJ])
    entry_type = EntryTipe.ENTRADA if row["Inf."] == "C" else EntryTipe.SAIDA
    date = row[Columns.DATE]
    assert isinstance(date, datetime)

    transaction = Transactions(
        bank=BB_BANK,
        date=date,
        entry_type=entry_type,
        transaction_type=str(row[Columns.TRANSACTION]),
        category=None,
        description=str(row["Detalhamento Hist."]),
        value=str(row[Columns.VALOR]),
        counterpart_name=str(row[Columns.NAME]),
        validated=False,
    )
    return transaction, cnpj


def _has_counterpart(transaction: Transactions) -> bool:
    # Handle inflow and no counterpaty transactions
    has_counterparty = transaction.transaction_type not in NO_COUNTERPARTY
    saida = transaction.entry_type == EntryTipe.SAIDA
    return has_counterparty and saida


def _get_company(
    companies: dict[str, Companies],
    counterpart: str,
    cnpj: str | None,
    db: Database,
) -> Companies:
    # Simple case where name is on database
    if counterpart in companies:
        return companies[counterpart]

    # If no name on database, check if there is a cnpj
    if not cnpj:
        cnpj = _ask_for_cnpj(companies, counterpart)

    if cnpj in companies:
        company = companies[cnpj]
        db.add(Company_Naming(nickname=counterpart, name=company.name))
        return company

    # Otherwise create the company
    return _create_company(counterpart, cnpj, db)


def _print_company_suggestion(companies: dict[str, Companies], nickname: str):
    values = list(key for key in companies.keys() if not key.isdigit())

    chosen = process.extractOne(nickname, values)

    found_cnpj = ""
    if chosen:
        chosen = str(chosen[0])
        found_cnpj = companies[chosen]

    print(f"Similar named {chosen} with cnpj {found_cnpj}")


def _default_cat_for_transaction(
    transaction: Transactions, company: Companies | None
) -> str | None:
    if transaction.entry_type == EntryTipe.ENTRADA:
        return Categories.ENTRADA

    if company:
        return company.default_category

    type_ = transaction.transaction_type
    if type_ in IGNORE_TYPE:
        return Categories.IGNORAR
    if type_ in TARIFAS:
        return Categories.BANCOS
    if type_ == TransactionType.IMPOSTO:
        return Categories.IMPOSTO

    raise ValueError(f"Not found '{type_}'")


if __name__ == "__main__":
    typer.run(update_bb)
