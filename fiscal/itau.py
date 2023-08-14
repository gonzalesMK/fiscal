from datetime import datetime
from enum import Enum

import pandas as pd
from sqlmodel import select
from tabulate import tabulate

from fiscal.db import DATE_FORMAT, Balance, Category, Database, EntryType, Transactions
from fiscal.fetcher import TransactionType, handle_inserts
from fiscal.reports import last_day_of_month


class Columns(str, Enum):
    BALANCE = "saldo (R$)"
    DATE = "data"
    DESCRIPTION = "lançamento"
    VALUE = "valor (R$)"
    EXTERNAL_ID = "external_id"
    ENTRY_TYPE = "positivo"
    TRANSACTION_TYPE = "tipo de lançamento"


def _get_transaction_type(_str):
    match _str:
        case "REDE   ELO  DB093122470":
            return "débito"
        case "REDE   MAST DB093122470":
            return "débito"
        case "REDE   VISA DB093122470":
            return "débito"
        case "REDE  MC  093122470":
            return "crédito"
        case "REDE  VS  093122470":
            return "crédito"
        case "REDE  DN  093122470":
            return "crédito"
        case "REDE  AM  093122470":
            return "crédito"
        case "REDE  EL  093122470":
            return "crédito"
        case "PIX QRS CONSOLIDADO":
            return "pix"
        case "SISPAG  PIX TRANSFERENCI":
            return "pix"

    if "PIX" in _str:
        return "pix"
    raise ValueError(f"Unknown transaction type: {_str}")


def _get_dataframe(xlsx_path: str) -> tuple[pd.DataFrame, float]:
    """Reads the excel file and returns a dataframe and the balance"""
    df = pd.read_excel(xlsx_path, skiprows=9)

    print(df.columns)
    # Select columns on Enum Columns

    # Transaction type should be "entrada" if value is positive
    df[Columns.ENTRY_TYPE] = df[Columns.VALUE].apply(
        lambda x: EntryType.ENTRADA if x > 0 else EntryType.SAIDA
    )

    # Convert '31/05/2023' to datetime
    df[Columns.DATE] = pd.to_datetime(df[Columns.DATE], format="%d/%m/%Y")

    ## Get Balances
    balance = df[df[Columns.DESCRIPTION] == "SALDO DO DIA"].iloc[-1][Columns.BALANCE]

    # Drop where value is missing
    df = df.dropna(subset=[Columns.VALUE])
    df = df[[Columns.DATE, Columns.VALUE, Columns.DESCRIPTION, Columns.ENTRY_TYPE]]

    # Adding external id
    for group, df_groupd in df.groupby(Columns.DATE):
        df.loc[df_groupd.index, Columns.EXTERNAL_ID] = (
            group.strftime(DATE_FORMAT)
            + "-"
            + df_groupd.reset_index().index.astype(str)
        )

    # Adding transaction type
    df[Columns.TRANSACTION_TYPE] = df[Columns.DESCRIPTION].apply(_get_transaction_type)

    # Create transaction_type from description
    return df, balance


def _parse_row(row: pd.Series) -> tuple[Transactions, str]:
    date = row[Columns.DATE]
    assert isinstance(date, datetime)

    value = row[Columns.VALUE]
    assert isinstance(value, float)

    return (
        Transactions(
            bank="itau",
            date=date,
            value=abs(value),
            description=str(row[Columns.DESCRIPTION]),
            entry_type=EntryType(row[Columns.ENTRY_TYPE]),
            transaction_type=str(row[Columns.TRANSACTION_TYPE]),
            category=Category.IGNORAR,
            counterpart_name="itau",
            validated=True,
            external_id=str(row[Columns.EXTERNAL_ID]),
        ),
        str(row[Columns.DESCRIPTION]),
    )


def _update_balance(db: Database, balance: float):
    last_day = last_day_of_month()

    statement = (
        select(Balance)
        .where(Balance.bank == "itau")
        .where(Balance.date == last_day.date())
    )
    exist_balance = db.exec(statement).all()

    if exist_balance:
        return

    db.insert_balance(
        Balance(
            date=last_day,
            bank="itau",
            balance=balance,
        )
    )


def update_itau(xlsx_path: str):
    d_f, balance = _get_dataframe(xlsx_path)

    print(tabulate(d_f, headers="keys", tablefmt="psql"))

    transactions = [_parse_row(row) for _, row in d_f.iterrows()]

    db = Database.from_default()
    with db:
        _update_balance(db, balance)
        handle_inserts(transactions, db)
    print(d_f)
