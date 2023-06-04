from datetime import datetime
from enum import Enum
from pathlib import Path

import pandas as pd
import typer
from tabulate import tabulate

from fiscal.db import DATE_FORMAT, Database, EntryType, Transactions
from fiscal.fetcher import handle_inserts

root = Path(__file__).parent

BB_BANK = "bb"


class Columns(str, Enum):
    CNPJ = "cnpj"
    NAME = "nome"
    TRANSACTION = "Historico"
    DESCRIPTION = "Detalhamento Hist."
    VALOR = "Valor R$ "
    DATE = "Data"
    EXTERNAL_ID = "external_id"


class EntryInf(str, Enum):
    ENTRADA = "ENTRADA"
    SAIDA = "SAIDA"

    def to_entry_type(self) -> EntryType:
        match self.value:
            case EntryInf.SAIDA:
                return EntryType.SAIDA
            case EntryInf.ENTRADA:
                return EntryType.ENTRADA
        raise ValueError(self.value)


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
    """
    BB description is in the form:

    ...

    so we have those returns:
    """
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


def _get_dataframe(xlsx_path: str) -> pd.DataFrame:
    d_f = pd.read_excel(xlsx_path, skiprows=2)
    d_f.drop(
        columns=[
            "Data balancete",
            "Agencia Origem",
            "Lote",
            "Numero Documento",
            "Cod. Historico",
            "observacao",
        ],
        inplace=True,
    )

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

    for group, df_groupd in d_f.groupby(Columns.DATE):
        d_f.loc[df_groupd.index, Columns.EXTERNAL_ID] = (
            group.strftime(DATE_FORMAT)
            + "-"
            + df_groupd.reset_index().index.astype(str)
        )

    return d_f


def _currency_to_float(series: pd.Series):
    return (
        series.str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )


def _parse_row(row: pd.Series) -> tuple[Transactions, str]:
    cnpj = str(row[Columns.CNPJ])
    entry_type = EntryInf.ENTRADA if row["Inf."] == "C" else EntryInf.SAIDA
    date = row[Columns.DATE]
    assert isinstance(date, datetime)

    transaction = Transactions(
        bank=BB_BANK,
        date=date,
        entry_type=entry_type.to_entry_type(),
        transaction_type=str(row[Columns.TRANSACTION]),
        category=None,
        description=str(row["Detalhamento Hist."]),
        value=float(row[Columns.VALOR]),
        counterpart_name=str(row[Columns.NAME]),
        validated=False,
        external_id=str(row[Columns.EXTERNAL_ID]),
    )

    return transaction, cnpj


def update_bb(xlsx_path: str = "resources/bb_abril.xlsx"):
    """Update banco do brasil"""

    d_f = _get_dataframe(xlsx_path=xlsx_path)
    print(tabulate(d_f, headers="keys", tablefmt="psql"))
    transactions = [_parse_row(row) for _, row in d_f.iterrows()]

    db = Database.from_default()
    with db:
        handle_inserts(transactions, db)


if __name__ == "__main__":
    typer.run(update_bb)
