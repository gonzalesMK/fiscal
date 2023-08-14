from datetime import date, datetime, time, timedelta
from enum import Enum

import pandas as pd
import requests
import typer
from pydantic import BaseModel, Field
from requests.models import HTTPBasicAuth, HTTPError
from tabulate import tabulate
from thefuzz.process import logging
from urllib3.connectionpool import HTTPConnection

from fiscal.banco_inter import INTER_BANK
from fiscal.db import DATE_FORMAT, Category, Database, EntryType, Transactions
from fiscal.fetcher import handle_inserts


class Columns(str, Enum):
    DATE = "data"
    VALUE = "valor depositado"
    DESCRIPTION = "bandeira"
    TRANSACTION_TYPE = "modalidade"


def _get_dataframe(xlsx_path: str) -> pd.DataFrame:
    """Reads the excel file and returns a dataframe"""
    df = pd.read_excel(xlsx_path, skiprows=1, sheet_name="recebidos")

    # Select columns on Enum Columns
    df = df[
        [Columns.DATE, Columns.VALUE, Columns.DESCRIPTION, Columns.TRANSACTION_TYPE]
    ]

    # Convert 'R$ 1.455,91' to float
    # df[Columns.VALUE] = df[Columns.VALUE].apply(
    #     lambda x: float(x.replace("R$ ", "").replace(".", "").replace(",", "."))
    # )

    # Convert '31/05/2023' to datetime
    df[Columns.DATE] = pd.to_datetime(df[Columns.DATE], format="%d/%m/%Y")

    return df


TRANSACION_TYPE = {
    "credit": "crédito",
    "debit": "débito",
}
BRAND_CODE = {
    1: "mastercard",
    2: "visa",
    3: "dinners",
    4: "cabal",
    5: "sicred",
    6: "sorocred",
    7: "hipercard",
    8: "cup",
    9: "calcard",
    10: "construcard",
    11: "avista",
    12: "credsystem",
    13: "amex",
    14: "elo",
    15: "hiper",
    16: "alelo",
    20: "sodexo",
    21: "vr",
    22: "greencard",
    23: "nutricash",
    24: "planvale",
    25: "verocheque",
    26: "coopercard",
    27: "abrapetite",
    28: "bamex beneficios",
    29: "biq benefícios",
    30: "bonuscred",
    31: "convenios card",
    32: "credialimentacao",
    33: "eucard",
    34: "facecard",
    35: "flex",
    36: "goodcard",
    37: "lecard",
    38: "libercard",
    39: "maxxcard",
    40: "nutricard",
    41: "ok cartoes",
    42: "onecard",
    43: "sindplus",
    44: "uauhbeneficios",
    45: "vale shop",
    46: "vegas card",
    47: "visasoft pay",
    48: "volus",
    49: "vscard",
    50: "up brasil",
    51: "verocard",
    52: "ticket",
    53: "van",
    54: "pli itau fai",
    55: "pl bradesco",
    56: "pl banco do brasil",
    57: "pl citibank",
    58: "pl credsystem",
    59: "pl porto seguro",
    60: "pagamento de fatura",
    72: "nova bandeira",
    74: "banescard",
    76: "jcb",
    77: "credz",
    999: "outros",
}
# {
#                 "status": "approved",
#                 "brandcode": 1,
#                 "mdramount": 0.58,
#                 "nsu": 29871748,
#                 "flexfee": 0.0,
#                 "salesummarynumber": 4132811,
#                 "saledate": "2023-08-05",
#                 "flex": false,
#                 "device": "sn795714",
#                 "boardingfeeamount": 0.0,
#                 "feetotal": 1.99,
#                 "installmentquantity": 1,
#                 "capturetype": "pos",
#                 "salehour": "13:27:37",
#                 "movementdate": "2023-08-05",
#                 "amount": 29.0,
#                 "tokenized": true,
#                 "tracking": [
#                     {
#                         "date": "2023-08-05",
#                         "status": "approved",
#                         "amount": 29.0
#                     }
#                 ],
#                 "capturetypecode": 4,
#                 "flexamount": 0.0,
#                 "authorizationcode": 273864,
#                 "netamount": 28.42,
#                 "devicetype": "poo",
#                 "prepaid": false,
#                 "discountamount": 0.58,
#                 "mdrfee": 1.99,
#                 "merchant": {
#                     "companynumber": "93122470",
#                     "companyname": "montelena padaria",
#                     "documentnumber": "27723354000110",
#                     "tradename": "helena maria usberti decico"
#                 },
#                 "modality": {
#                     "type": "credit",
#                     "code": 1,
#                     "product": "no_installments",
#                     "productcode": 1
#                 },
#                 "tokennumber": "545931******7351"
#             }


class Modality(BaseModel):
    type: str
    code: int
    product: str
    productCode: int


class Transaction(BaseModel):
    status: str
    brandCode: int
    feeTotal: float
    movementDate: date
    saleHour: time
    amount: float
    modality: Modality
    authorizationCode: str
    strAuthorizationCode: str | None
    tokenNumber: str | None


class Cursor(BaseModel):
    hasNextKey: bool
    nextKey: str | None = Field(None)


class RedeContent(BaseModel):
    transactions: list[Transaction]


class RedeDTO(BaseModel):
    content: RedeContent
    cursor: Cursor


class Rede:
    bearer_token: str

    def __init__(
        self, username: str, password: str, client_id: str, client_secret: str
    ):
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self._session = requests.session()

        self.authenticate()

    def authenticate(self) -> None:
        basic = HTTPBasicAuth(self.client_id, self.client_secret)
        params = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }

        resp = self._session.post(
            url="https://api.userede.com.br/redelabs/oauth/token",
            data=params,
            auth=basic,
        )

        resp.raise_for_status()

        self.bearer_token = f"Bearer {resp.json()['access_token']}"

    def get_transactions(
        self, start_date: datetime, end_date: datetime
    ) -> list[Transactions]:
        has_next = True
        next_key: str | None = ""
        trans = []

        while has_next:
            response = self._get_transactions(start_date, end_date, next_key)

            trans += [
                self._to_default_transaction(tran)
                for tran in response.content.transactions
            ]

            has_next = response.cursor.hasNextKey
            next_key = response.cursor.nextKey

        return trans

    def _get_transactions(
        self, start_date: datetime, end_date: datetime, next_key: str
    ) -> RedeDTO:
        resp = self._session.get(
            url="https://api.userede.com.br/redelabs/merchant-statement/v1/sales",
            headers={"Authorization": self.bearer_token},
            params={
                "startDate": str(start_date.date().strftime(DATE_FORMAT)),
                "endDate": str(end_date.date().strftime(DATE_FORMAT)),
                "parentCompanyNumber": "93122470",
                "subsidiaries": "93122470",
                "size": "100",
                **({"pageKey": next_key} if next_key else {}),
            },
        )

        try:
            resp.raise_for_status()
        except HTTPError as e:
            print(resp.text)
            raise e

        return RedeDTO.parse_obj(resp.json())

    @staticmethod
    def _to_default_transaction(tran: Transaction) -> Transactions:
        return Transactions(
            bank="rede",
            date=datetime.combine(tran.movementDate, tran.saleHour),
            value=tran.amount,
            description=BRAND_CODE[tran.brandCode],
            entry_type=EntryType.ENTRADA,
            transaction_type=TRANSACION_TYPE[tran.modality.type],
            category=Category.ENTRADA,
            counterpart_name="rede",
            validated=True,
            external_id=f"{tran.movementDate}-{tran.saleHour}-{tran.authorizationCode}-{tran.strAuthorizationCode}-{tran.tokenNumber}",
        )


def _parse_row(row: pd.Series) -> tuple[Transactions, str]:
    date = row[Columns.DATE]
    assert isinstance(date, datetime)

    value = row[Columns.VALUE]
    assert isinstance(value, float)

    # datetime to isoformat
    date_str = date.strftime(DATE_FORMAT)

    return (
        Transactions(
            bank="rede",
            date=date,
            value=value,
            description=str(row[Columns.DESCRIPTION]),
            entry_type=EntryType.ENTRADA,
            transaction_type=str(row[Columns.TRANSACTION_TYPE]),
            category=Category.ENTRADA,
            counterpart_name="rede",
            validated=True,
            external_id=f"{date_str}-{row[Columns.DESCRIPTION]}",
        ),
        str(row[Columns.DESCRIPTION]),
    )


def _get_latest_transactions(
    client: Rede, db: Database
) -> list[tuple[Transactions, str]]:
    last_date = db.get_latest_transaction(bank="rede") or (
        datetime.now() - timedelta(days=1)
    )
    last_date -= timedelta(days=1)

    yesterday = datetime.combine(date.today() + timedelta(days=-1), datetime.max.time())

    return [
        (t, t.description)
        for t in client.get_transactions(start_date=last_date, end_date=yesterday)
    ]


def update_rede(
    username: str = typer.Option(..., envvar="REDE_USERNAME"),
    password: str = typer.Option(..., envvar="REDE_PASSWORD"),
    client_id: str = typer.Option(..., envvar="REDE_CLIENT_ID"),
    client_secret: str = typer.Option(..., envvar="REDE_CLIENT_SECRET"),
):
    client = Rede(
        username=username,
        password=password,
        client_id=client_id,
        client_secret=client_secret,
    )

    db = Database.from_default()

    with db:
        transactions = _get_latest_transactions(client, db)

        print(
            tabulate(
                [t[0].dict() for t in transactions], headers="keys", tablefmt="psql"
            )
        )
        handle_inserts(transactions, db)
