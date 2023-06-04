from datetime import date, datetime, timedelta
from enum import Enum

import requests
import typer
from pydantic import BaseModel
from responses import _recorder
from typing_extensions import Self

from fiscal import fetcher
from fiscal.db import DATE_FORMAT, Database, EntryType, Transactions

URL_OAUTH = "https://cdpj.partners.bancointer.com.br/oauth/v2/token"
URL_EXTRATO = "https://cdpj.partners.bancointer.com.br/banking/v2/extrato/completo"


INTER_BANK = "inter"


class TipoOperacao(str, Enum):
    D = "D"
    C = "C"

    def to_entry_type(self) -> EntryType:
        match self.value:
            case TipoOperacao.D:
                return EntryType.SAIDA
            case TipoOperacao.C:
                return EntryType.ENTRADA
        raise ValueError(self.value)


class PagamentoTransacao(BaseModel):
    valorTotal: str
    detalheDescricao: str
    contaBancaria: str
    adicionado: str
    dataVencimento: str
    empresaEmissora: str | None
    valorOriginal: str
    desconto: str
    cpfCnpj: str
    valorAumentado: str | None
    codBarras: str
    valorParcial: str
    hora: str
    juros: str
    multa: str
    empresaOrigem: str
    nomeDestinatario: str
    tipoDetalhe: str
    nomeOrigem: str
    autenticacao: str

    class Config:
        anystr_lower = True


class DepositoTransacao(BaseModel):
    dataVencimento: str
    tipoDetalhe: str
    dataEmissao: str
    nossoNumero: str
    codBarras: str

    class Config:
        anystr_lower = True


class DebitoTransacao(BaseModel):
    estabelecimento: str
    tipoDetalhe: str

    class Config:
        anystr_lower = True


class ChequeTransacao(BaseModel):
    agencia: str
    numeroChequeBancario: str
    contaBancaria: str
    dataRetorno: str
    motivoRetorno: str
    descricaoChequeBancario: str
    nomeEmpresa: str
    tipoDetalhe: str
    codigoAfiliado: str

    class Config:
        anystr_lower = True


class BoletoTransacao(BaseModel):
    dataVencimento: str
    dataTransacao: str
    nossoNumero: str
    seuNumero: str
    codBarras: str
    juros: str
    multa: str
    nome: str
    dataLimite: str
    tipoDetalhe: str
    cpfCnpj: str
    dataEmissao: str
    abatimento: str

    class Config:
        anystr_lower = True


class TransferenciaTransacao(BaseModel):
    contaBancariaPagador: str
    descricaoTransferencia: str
    agenciaPagador: str
    bancoRecebedor: str
    contaBancariaRecebedor: str
    cpfCnpjRecebedor: str
    cpfCnpjPagador: str
    nomePagador: str
    nomeEmpresaPagador: str
    nomeRecebedor: str
    tipoDetalhe: str
    idTransferencia: str | None
    agenciaRecebedor: str
    dataEfetivacao: str

    class Config:
        anystr_lower = True


class PixTransacao(BaseModel):
    nomePagador: str
    descricaoPix: str | None
    cpfCnpjPagador: str
    nomeEmpresaPagador: str
    tipoDetalhe: str
    endToEndId: str
    chavePixRecebedor: str
    nomeEmpresaRecebedor: str | None
    nomeRecebedor: str | None
    cpfCnpjRecebedor: str | None
    origemMovimentacao: str | None

    class Config:
        anystr_lower = True


class Transaction(BaseModel):
    idTransacao: str
    dataInclusao: datetime
    dataTransacao: date
    tipoTransacao: str
    tipoOperacao: TipoOperacao
    valor: str
    titulo: str
    descricao: str
    detalhes: PixTransacao | TransferenciaTransacao | BoletoTransacao | ChequeTransacao | DebitoTransacao | DepositoTransacao | PagamentoTransacao | None

    class Config:
        anystr_lower = True


class GetTransactions(BaseModel):
    totalPaginas: int
    totalElementos: int
    ultimaPagina: bool
    primeiraPagina: bool
    tamanhoPagina: int
    numeroDeElementos: int
    transacoes: list[Transaction]

    class Config:
        anystr_lower = True


class InterBank:
    bearer_token: str | None

    def __init__(self) -> None:
        self._session = requests.session()

    @classmethod
    def from_secrets(cls, client_id: str, client_secret: str) -> Self:
        client = cls()
        client.authenticate(client_id, client_secret)
        return client

    def authenticate(self, client_id: str, client_secret: str) -> None:
        headers = {
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "extrato.read",
            "grant_type": "client_credentials",
        }

        resp = self._session.post(
            url=URL_OAUTH,
            data=headers,
            cert=("certificado.crt", "chave.key"),
        )

        resp.raise_for_status()

        self.bearer_token = f"Bearer {resp.json()['access_token']}"

    def get_transactions(
        self, start_date: datetime, end_date: datetime
    ) -> GetTransactions:
        pagina = 0
        transactions = self._get_transactions(
            start_date=start_date, end_date=end_date, pagina=pagina
        )

        while not transactions.ultimaPagina:
            pagina += 1
            more_transactions = self._get_transactions(
                start_date=start_date, end_date=end_date, pagina=pagina
            )
            transactions.transacoes += more_transactions.transacoes
            transactions.ultimaPagina = more_transactions.ultimaPagina

        return transactions

    def _get_transactions(
        self, start_date: datetime, end_date: datetime, pagina: int | None = None
    ) -> GetTransactions:
        """
        Should paginate requests here
        """
        if not self.bearer_token:
            raise ValueError("Not Authenticated")

        params = {
            "dataInicio": start_date.date().strftime(DATE_FORMAT),
            "dataFim": end_date.date().strftime(DATE_FORMAT),
        }
        if pagina:
            params["pagina"] = str(pagina)

        resp = self._session.get(
            URL_EXTRATO,
            headers={
                "Authorization": self.bearer_token,
            },
            params=params,
            cert=("certificado.crt", "chave.key"),
        )

        resp.raise_for_status()

        return GetTransactions.parse_raw(resp.content)


@_recorder.record(file_path="out.yaml")
def test_recorder():
    client = InterBank()

    client.authenticate(
        client_id="",
        client_secret="",
    )

    client.get_transactions(
        start_date=datetime(year=2023, month=3, day=19),
        end_date=datetime(year=2023, month=3, day=20),
    )
    client.get_transactions(
        start_date=datetime(year=2023, month=3, day=17),
        end_date=datetime(year=2023, month=3, day=18),
    )
    client.get_transactions(
        start_date=datetime(year=2023, month=3, day=20),
        end_date=datetime(year=2023, month=3, day=23),
    )
    client.get_transactions(
        start_date=datetime(year=2023, month=1, day=1),
        end_date=datetime(year=2023, month=3, day=22),
    )


def _convert_transaction(transaction: Transaction) -> tuple[Transactions, str]:
    cnpj = ""
    if isinstance(transaction.detalhes, PixTransacao) and hasattr(
        transaction.detalhes, "cpfCnpjRecebedor"
    ):
        cnpj = transaction.detalhes.cpfCnpjRecebedor or ""

    return (
        Transactions(
            bank=INTER_BANK,
            date=datetime.combine(transaction.dataTransacao, datetime.min.time()),
            entry_type=transaction.tipoOperacao.to_entry_type(),
            transaction_type=transaction.tipoTransacao,
            category=None,
            description=transaction.descricao,
            value=transaction.valor,
            counterpart_name=transaction.descricao,
            validated=False,
            external_id=transaction.idTransacao,
        ),
        cnpj,
    )


def _get_transactions(
    client: InterBank, db: Database
) -> list[tuple[Transactions, str]]:
    last_date = db.get_latest_transaction(bank=INTER_BANK) or (
        datetime.now() - timedelta(days=89)
    )
    last_date -= timedelta(days=1)

    yesterday = datetime.combine(date.today() + timedelta(days=-1), datetime.max.time())

    inter_transactions = client.get_transactions(
        start_date=last_date, end_date=yesterday
    )

    return [_convert_transaction(tran) for tran in inter_transactions.transacoes]


def update_banco_inter(
    client_id: str = typer.Option(..., envvar="INTER_CLIENT_ID"),
    client_secret: str = typer.Option(..., envvar="INTER_CLIENT_SECRET"),
):
    db = Database.from_default()

    client = InterBank.from_secrets(client_id=client_id, client_secret=client_secret)

    with db:
        transactions = _get_transactions(db=db, client=client)
        fetcher.handle_inserts(transactions, db)


if __name__ == "__main__":
    typer.run(update_banco_inter)
