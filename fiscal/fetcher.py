import uuid
from enum import Enum
from thefuzz import process

from fiscal.db import (
    Category,
    Companies,
    Company_Naming,
    Database,
    EntryType,
    Transactions,
)


class TransactionType(str, Enum):
    # BB
    BACEN = "BACEN-Res.An.Céd.Encam."
    TARIFA = "Tarifa Pacote de Serviços"
    IMPOSTO = "Impostos"
    PIX_REJEITADO = "Pix - Rejeitado"
    TARIFA_DOC = "Tar DOC/TED Eletrônico"
    DEPOSITO = "Depósito Online"

    # Inter BB
    DEBITO_EM_CONTA = "debito_em_conta"
    DEPOSITO_BOLETO = "deposito_boleto"
    ANTECIPACAO_RECEBIVEIS = "antecipacao_recebiveis"
    ANTECIPACAO_RECEBIVEIS_CARTAO = "antecipacao_recebiveis_cartao"
    BOLETO_COBRANCA = "boleto_cobranca"
    CAMBIO = "cambio"
    CASHBACK = "cashback"
    CHEQUE = "cheque"
    ESTORNO = "estorno"
    DOMICILIO_CARTAO = "domicilio_cartao"
    FINANCIAMENTO = "financiamento"
    IMPOSTO_INTER = "imposto"
    INTERPAG = "interpag"
    INVESTIMENTO = "investimento"
    JUROS = "juros"
    MAQUININHA_GRANITO = "maquininha_granito"
    MULTA = "multa"
    OUTROS = "outros"
    PAGAMENTO = "pagamento"
    PIX = "pix"
    PROVENTOS = "proventos"
    SAQUE = "saque"
    COMPRA_DEBITO = "compra_debito"
    DEBITO_AUTOMATICO = "debito_automatico"
    TARIFA_INTER = "tarifa"
    TRANSFERENCIA = "transferencia"


NO_COUNTERPARTY = [
    # BB
    TransactionType.IMPOSTO,
    TransactionType.BACEN,
    TransactionType.TARIFA,
    TransactionType.TARIFA_DOC,
    TransactionType.PIX_REJEITADO,
    TransactionType.DEPOSITO,
    # Inter
    TransactionType.IMPOSTO_INTER,
    TransactionType.JUROS,
    TransactionType.SAQUE,
    TransactionType.TARIFA_INTER,
    TransactionType.INVESTIMENTO,
]

IGNORE_TYPE = [
    TransactionType.PIX_REJEITADO,
    TransactionType.DEPOSITO,
    TransactionType.INVESTIMENTO,
]
TARIFAS = [
    TransactionType.BACEN,
    TransactionType.TARIFA,
    TransactionType.TARIFA_DOC,
    TransactionType.TARIFA_INTER,
]

IMPOSTOS = [
    TransactionType.IMPOSTO,
    TransactionType.IMPOSTO_INTER,
]

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


def _get_companies_mapping(db: Database) -> dict[str, Companies]:
    company_listing = db.get_companies()
    names = db.get_company_names()
    companies = {company.cnpj: company for company in company_listing}
    companies |= {company.name.lower(): company for company in company_listing}
    companies |= {name.nickname.lower(): companies[name.name] for name in names}
    return companies


def _has_counterpart(transaction: Transactions) -> bool:
    # Handle inflow and no counterpaty transactions
    has_counterparty = transaction.transaction_type not in NO_COUNTERPARTY
    saida = transaction.entry_type == EntryType.SAIDA
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
    values = list(key.lower() for key in companies.keys() if not key.isdigit())

    chosen = process.extractOne(nickname.lower(), values)

    found_cnpj = ""
    if chosen:
        chosen = str(chosen[0])
        found_cnpj = companies[chosen].cnpj

    print(f"Similar named {chosen} with cnpj {found_cnpj}")


def _default_cat_for_transaction(
    transaction: Transactions, company: Companies | None
) -> str | None:
    if transaction.entry_type == EntryType.ENTRADA:
        return Category.ENTRADA

    if company:
        return company.default_category or None

    type_ = transaction.transaction_type
    if type_ in IGNORE_TYPE:
        return Category.IGNORAR
    if type_ in TARIFAS:
        return Category.BANCOS
    if type_ in IMPOSTOS:
        return Category.IMPOSTO

    raise ValueError(f"Not found '{type_}'")


def _remove_existent_transactions(
    db: Database, new_transactions: list[tuple[Transactions, str]]
):
    """
    Filter transactions by checking if their id is already in the database
    """
    transactions = db.get_transactions(bank=new_transactions[0][0].bank)
    existent_ids = {tran.external_id for tran in transactions}
    return [
        trans for trans in new_transactions if trans[0].external_id not in existent_ids
    ]


def handle_inserts(transactions: list[tuple[Transactions, str]], db: Database) -> None:
    transactions = _remove_existent_transactions(db, transactions)
    companies = _get_companies_mapping(db)

    transactions.sort(key=lambda row: row[0].date)

    for trans, cnpj in transactions:
        counterpart = trans.counterpart_name or ""

        print(
            f"{trans.counterpart_name}\t|{trans.entry_type}\t|{trans.transaction_type}\t|{trans.date}"
        )

        if _has_counterpart(trans):
            company = _get_company(companies, counterpart, cnpj, db)

            companies[company.cnpj] = company
            companies[company.name] = company
            companies[counterpart] = company
            print(companies[counterpart])
        else:
            company = None
            trans.counterpart_name = None

        category = _default_cat_for_transaction(trans, company)

        trans.category = category

        db.add(trans)

        db._session.commit()
        db._session.flush()
