from datetime import date, datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import case, func
from sqlmodel import select
from tabulate import tabulate

from fiscal.db import Balance, Database, Transactions


def first_day_of_month(n_previous=1) -> datetime:
    today = datetime.today()
    target_date = today - relativedelta(months=n_previous) + relativedelta(day=1)

    # Date to datetime on start of the day
    target_date = datetime.combine(target_date.date(), datetime.min.time())

    return target_date


def last_day_of_month(n_previous=1) -> datetime:
    today = datetime.today()
    last_day_of_month = today - relativedelta(months=n_previous) + relativedelta(day=31)

    # Date to datetime on end of the day
    last_day_of_month = datetime.combine(last_day_of_month.date(), datetime.max.time())

    return last_day_of_month


def diff_balance():
    """
    Return the difference in balance for each bank from the end of one month to the end of the next one
    """
    print("Balance Report")

    db = Database.from_default()

    with db:
        last_day = last_day_of_month(1)
        first_day = last_day_of_month(2).date()

        statement = (
            select(Balance)
            .where(Balance.date >= first_day)
            .where(Balance.date <= last_day)
            .where(Balance.bank != "inter - investimentos")
        )

        balances = db.exec(statement).all()

        # Convert list of balances to pandas dataframe
        df = pd.DataFrame([balance.dict() for balance in balances])

        # Pivot so date is a new column
        df = df.pivot(index="bank", columns="date", values="balance")

        # Include row with index total
        df.loc["Total"] = df.sum()

        df["diff"] = df[last_day.date()] - df[first_day]
        print(tabulate(df, headers="keys", tablefmt="psql"))


def dre():
    """
    Return the difference in balance for each bank from the end of one month to the end of the next one
    """
    db = Database.from_default()

    print("Total here should match the total in balances\n")

    print(
        (
            "\t1. If consolidado and balances are matching, this means there are differences\n"
            + "\tthis means that  there are transferência/ignorar wrong labeld"
        )
    )
    with db:
        last_day = last_day_of_month(1)
        first_day = first_day_of_month(1)

        statement = (
            select(Transactions)
            .where(Transactions.category != "transferencia")
            .where(Transactions.bank != "rede")
            .where(Transactions.date >= first_day)
            .where(Transactions.date <= last_day)
        )

        transactions = db.exec(statement).all()

        # Convert list of balances to pandas dataframe
        df = pd.DataFrame([balance.dict() for balance in transactions])

        # fill NA on category empty
        df["category"] = df["category"].fillna("n/a")

        # Conver value column to float
        df["value"] = df["value"].astype(float)

        # Make negative where transactions are not entrada or transferencia
        mask_saidas = df["entry_type"] == "saida"
        df.loc[mask_saidas, "value"] = df.loc[mask_saidas, "value"] * -1

        df = df[["date", "value", "category"]]

        # Group by category
        df = (
            df.groupby(["category"])
            .sum(numeric_only=True)
            .sort_values(by=["value"], ascending=False)
        )

        df.sort_values(by=["value"], ascending=False, inplace=True)

        print("All Transactions")

        df.loc["Total"] = df.sum()
        print(tabulate(df, headers="keys", tablefmt="psql"))

        print("DRE")

        # Remove "compras" from the dataframe
        df = df.drop("compras")
        df = df.drop("investimentos", errors="ignore")
        df = df.drop("resgate", errors="ignore")
        df = df.drop("estorno", errors="ignore")
        df = df.drop("Total")

        df.loc["Total"] = df.sum()
        print(tabulate(df, headers="keys", tablefmt="psql"))


def transfers():
    """
    Validate all transfers between banks
    """
    print("Transfer Report\n")
    print("\t1. Totals should match\n\n")

    db = Database.from_default()

    with db:
        last_day = last_day_of_month(1)
        first_day = first_day_of_month(1)

        statement = (
            select(
                func.sum(Transactions.value),  # noqa
                Transactions.bank,
                Transactions.entry_type,
            )
            .where(Transactions.category == "transferencia")
            .where(Transactions.date >= first_day)
            .where(Transactions.date <= last_day)
            .group_by(Transactions.bank, Transactions.entry_type)
            .order_by(Transactions.value)
        )

        transactions = db.exec(statement).all()

        # Convert list of balances to pandas dataframe
        df_saidas = (
            pd.DataFrame(transactions, columns=["value", "bank", "entry_type"])
            .pivot(index="bank", columns="entry_type", values="value")
            .fillna(0)
        )

        # Create a column total with the diff from entrada and saida
        df_saidas.loc["total"] = df_saidas.sum()

        # pandas display dataframe pretty
        print(df_saidas.to_markdown(floatfmt=",.2f"))


def entradas_e_saidas_por_banco():
    """
    Sum all transactions for each bank
    """
    print("Consolidado Report - all transactions")

    db = Database.from_default()

    with db:
        last_day = last_day_of_month(1)
        first_day = first_day_of_month(1)

        statement = (
            select(
                func.sum(Transactions.value),  # noqa
                Transactions.bank,
                Transactions.entry_type,
            )
            .where(Transactions.date >= first_day)
            .where(Transactions.date <= last_day)
            .group_by(Transactions.bank, Transactions.entry_type)
            .order_by(Transactions.value)
        )

        transactions = db.exec(statement).all()

        # Convert list of balances to pandas dataframe
        df_saidas = (
            pd.DataFrame(transactions, columns=["value", "bank", "entry_type"])
            .pivot(index="bank", columns="entry_type", values="value")
            .fillna(0)
        )

        # Create a column total with the diff from entrada and saida
        df_saidas["total"] = df_saidas["entrada"] - df_saidas["saida"]

        # pandas display dataframe pretty
        print(df_saidas.to_markdown(floatfmt=",.2f"))


# Validar se o que a REDE diz que me transferiu bate com o que eu recebi no itau


def compare_itau_and_rede():
    """
    (first) Check if numbers are sound
    """
    db = Database.from_default()

    print()

    print(
        (
            " Entradas regras: "
            + "\n\t 1. Checar se o débito|crédito da REDE e do ITAÚ batem"
            + "\n\t 2. Inter não deve ter pix na categoria entrada (pois não recebemos $ no pix) "
            + "\n\t\t em geral inter é recebimento, transferencia de outro banco ou estorno "
            + "\n\t 3. Pix - rejeitado deve ser categoria ignorar"
        )
    )
    print("\n\n\n")
    with db:
        last_day = last_day_of_month(1)
        first_day = first_day_of_month(1)

        statement = (
            select(Transactions)
            .where(Transactions.date >= first_day)
            .where(Transactions.date <= last_day)
        )
        transactions = db.exec(statement).all()

        # Convert list of balances to pandas dataframe
        df = pd.DataFrame([balance.dict() for balance in transactions])
        df = df[["date", "value", "transaction_type", "entry_type", "bank", "category"]]

        df["transaction_type"] = df["transaction_type"].replace("pix - recebido", "pix")
        df["transaction_type"] = df["transaction_type"].replace("pix - enviado", "pix")

        # Conver value column to float
        df["value"] = df["value"].astype(float)

        df = (
            df.groupby(["bank", "entry_type", "transaction_type", "category"])
            .sum(numeric_only=True)
            .sort_values(by=["value"], ascending=False)
            .reset_index()
            .pivot(
                index=["transaction_type", "entry_type", "category"],
                columns="bank",
                values="value",
            )
            .reset_index()
            .sort_values(by=["entry_type", "transaction_type"])
            .fillna(0)
        )

        entradas = df.loc[df["entry_type"] == "entrada", :].copy()
        entradas.loc["Total"] = entradas.sum()
        entradas.loc["Total", "entry_type"] = None
        entradas.loc["Total", "transaction_type"] = None
        entradas.loc["Total", "category"] = None
        print(entradas.to_markdown())

        print("\n\n\n")
        saidas = df.loc[df["entry_type"] != "entrada", :].copy()
        saidas.loc["Total"] = saidas.sum()
        saidas.loc["Total", "entry_type"] = None
        saidas.loc["Total", "transaction_type"] = None
        saidas.loc["Total", "category"] = None
        print(saidas.to_markdown())

        print(
            (
                entradas.loc["Total"].fillna(0) - saidas.loc["Total"].fillna(0)
            ).to_markdown()
        )


def entradas():
    """
    See all money that came in
    """
    db = Database.from_default()

    print()

    print(
        (
            " Entradas regras: "
            + "\n\t 1. Inter não deve ter pix na categoria entrada (pois não recebemos $ no pix) "
            + "\n\t\t em geral inter é recebimento, transferencia de outro banco ou estorno "
            + "\n\t 2. Pix - rejeitado deve ser categoria ignorar"
        )
    )
    print("\n\n\n")
    with db:
        last_day = last_day_of_month(1)
        first_day = first_day_of_month(1)

        statement = (
            select(
                func.sum(Transactions.value),
                Transactions.bank,
                Transactions.category,
            )
            .where(Transactions.category != "transferencia")
            .where(Transactions.entry_type == "entrada")
            .where(Transactions.bank != "rede")
            .where(Transactions.date >= first_day)
            .where(Transactions.date <= last_day)
            .group_by(
                Transactions.bank,
                Transactions.category,
            )
            .order_by(Transactions.value)
        )
        transactions = db.exec(statement).all()

        # Convert list of balances to pandas dataframe
        df = pd.DataFrame(transactions, columns=["value", "bank", "category"])

        # Conver value column to float
        df["value"] = df["value"].astype(float)

        df = df.pivot(
            index=["category"],
            columns="bank",
            values="value",
        ).fillna(0)

        transactions = df
        transactions.loc["Total"] = transactions.sum()
        print(transactions.to_markdown())

        print(f"\n\nTotal : {transactions.loc['Total'].sum()}")


def saidas():
    """
    See all money that we paid out
    """
    db = Database.from_default()

    print()

    with db:
        last_day = last_day_of_month(1)
        first_day = first_day_of_month(1)

        statement = (
            select(
                func.sum(Transactions.value),
                Transactions.bank,
                Transactions.category,
            )
            .where(Transactions.category != "transferencia")
            .where(Transactions.entry_type == "saida")
            .where(Transactions.bank != "rede")
            .where(Transactions.date >= first_day)
            .where(Transactions.date <= last_day)
            .group_by(
                Transactions.bank,
                Transactions.category,
            )
            .order_by(Transactions.value)
        )
        transactions = db.exec(statement).all()

        # Convert list of balances to pandas dataframe
        df = pd.DataFrame(transactions, columns=["value", "bank", "category"])

        # Conver value column to float
        df["value"] = df["value"].astype(float)

        df = df.pivot(
            index=["category"],
            columns="bank",
            values="value",
        ).fillna(0)

        transactions = df
        transactions.loc["Total"] = transactions.sum()
        print(transactions.to_markdown(floatfmt=",.2f"))

        print(f"Total : {transactions.loc['Total'].sum()}")


def fornecedores():
    """
    See all money categorized by fornecedor
    """
    db = Database.from_default()

    print()

    with db:
        last_day = last_day_of_month(1)
        first_day = first_day_of_month(1)

        statement = (
            select(
                func.sum(Transactions.value),
                Transactions.counterpart_name,
            )
            .where(Transactions.category != "transferencia")
            .where(Transactions.entry_type == "saida")
            .where(Transactions.bank != "rede")
            .where(Transactions.date >= first_day)
            .where(Transactions.date <= last_day)
            .group_by(
                Transactions.counterpart_name,
            )
            .order_by(Transactions.value)
        )
        transactions = db.exec(statement).all()

        # Convert list of balances to pandas dataframe
        df = pd.DataFrame(transactions, columns=["value", "fornecedor"])

        # Conver value column to float
        df["value"] = df["value"].astype(float)

        df.sort_values(by="value", inplace=True)
        print(df.to_markdown(floatfmt=",.2f"))
