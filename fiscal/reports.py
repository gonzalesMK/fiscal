from datetime import date, datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
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


def saidas_report():
    """
    Return the difference in balance for each bank from the end of one month to the end of the next one
    """
    db = Database.from_default()

    print("Total here should match the total in balances")

    with db:
        last_day = last_day_of_month(1)
        first_day = first_day_of_month(1)

        statement = (
            select(Transactions)
            .where(Transactions.category != "transferencia")
            # .where(Transactions.category != "investimentos")
            .where(Transactions.category != "ignorar")
            .where(Transactions.bank != "rede")
            .where(Transactions.date >= first_day)
            .where(Transactions.date <= last_day)
        )

        transactions = db.exec(statement).all()

        # Convert list of balances to pandas dataframe
        df = pd.DataFrame([balance.dict() for balance in transactions])
        df = df[["date", "value", "category"]]

        # fill NA on category empty
        df["category"] = df["category"].fillna("n/a")

        # Conver value column to float
        df["value"] = df["value"].astype(float)

        # Group by category
        df = (
            df.groupby(["category"])
            .sum(numeric_only=True)
            .sort_values(by=["value"], ascending=False)
        )

        # Make negative where transactions are not entrada or transferencia
        mask_saidas = ~df.index.isin(["entrada", "transferencia", "estorno"])
        df.loc[mask_saidas, "value"] = df.loc[mask_saidas, "value"] * -1

        df.sort_values(by=["value"], ascending=False, inplace=True)

        print("All Transactions")

        df.loc["Total"] = df.sum()
        print(tabulate(df, headers="keys", tablefmt="psql"))

        print("DRE")

        # Remove "compras" from the dataframe
        df = df.drop("compras")
        df = df.drop("investimentos")
        df = df.drop("Total")

        df.loc["Total"] = df.sum()
        print(tabulate(df, headers="keys", tablefmt="psql"))


def entradas_e_saidas_por_banco():
    """ """
    print("Consolidado Report")

    db = Database.from_default()

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
        df = df[["date", "value", "entry_type", "bank"]]

        # Conver value column to float
        df["value"] = df["value"].astype(float)

        df_saidas = (
            df.groupby(["bank", "entry_type"])
            .sum(numeric_only=True)
            .sort_values(by=["value"], ascending=False)
            .reset_index()
            .pivot(index="bank", columns="entry_type", values="value")
            .fillna(0)
        )

        # Create a column total with the diff from entrada and saida
        df_saidas["total"] = df_saidas["entrada"] - df_saidas["saida"]

        # Format value to currency string
        df_saidas["entrada"] = df_saidas["entrada"].map(lambda x: f"R$ {x:,.2f}")
        df_saidas["saida"] = df_saidas["saida"].map(lambda x: f"R$ {x:,.2f}")
        df_saidas["total"] = df_saidas["total"].map(lambda x: f"R$ {x:,.2f}")

        # pandas display dataframe pretty
        print(df_saidas.to_markdown())


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
            .where(Transactions.category != "ignorar")
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
            select(Transactions)
            .where(Transactions.category != "ignorar")
            .where(Transactions.category != "transferencia")
            .where(Transactions.entry_type == "entrada")
            .where(Transactions.bank != "rede")
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
            df.groupby(["bank", "transaction_type", "category"])
            .sum(numeric_only=True)
            .sort_values(by=["value"], ascending=False)
            .reset_index()
            .pivot(
                index=["transaction_type", "category"],
                columns="bank",
                values="value",
            )
            .reset_index()
            .sort_values(by=["transaction_type"])
            .fillna(0)
        )

        transactions = df
        transactions.loc["Total"] = transactions.sum()
        transactions.loc["Total", "entry_type"] = None
        transactions.loc["Total", "transaction_type"] = None
        transactions.loc["Total", "category"] = None
        print(transactions.to_markdown())

        print(f"Total : {transactions.loc['Total'].sum()}")
