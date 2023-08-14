from typing import Any

import typer

from fiscal.banco_inter import update_banco_inter
from fiscal.bb import update_bb
from fiscal.itau import update_itau
from fiscal.match import manual_match, match, undo
from fiscal.nfes import update_nfes
from fiscal.rede import update_rede
from fiscal.reports import (
    compare_itau_and_rede,
    diff_balance,
    entradas,
    entradas_e_saidas_por_banco,
    saidas_report,
)


def create_app() -> Any:
    app = typer.Typer()
    app.command("bb")(update_bb)
    app.command("nfe")(update_nfes)
    app.command("rede")(update_rede)
    app.command("itau")(update_itau)
    app.command("inter")(update_banco_inter)
    app.command("match")(match)

    report_app = typer.Typer()
    report_app.command("balances")(diff_balance)
    report_app.command("transactions")(saidas_report)
    report_app.command("consolidado")(entradas_e_saidas_por_banco)
    report_app.command("vendas")(compare_itau_and_rede)
    report_app.command("entradas")(entradas)
    app.add_typer(report_app, name="report")

    transaction_app = typer.Typer()
    transaction_app.command("add")(manual_match)
    transaction_app.command("undo")(undo)
    app.add_typer(transaction_app, name="transaction")

    return app


if __name__ == "__main__":
    create_app()()
