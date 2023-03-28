from datetime import datetime
from decimal import Decimal
from ofxtools.Parser import OFXTree
from sqlalchemy.orm import state


parser = OFXTree()

with open("resources/Extrato-15-03-2023-a-22-03-2023.ofx", "rb") as file:
    parser.parse(file)

ofx = parser.convert()

transactions = [
    tran for statement in ofx.bankmsgsrsv1 for tran in statement.transactions
]

DATE_FORMAT = "%Y-%m-%d"
print(transactions[0].__dict__)
for tran in transactions:
    print(
        f"{tran.trntype} - {tran.dtposted.strftime(DATE_FORMAT)} - {tran.trnamt} - {tran.fitid} - {tran.memo}"
    )


class TransactionTypeOFX:
    PAYMENT = "PAYMENT"
    CREDIT = "CREDIT"


class OfxTransactions:
    transaction_type: TransactionTypeOFX
    dt_posted: datetime
    value: Decimal
    fitid: str
    description: str
