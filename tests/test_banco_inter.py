import builtins
import contextlib
from datetime import datetime
import os
from unittest import TestCase

import responses
from sqlmodel import SQLModel, text
from fiscal.banco_inter import INTER_BANK, update_banco_inter

from fiscal.db import (
    Banks,
    Categories,
    Companies,
    Company_Naming,
    Database,
    EntryType,
    Transactions,
    Category,
)
from mockito import when
from tests.test_banco_inter_client import RESOURCES
from freezegun import freeze_time


@contextlib.contextmanager
def mock_context():
    try:
        yield 1
    finally:
        ...


def TRANSACTION_CPFL_1():
    return Transactions(
        transaction_type="PAGAMENTO",
        id=2,
        bank="inter",
        date=datetime(2023, 3, 17, 0, 0),
        entry_type=EntryType.SAIDA,
        category=Category.INSUMOS,
        description="CPFL CIA PAULISTA DE FORCA LUZ",
        value="1513.87",
        counterpart_name="CPFL CIA PAULISTA DE FORCA LUZ",
        validated=False,
        external_id="mdaxxzawmdE5xzi1njuzNDMyMl8yMDIzLTAzLTE3XzkwMDAwMDA1NA==",
    )


def delete_content(client: Database):
    tables = SQLModel.metadata.tables.keys()
    with client.engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys=OFF"))
        for table in tables:
            conn.execute(text(f'DELETE FROM "{table.upper()}"'))
        conn.execute(text("PRAGMA foreign_keys=ON"))


def INSERT_TRANSACTION():
    return Transactions(
        id=None,
        bank=INTER_BANK,
        date=datetime(2023, 3, 17, 0, 0),
        entry_type=EntryType.SAIDA,
        transaction_type="PAGAMENTO",
        category=str(Category.INSUMOS.value),
        description="CPFL CIA PAULISTA DE FORCA LUZ",
        value="521.31",
        counterpart_name="CPFL CIA PAULISTA DE FORCA LUZ",
        validated=False,
        external_id="external_id_example",
    )


def INSERT_TRANSACTION_AMBEV():
    return Transactions(
        id=None,
        bank=INTER_BANK,
        date=datetime(2023, 3, 17, 0, 0),
        entry_type=EntryType.SAIDA,
        transaction_type="PAGAMENTO",
        category=str(Category.INSUMOS.value),
        description="AMBEV",
        value="521.31",
        counterpart_name="AMBEV",
        validated=False,
        external_id="external_id_example",
    )


def SETUP():
    return [Categories(category=str(cat.value)) for cat in list(Category)] + [
        Banks(bank=INTER_BANK, description="inter bank")
    ]


def AMBEV():
    return [
        Companies(
            name="AMBEV",
            cnpj="1234",
            default_category=Category.INSUMOS,
        ),
        Company_Naming(
            name="AMBEV",
            nickname="AMBEV",
        ),
    ]


def CPFL():
    return [
        Companies(
            name="CPFL CIA PAULISTA DE FORCA LUZ",
            cnpj="123",
            default_category=Category.INSUMOS,
        ),
        Company_Naming(
            name="CPFL CIA PAULISTA DE FORCA LUZ",
            nickname="CPFL CIA PAULISTA DE FORCA LUZ",
        ),
    ]


DB_PATH = str((RESOURCES.parent.parent / "fiscal_test.db").absolute())
os.environ["DB_PATH"] = DB_PATH


class TestBancoInter(TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.r_mock = responses.RequestsMock(assert_all_requests_are_fired=True)

    def setUp(self) -> None:
        self.db = Database.from_default()
        self.r_mock.start()

        self.r_mock._add_from_file(file_path=RESOURCES / "inter_authenticate.yaml")
        delete_content(self.db)

        return super().setUp()

    def tearDown(self):
        self.r_mock.stop()
        self.r_mock.reset()
        # delete_content(self.db)

    @freeze_time("2023-03-19")
    def test_fetcher_works_already_exist_companies(self):
        self.r_mock._add_from_file(file_path=RESOURCES / "inter_single_requests.yaml")

        with self.db:
            for model in SETUP() + CPFL():
                self.db.add(model)

            self.db.add(INSERT_TRANSACTION())

        update_banco_inter(client_id="123", client_secret="abc")

        with self.db:
            all_companies = {comp.name: comp for comp in self.db.get_companies()}
            all_namings = {comp.nickname: comp for comp in self.db.get_company_names()}
            all_transactions = {
                trans.external_id: trans
                for trans in self.db.get_transactions(INTER_BANK)
            }

            # Already existing company
            assert "cpfl cia paulista de forca luz" in all_companies
            assert len(all_companies) == 1

            assert "cpfl cia paulista de forca luz" in all_namings
            assert len(all_namings) == 1

            # Default category on transaction
            assert len(all_transactions) == 2
            self.assertDictEqual(
                all_transactions[
                    "mdaXxZaWmde5xZi1nJuZndmYmL8YmdiZltaZlte3xZKWmdaWmda1na=="
                ].dict(),
                TRANSACTION_CPFL_1().dict(),
            )

    @freeze_time("2023-03-19")
    def test_fetcher_works_new_company(self):
        self.r_mock._add_from_file(file_path=RESOURCES / "inter_single_requests.yaml")

        with self.db:
            for model in SETUP() + AMBEV():
                self.db.add(model)

            self.db.add(INSERT_TRANSACTION_AMBEV())
        # when(fetcher).handle_inserts(TRANSACTIOS_MOCKED, []).thenReturn(None)
        when(builtins).input("Which CNPJ to use: ").thenReturn("123456")
        when(builtins).input("Which Name to use: ").thenReturn("")
        when(builtins).input("Which category to use: ").thenReturn("Insumos")

        update_banco_inter(client_id="123", client_secret="abc")

        with self.db:
            all_companies = {comp.name: comp for comp in self.db.get_companies()}
            all_namings = {comp.nickname: comp for comp in self.db.get_company_names()}
            all_transactions = {
                trans.external_id: trans
                for trans in self.db.get_transactions(INTER_BANK)
            }

            # New company with default name (and naming)
            # Assert company created with cnpj
            assert "cpfl cia paulista de forca luz" in all_companies
            assert len(all_companies) == 2
            assert all_companies["cpfl cia paulista de forca luz"].cnpj == "123456"
            assert (
                all_companies["cpfl cia paulista de forca luz"].default_category
                == "Insumos"
            )

            assert "CPFL CIA PAULISTA DE FORCA LUZ" in all_namings
            assert len(all_namings) == 2

            # Default category on transaction
            assert len(all_transactions) == 2
            self.assertDictEqual(
                all_transactions[
                    "mdaXxZaWmde5xZi1nJuZndmYmL8YmdiZltaZlte3xZKWmdaWmda1na=="
                ].dict(),
                TRANSACTION_CPFL_1().dict(),
            )

    @freeze_time("2023-03-19")
    def test_fetcher_works_renaming_company(self):
        self.r_mock._add_from_file(file_path=RESOURCES / "inter_single_requests.yaml")

        with self.db:
            for model in SETUP() + AMBEV():
                self.db.add(model)

            self.db.add(INSERT_TRANSACTION_AMBEV())

        # when(fetcher).handle_inserts(TRANSACTIOS_MOCKED, []).thenReturn(None)
        when(builtins).input("Which CNPJ to use: ").thenReturn("1234")

        update_banco_inter(client_id="123", client_secret="abc")

        with self.db:
            all_companies = {comp.name: comp for comp in self.db.get_companies()}
            all_namings = {comp.nickname: comp for comp in self.db.get_company_names()}
            all_transactions = {
                trans.external_id: trans
                for trans in self.db.get_transactions(INTER_BANK)
            }

            # New naming for an existing company
            assert len(all_companies) == 1

            assert "cpfl cia paulista de forca luz" in all_namings
            assert all_namings["cpfl cia paulista de forca luz"].name == "AMBEV"
            assert len(all_namings) == 2

            # Default category on transaction
            assert len(all_transactions) == 2
            self.assertDictEqual(
                all_transactions[
                    "mDAxXzAwMDE5XzI1NjUzNDMyMl8yMDIzLTAzLTE3XzkwMDAwMDA1NA=="
                ].dict(),
                TRANSACTION_CPFL_1().dict(),
            )

    @freeze_time("2023-03-19")
    def test_fetcher_works_correct_categories(self):
        self.r_mock._add_from_file(file_path=RESOURCES / "inter_categories.yaml")

        with self.db:
            for model in SETUP() + CPFL():
                self.db.add(model)

            self.db.add(INSERT_TRANSACTION())

        update_banco_inter(client_id="123", client_secret="abc")

        with self.db:
            all_companies = {comp.name: comp for comp in self.db.get_companies()}
            all_namings = {comp.nickname: comp for comp in self.db.get_company_names()}
            all_transactions = {
                trans.external_id: trans
                for trans in self.db.get_transactions(INTER_BANK)
            }

            # Already existing company
            assert "CPFL CIA PAULISTA DE FORCA LUZ" in all_companies
            assert len(all_companies) == 1

            assert "CPFL CIA PAULISTA DE FORCA LUZ" in all_namings
            assert len(all_namings) == 1

            assert len(all_transactions) == 3
            # Example of entrada
            # Transaction without company (ENTRADA)
            assert all_transactions["1"].entry_type == "ENTRADA"
            assert all_transactions["1"].category == "Entrada"
            # Example of imposto
            assert all_transactions["2"].transaction_type == "IMPOSTO"
            assert all_transactions["2"].category == "Imposto"

            # Example of ignore type

            # Example of tarifa

        # Example of picking company suggestion

    @freeze_time("2023-03-19")
    def test_fetcher_works_with_repetitive_transaction(self):
        self.r_mock._add_from_file(file_path=RESOURCES / "inter_single_requests.yaml")

        with self.db:
            for model in SETUP() + CPFL():
                self.db.add(model)

            self.db.add(TRANSACTION_CPFL_1())

        update_banco_inter(client_id="123", client_secret="abc")

        with self.db:
            all_companies = {comp.name: comp for comp in self.db.get_companies()}
            all_namings = {comp.nickname: comp for comp in self.db.get_company_names()}
            all_transactions = {
                trans.external_id: trans
                for trans in self.db.get_transactions(INTER_BANK)
            }

            # Already existing company
            assert "cpfl cia paulista de forca luz" in all_companies
            assert len(all_companies) == 1

            assert "cpfl cia paulista de forca luz" in all_namings
            assert len(all_namings) == 1

            # Default category on transaction
            assert len(all_transactions) == 1
            self.assertDictEqual(
                all_transactions["external_id_example"].dict(),
                TRANSACTION_CPFL_1().dict(),
            )
