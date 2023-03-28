from datetime import datetime
import responses
from fiscal.banco_inter import InterBank
from unittest import TestCase
from pathlib import Path

RESOURCES = Path(__file__).parent / "resources"


class TestInterBankClient(TestCase):
    @classmethod
    def setUpClass(self) -> None:
        self.client = InterBank()
        self.client.bearer_token = "some token"

        self.r_mock = responses.RequestsMock(assert_all_requests_are_fired=True)
        self.r_mock.start()
        self.r_mock._add_from_file(file_path=RESOURCES / "multiple_requests.yaml")

    @classmethod
    def tearDownClass(cls):
        cls.r_mock.stop()

    def test_authenticate(self):
        self.r_mock.post(
            body="""{"access_token": "6cab577f-81d8-409f-8ba8-8e2b4e54ee73","token_type": "Bearer",
      "expires_in": 3600, "scope": "extrato.read"}""",
            url="https://cdpj.partners.bancointer.com.br/oauth/v2/token",
        )

        self.client.authenticate(client_id="client_id", client_secret="client_secret")

        assert self.client.bearer_token == "Bearer 6cab577f-81d8-409f-8ba8-8e2b4e54ee73"

    def test_parse_get_extrato(self):
        ...
        # self.r_mock._add_from_file(file_path=RESOURCES / "get_extrato_refinado.yaml")

        response = self.client.get_transactions(
            start_date=datetime(year=2023, month=3, day=20),
            end_date=datetime(year=2023, month=3, day=23),
        )

        assert response

    def test_parse_get_extrato_2(self):
        response = self.client.get_transactions(
            start_date=datetime(year=2023, month=3, day=17),
            end_date=datetime(year=2023, month=3, day=18),
        )

        assert response

    def test_parse_get_extrato_3(self):
        response = self.client.get_transactions(
            start_date=datetime(year=2023, month=3, day=19),
            end_date=datetime(year=2023, month=3, day=20),
        )

        assert response

    def test_parse_get_extrato_4(self):
        response = self.client.get_transactions(
            start_date=datetime(year=2023, month=1, day=1),
            end_date=datetime(year=2023, month=3, day=22),
        )

        assert response
