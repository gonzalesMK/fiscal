import re
import zipfile
from datetime import datetime

from pydantic import BaseModel, Field
from tabulate import tabulate

from fiscal.db import Companies, Company_Naming, Database, NFEs, Products

NF_VALUE = re.compile(r"<vNF>(.*)</vNF>")
NF_TOTAL = re.compile(r"<total>.*<vProd>(.*)</vProd>.*\<\/total>")
NF_CODE = re.compile(r"<infNFe.*Id=\"NFe([0-9]*).*\">")
NF_DATE = re.compile(r"<dhEmi>(.*?)</dhEmi>")
NF_NAME = re.compile(r"<xNome>(.*?)</xNome>.*\<\/emit\>")
NF_CNPJ = re.compile(r"<CNPJ>(.*?)</CNPJ>.*\<\/emit\>")
NF_PROD = re.compile(r"<xProd>(.*?)</xProd>")

PROD_VALUES = re.compile(r"<prod>(.*?)</prod>")
PROD_VALUE = re.compile(r"<vProd>(.*?)</vProd>")
PROD_QTD = re.compile(r"<qCom>(.*?)</qCom>")
PROD_UNIT_VALUE = re.compile(r"<vUnCom>(.*?)</vUnCom>")
PROD_UNIT_TYPE = re.compile(r"<uCom>(.*?)</uCom>")
PROD_NAME = re.compile(r"<xProd>(.*?)</xProd>")


class XML_Produtos(BaseModel):
    codigo_acesso: str = Field(default=None)
    nome: str
    valor_unitario: str
    valor_total: str
    quantidade: str
    unidade: str


class XML_NFEs(BaseModel):
    codigo_acesso: str = Field(default=None)
    emissor: str
    cnpj_emissor: str
    dt_emissao: datetime
    valor_liquido: str
    valor_total: str
    description: str = Field(default="")
    produtos: list[XML_Produtos] = Field(default_factory=list)

    class Config:
        anystr_lower = True


def _get_group(matcher: re.Match[str] | None):
    if not matcher:
        raise ValueError("Matcher is None")
    return matcher.group(1)


def _get_produtos(content: str) -> list[XML_Produtos]:
    prod_content = PROD_VALUES.findall(content)

    produtos = []
    for prod in prod_content:
        produtos.append(
            XML_Produtos(
                nome=_get_group(PROD_NAME.search(prod)),
                valor_unitario=_get_group(PROD_UNIT_VALUE.search(prod)),
                valor_total=_get_group(PROD_VALUE.search(prod)),
                quantidade=_get_group(PROD_QTD.search(prod)),
                unidade=_get_group(PROD_UNIT_TYPE.search(prod)),
            )
        )

    return produtos


def _get_nfes(path: str) -> list[XML_NFEs]:
    xmls = []
    with zipfile.ZipFile(path) as zip_ref:
        files = zip_ref.namelist()
        for file in files:
            with zip_ref.open(file) as xml:
                content = str(xml.read())

                if "-in" in file:
                    continue
                if "cce" in file:
                    continue

                xmls.append(
                    XML_NFEs(
                        codigo_acesso=_get_group(NF_CODE.search(content)),
                        dt_emissao=_get_group(NF_DATE.search(content)),
                        valor_total=_get_group(NF_TOTAL.search(content)),
                        valor_liquido=_get_group(NF_VALUE.search(content)),
                        emissor=_get_group(NF_NAME.search(content)),
                        cnpj_emissor=_get_group(NF_CNPJ.search(content)),
                        description=",".join(re.findall(NF_PROD, content)),
                        produtos=_get_produtos(content),
                    )
                )

    print(
        tabulate(
            [
                {
                    k: v
                    for k, v in x.dict().items()
                    if k not in ["description", "produtos"]
                }
                for x in xmls
            ],
            headers="keys",
            tablefmt="psql",
        )
    )
    return xmls


def update_nfes(path: str = "resources/nfs_abril.zip") -> None:
    """Atualiza as notas fiscais no banco de dados"""

    nfes = _get_nfes(path)

    # Read XML from zipfile
    db = Database.from_default()
    with db:
        company_by_name = {company.name: company for company in db.get_companies()}
        company_by_cnpj = {
            company.cnpj: company for company in company_by_name.values()
        }
        company_by_nickname = {
            company.nickname: company_by_name[company.name]
            for company in db.get_company_names()
        }
        codigos = {nfe.codigo_acesso for nfe in db.get_nfes()}

        for row in nfes:
            codigo_acesso = row.codigo_acesso
            cnpj = row.cnpj_emissor
            name = row.emissor.lower()
            date = row.dt_emissao

            if codigo_acesso in codigos:
                continue

            print(f"{name} - {cnpj}")
            if name in company_by_nickname:
                # Name of the company already exists
                if cnpj not in company_by_cnpj:
                    # Protect against same name but other CNPJ
                    match_cnpj = company_by_nickname[name]
                    print(f"{name} has same cnpj as {cnpj} but not like {match_cnpj}")
                    raise ValueError(
                        f"{name} has same cnpj as {cnpj} but like {match_cnpj}"
                    )
            elif cnpj in company_by_cnpj:
                ## Add company naming for existing cnpj
                db.add(Company_Naming(nickname=name, name=company_by_cnpj[cnpj].name))
                company_by_nickname[name] = company_by_cnpj[cnpj]
            else:
                # add cnpj
                company = db.add(Companies(name=name, cnpj=cnpj, default_category=""))
                db.add(Company_Naming(nickname=name, name=name))
                company_by_cnpj[cnpj] = company
                company_by_nickname[name] = company

            db.add(
                NFEs(
                    codigo_acesso=codigo_acesso,
                    dt_emissao=date,
                    valor_total=row.valor_total,
                    valor_liquido=row.valor_total,
                    emissor=name,
                    produtos=[
                        Products(
                            codigo_acesso=codigo_acesso,
                            name=p.nome,
                            unit_value=p.valor_unitario,
                            total_value=p.valor_total,
                            quantity=p.quantidade,
                            dt_emissao=date,
                            unity=p.unidade,
                        )
                        for p in row.produtos
                    ],
                )
            )

            db.commit()


update_nfes()
