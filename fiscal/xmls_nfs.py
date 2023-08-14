import re
import zipfile
from datetime import datetime

from pydantic import BaseModel, Field

from fiscal.db import Companies, Company_Naming, Database, NFEs

NF_VALUE = re.compile(r"<vNF>(.*)</vNF>")
NF_TOTAL = re.compile(r"<total>.*<vProd>(.*)</vProd>.*\<\/total>")
NF_CODE = re.compile(r"<infNFe.*Id=\"NFe(.*?)\">")
NF_DATE = re.compile(r"<dhEmi>(.*?)</dhEmi>")
NF_NAME = re.compile(r"<xNome>(.*?)</xNome>.*\<\/emit\>")
NF_CNPJ = re.compile(r"<CNPJ>(.*?)</CNPJ>.*\<\/emit\>")
NF_PROD = re.compile(r"<xProd>(.*?)</xProd>")


class XML_NFEs(BaseModel):
    codigo_acesso: str = Field(default=None)
    emissor: str
    cnpj_emissor: str
    dt_emissao: datetime
    valor_liquido: str
    valor_total: str
    description: str = Field(default="")

    class Config:
        anystr_lower = True


def update_nfes(path: str = "resources/nfs_jungo.zip") -> None:
    """Atualiza as notas fiscais no banco de dados"""

    # Read XML from zipfile
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
                        codigo_acesso=NF_CODE.search(content).group(1),
                        dt_emissao=NF_DATE.search(content).group(1),
                        valor_total=NF_TOTAL.search(content).group(1),
                        valor_liquido=NF_VALUE.search(content).group(1),
                        emissor=NF_NAME.search(content).group(1),
                        cnpj_emissor=NF_CNPJ.search(content).group(1),
                        description=",".join(re.findall(NF_PROD, content)),
                    )
                )

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

        for row in xmls:
            codigo_acesso = row.codigo_acesso
            cnpj = row.cnpj_emissor
            name = row.emissor.lower()
            date = row.dt_emissao

            if codigo_acesso in codigos:
                continue

            print(f"{name} - {cnpj}")
            if name in company_by_nickname:
                if cnpj not in company_by_cnpj:
                    match_cnpj = company_by_nickname[name]
                    print(f"{name} has same cnpj as {cnpj} but not like {match_cnpj}")
                    raise ValueError(
                        f"{name} has same cnpj as {cnpj} but like {match_cnpj}"
                    )
            elif cnpj in company_by_cnpj:
                print("!!!!!")
                db.add(Company_Naming(nickname=name, name=company_by_cnpj[cnpj].name))
                company_by_nickname[name] = company_by_cnpj[cnpj]
            else:
                print("??????????")
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
                )
            )

            db.commit()


update_nfes()
