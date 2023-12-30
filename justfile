set dotenv-load

migrate:
    liquibase update \
        --url  "jdbc:sqlite:fiscal.db?foreign_keys=1" \
        --changelog-file "db/db.changelog-master.yaml"
rollback:
    liquibase rollback-count \
        --url  jdbc:sqlite:fiscal.db \
        --changelog-file "db/db.changelog-master.yaml" \
        --count 1

xmls FILE:
    python fiscal/main.py xmls {{FILE}}

inter:
    python fiscal/main.py inter

db:
    sqlitebrowser fiscal.db

itau FILE:
    python fiscal/main.py itau {{FILE}}

bb FILE:
    python fiscal/main.py bb {{FILE}}

rede:
    python fiscal/main.py rede

report REPORT="--help":
    python fiscal/main.py report {{REPORT}}

    
backup:
    cp fiscal.db backups/fiscal_$(date +"%Y_%m_%d_%H_%M_%S").db
