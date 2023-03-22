
migrate:
    liquibase update \
        --url  "jdbc:sqlite:fiscal.db?foreign_keys=1" \
        --changelog-file "db/db.changelog-master.yaml"

rollback:
    liquibase rollback \
        --url  jdbc:sqlite:fiscal.db \
        --changelog-file "db/db.changelog-master.yaml"
