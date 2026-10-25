# Ingest contract v1

Bundle canonico, offline e versionato per verificare producer e transport degli SDK CloseYourIt.
`schema.json` usa JSON Schema Draft 2020-12; `fixtures/valid` deve essere accettato, mentre
`fixtures/invalid` descrive payload che un producer deve rifiutare prima dell'invio.

## Semantica

- `additionalProperties` resta consentito per evoluzioni wire additive.
- `event_id` e `sample_id` vengono generati prima del primo tentativo e non cambiano nei retry.
- Timestamp e identificativi delle fixture sono deterministici.
- `__PYTHON_VERSION__` è l'unico placeholder: il consumer Python lo sostituisce con la versione
  runtime prima del confronto golden.
- `fixtures/http/cases.json` descrive classificazione e retry senza effettuare rete reale.
- `fixtures/http/auth-cases.json` separa il bearer server-side dalla chiave pubblica DSN e rende
  esplicito che pageview/replay browser passano da un gateway applicativo fidato.

`manifest.json` elenca ogni fixture e il relativo `$defs`; `SHA256SUMS` protegge lo snapshot
vendorizzato nei consumer. Per aggiornare il bundle bisogna modificare schema/fixture, rigenerare i
checksum e aggiornare la versione del contratto nel consumer.

Eseguire `python scripts/verify_ingest_contract.py` dalla root del repository per verificare schema,
fixture, matrice HTTP/auth e checksum prima di vendorizzare il bundle negli SDK.
