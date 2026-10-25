# Changelog

Tutte le modifiche rilevanti dello SDK saranno documentate in questo file.

## [Unreleased]

## [0.4.2] - 2026-10-25

### Changed

- **Sorgente pubblico a ogni rilascio.** Ogni versione stabile pubblica il codice dello SDK su `bussolabs/closeyourit-python-community`, dopo il controllo di credenziali e indirizzi interni. (CYRA-1054)

## [0.4.1] - 2026-10-07

### Fixed

- **Contesti degli errori mascherati.** I dati passati a `capture_exception` con `contexts` seguono lo stesso filtro delle chiavi sensibili del resto del payload, anche se annidati. (CYPY-16)
- **Avviso per endpoint http remoto.** Fuori da production un endpoint `http://` su un host diverso da localhost produce un avviso sul logger `closeyourit`; production continua a rifiutarlo. (CYPY-16)

## [0.4.0] - 2026-10-04

### Added

- **Query Django osservabili su richiesta.** Monitoraggio opt-in di query lente, N+1 e conteggi, con isolamento sync/async, streaming e rimozione reversibile degli hook. (CYRA-970)
- **Adapter FastAPI e Starlette.** Errori, metriche e template di rotta seguono streaming e background task, con scope isolato e installazione reversibile. Gli input degli errori di validazione non vengono inviati. (CYRA-971)
- **Contesto dei tentativi Celery.** Il contesto job v1 separa durata, attesa e ritardo programmato. `lifecycle_logs`, spento per default, registra gli esiti osservati senza argomenti o risultati. (CYRA-972)

### Fixed

- **Commenti SQL annidati protetti.** Il fingerprint elimina anche i commenti annidati prima di costruire la telemetria, evitando che i loro valori restino visibili.
- **Licenza nel pacchetto.** Wheel e distribuzione sorgente includono la licenza MIT e i relativi metadati. (CYRA-915)

## [0.3.3] - 2026-10-01

### Changed

- **Manutenzione interna.** Commenti e test sono in inglese e i controlli automatici girano senza cache condivisa. Per chi usa la libreria non cambia niente.

## [0.3.2] - 2026-09-14

### Fixed

- **I controlli automatici tornano a girare su ogni proposta di modifica.** Le verifiche erano ferme e le modifiche passavano senza prova. Ora ogni proposta viene controllata prima di essere unita. (CITM-8)

## [0.3.1] - 2026-09-07

### Fixed

- **La credenziale citata in un errore non parte più in chiaro** (CYPY-15): quando un messaggio
  riporta un header di accesso, viene nascosto anche il valore dopo `Bearer`, non solo il tipo di
  autenticazione. Il testo diagnostico attorno resta leggibile.

## [0.3.0] - 2026-09-03

### Added

- **Telemetria d'uso delle funzioni** (CYPY-13): le applicazioni Python compaiono nella pagina d'uso e
  in `cyi usage list`, con rotte e task registrati da soli e `client.used("chiave")` per il resto.
  Spenta per default, si accende con `CLOSEYOURIT_USAGE_ENABLED`.

### Changed

- **La presentazione non promette più funzioni che non ci sono** (CYPY-14): il README dice che i
  quattro client CloseYourIt non fanno le stesse cose e rimanda alla tabella di parità in
  `closeyourit-docs`, rilevazione datata di cosa c'è dove.

## [0.2.2] - 2026-08-24

### Fixed

- La pubblicazione sull'archivio pubblico non viene più rifiutata: il pacchetto dichiara i
  metadati nella forma corrente e lo strumento che li controllava, dentro l'azione di pubblicazione,
  era fermo a una versione che non la conosceva. La 0.2.1 si era fermata lì e non è mai arrivata
  sull'archivio; questa versione la porta con sé.

## [0.2.1] - 2026-08-24

### Fixed

- Il flusso di pubblicazione non produce più un giro muto sul tag di prova (CYPY-12): qui un canale di
  prova non esiste, e la macchina che pubblica si svegliava lo stesso, non eseguiva niente e non
  falliva — dall'esterno identico a un rilascio riuscito. Ora il giro finisce rosso e la riga dice
  quale tag ha ricevuto e che questo repository pubblica soltanto tag stabili.

## [0.2.0] - 2026-08-01

### Added

- Rilevamento automatico della release dalle variabili d'ambiente di deploy/CI (CYPY-10): senza
  `CLOSEYOURIT_RELEASE` impostata a mano le segnalazioni partivano senza release, e i casi già risolti
  riaprivano come regressione. Ora si preferisce il tag semver (`APP_GIT_TAG`, `GIT_TAG`), poi lo short SHA
  da `KAMAL_VERSION`, `GIT_SHA`, `GIT_REVISION`, `SOURCE_VERSION`, `HEROKU_SLUG_COMMIT`. Nessun subprocess
  git. `CLOSEYOURIT_RELEASE` e il valore passato a mano mantengono la precedenza. Allineato a
  CloseYourIt-Ruby.

### Fixed

- L'SDK avvisa nei log quando si disattiva o perde eventi (CYPY-11): configurazione non valida, coda piena
  e spedizioni fallite venivano solo contate in `TransportStats`, che nessun modulo legge — l'SDK diventava
  muto e indistinguibile da "nessun errore". Ora c'è un logger diagnostico condiviso
  (`logging.getLogger("closeyourit")`, lo stesso nome che `CloseYourItHandler` già filtra, quindi mai
  ri-catturato) che emette un avviso una sola volta con il motivo della disattivazione.

## [0.1.0] - 2026-07-13

### Added

- Scaffold Python tipizzato con test, coverage, lint, type check e build del package.
- Pipeline di pubblicazione PyPI tramite Trusted Publishing OIDC, senza token permanenti.
- Configurazione immutabile e fail-safe con validazione delle credenziali server-side.
- Scope isolato per task e thread, snapshot immutabili e scrubbing PII ricorsivo.
- Builder tipizzati per errori, messaggi, log strutturati e metriche performance.
- Client con sampling deterministico, breadcrumb limitati e protocollo di sink sostituibile.
- Transport HTTP asincrono con coda limitata, retry, diagnostica, flush, shutdown e fork safety.
- Handler `logging` e hook reversibili per errori non gestiti di sys, threading e asyncio.
- Contratto golden ingest vendorizzato con schema, fixture, matrice HTTP, lock e checksum.
- Middleware WSGI/ASGI e adapter Django/Flask con scope isolato, streaming e privacy-by-default.
- Integrazioni opzionali e reversibili per Requests, HTTPX sync/async e SQLAlchemy, con URL e SQL
  privati per default, slow/repeated HTTP, query lente, conteggio query e rilevazione N+1.
- Extra Celery con signal reversibili, metriche task/coda e capture fail-safe di failure, retry e
  revoche senza acquisire argomenti o risultati.
