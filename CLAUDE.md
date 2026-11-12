# CLAUDE.md

Contexte d'onboarding pour les humains comme pour les agents IA qui travaillent dans ce dépôt. Lisez ce fichier en premier ; tout le reste se rejoint depuis ici.

## Ce que c'est

autometa-jobs est un petit système sur Scaleway qui fait tourner des agents Claude autonomes en jobs d'arrière-plan. On définit un *pipeline* (system prompt + config), on déclenche un *run*, et un container sandboxé neuf lance la CLI `claude` pour faire le boulot. Le résultat atterrit dans S3.

C'est **mono-locataire**, authentifié via le **token OAuth Claude Max** (pas l'API). Ça existe parce que le coût marginal d'un run est nul contre l'abonnement, là où l'API mesurerait chaque token.

Pour la visite guidée en langage clair : [EXPLAINER.md](EXPLAINER.md). Pour la spec système : [ARCHITECTURE.md](ARCHITECTURE.md). Pour les identifiants d'infra vivants : [RESOURCES.md](RESOURCES.md).

## La forme générale, en cinq cases

```
   vous (curl, jobsctl, cron)
            │
            ▼
   ┌──────────────────────┐
   │   orchestrateur      │   FastAPI sur un Serverless Container.
   │                      │   Détient l'API, l'état des runs en Postgres,
   │                      │   lit l'OAuth depuis Secret Manager au dispatch.
   └──────────┬───────────┘
              │ démarre un Job, injecte l'env
              ▼
   ┌──────────────────────┐
   │   worker             │   Job Scaleway. Container neuf par run,
   │                      │   ≤24h, sandbox gVisor, tourne en non-root.
   │                      │   Lance la CLI `claude` avec le prompt du pipeline.
   └──────────┬───────────┘
              │ callbacks signés HMAC (events, heartbeat, résultat)
              ▼
   l'orchestrateur enregistre chaque event → Postgres
   l'artefact final                       → S3 (s3://pipometa/runs/...)
```

Un Container cron Scaleway tape sur `/admin/tick` de l'orchestrateur toutes les minutes pour piloter les pas de dispatch + réconciliation (les Serverless Containers gèlent le CPU entre les requêtes, donc les tâches asyncio d'arrière-plan ne sont pas fiables — voir [ARCHITECTURE.md § Décisions structurantes](ARCHITECTURE.md#décisions-structurantes)).

## Arborescence

```
autometa-jobs/
├── README.md             # point d'entrée court
├── CLAUDE.md             # ← vous êtes ici
├── EXPLAINER.md          # visite guidée en langage clair
├── ARCHITECTURE.md       # spec système, modèle de données, choix de design
├── RESOURCES.md          # identifiants Scaleway vivants
├── .env.local            # gitignoré — exporte les IDs en variables PIPOMETA_*
├── .env.example          # template de config pour des redéploiements à neuf
│
├── orchestrator/
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── alembic.ini
│   ├── alembic/                # env.py + versions/
│   └── src/orchestrator/
│       ├── main.py             # app FastAPI + lifespan
│       ├── config.py           # pydantic-settings, lit l'env PIPOMETA_*
│       ├── db.py               # session SQLAlchemy async
│       ├── models.py           # pipelines / runs / run_events / schedules
│       ├── schemas.py          # modèles requête/réponse
│       ├── auth.py             # vérification bearer + HMAC par run
│       ├── dispatch.py         # _dispatch_one : queued → starting → running
│       ├── reconcile.py        # _reconcile_once : rattrape les runs bloqués
│       ├── scaleway.py         # wrappers async sur le SDK scaleway (Jobs + Secret Manager)
│       └── routes/
│           ├── pipelines.py    # /pipelines (POST/GET/PATCH), /pipelines/:id/runs
│           ├── runs.py         # /runs/:id, /events, /cancel + callbacks worker
│           └── admin.py        # /admin/tick, POST / (entrées cron)
│
├── worker/
│   ├── Dockerfile              # debian-slim + installeur claude CLI + user non-root
│   ├── pyproject.toml
│   └── src/worker/
│       ├── __main__.py         # entrypoint : bypass onboarding, lance le runner
│       ├── runner.py           # spawn claude, parse stream-json, upload S3
│       └── client.py           # client de callback signé HMAC
│
├── jobsctl/
│   ├── pyproject.toml
│   └── src/jobsctl/cli.py      # jobsctl trigger | status | events | cancel
│
└── infra/
    ├── lib.sh                  # helpers partagés (couleur, confirm, vars)
    ├── 01-bootstrap.sh         # bucket + database
    ├── 02-secrets.sh           # token OAuth + clé API dans Secret Manager
    ├── 03-job-definition.sh    # job definition pipometa-worker
    ├── 04-container.sh         # informatif — recette de déploiement orchestrateur
    └── 05-init-db.sh           # apply schema (psql)
```

## Modèle de données

Trois tables qui comptent (une quatrième, `schedules`, est réservée pour la v2) :

- **`pipelines(id, name, system_prompt, config_jsonb)`** — `config_jsonb` porte `{scaleway_job_definition_id, allowed_tools, max_turns, model, output_format, agents, ...}`. `agents` (optionnel) déclare des sous-agents Claude natifs au format `claude --agents`, validés par `orchestrator/agents.py` et transmis au worker en `PIPOMETA_AGENTS` (cf. [ARCHITECTURE.md § Sous-agents natifs](ARCHITECTURE.md#sous-agents-natifs)). `output_format` (`md` par défaut, ou `csv`/`json`/`txt`) décide du nom et du content-type de l'artefact : le worker écrit `output.<ext>` et l'endpoint `/runs/:id/output` le sert tel quel. Le format ne transforme pas le contenu — c'est au system prompt d'émettre du CSV/JSON propre (cf. composer).
- **`runs(id, pipeline_id, status, scaleway_job_run_id, input_uri, output_uri, summary, hmac_key, last_heartbeat_at, ...)`** — `input_uri` est `s3://bucket/key` ou une URL `https://` (présignée), jamais `http://` ; contrat dans [ARCHITECTURE.md § Cycle de vie](ARCHITECTURE.md#cycle-de-vie-dun-run). Le statut est l'un de `queued | starting | running | completed | failed | cancelled | timed_out | quota_blocked`.
- **`run_events(id, run_id, seq, event_type, payload_jsonb)`** — journal append-only de la session. Le worker y écrit les events au fil de l'eau (started, prompt_loaded, assistant_message, tool_result, result, error, etc.).

Migrations : Alembic, sources dans `orchestrator/alembic/versions/`. Première révision : `0001_initial.py` ; `0002` élargit `runs.input_uri` en `text`. À appliquer avec `infra/05-init-db.sh` (qui lance `alembic upgrade head`).

## Workflows courants

### Préalable : sourcer la config locale

```sh
cd /Users/louije/Development/gip/autometa-jobs
source .env.local
export PIPOMETA_API_KEY=$(scw secret version access $PIPOMETA_SECRET_API_KEY_ID region=$PIPOMETA_REGION -o json \
  | python3 -c "import sys,json,base64; print(base64.b64decode(json.load(sys.stdin)['data']).decode().strip())")
```

(`.env.local` exporte tous les IDs Scaleway en `PIPOMETA_*`. La valeur de l'API key vit dans Secret Manager ; on la tire à la demande.)

### Déclencher un run

```sh
PIPELINE_ID=<pipeline-id>  # le pipeline `hello`
curl -fsS -X POST "$PIPOMETA_URL/pipelines/$PIPELINE_ID/runs" \
  -H "Authorization: Bearer $PIPOMETA_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{}'                       # ou {"input_uri": "s3://pipometa/inputs/.../in.json"}
```

Un dispatch best-effort en ligne se fait dans la même requête, donc un run passe typiquement de `queued` à `running` en moins d'une seconde ; sinon le cron à la minute le rattrape.

### Surveiller un run

```sh
RID=...
curl -fsS "$PIPOMETA_URL/runs/$RID" -H "Authorization: Bearer $PIPOMETA_API_KEY"
curl -fsS "$PIPOMETA_URL/runs/$RID/events" -H "Authorization: Bearer $PIPOMETA_API_KEY"
```

Ou via la CLI : `jobsctl status $RID` / `jobsctl events $RID`.

### Définir un nouveau pipeline

```sh
curl -fsS -X POST "$PIPOMETA_URL/pipelines" \
  -H "Authorization: Bearer $PIPOMETA_API_KEY" -H "Content-Type: application/json" -d "$(cat <<'JSON'
{
  "name": "weekly-brief",
  "system_prompt": "Tu es analyste DDETS. Pour chaque question fournie, exécute la requête, lis le résultat, synthétise un constat.",
  "config": {
    "scaleway_job_definition_id": "<worker-job-definition-id>",
    "max_turns": 20,
    "allowed_tools": ["Bash", "Read", "WebFetch"]
  }
}
JSON
)"
```

L'ID de la job-definition est toujours `$PIPOMETA_JOB_DEFINITION_ID`. Les outils sont les noms standards des outils Claude Code. `allowed_tools` ne retire rien à l'agent : il tourne sous `--dangerously-skip-permissions`, où cette liste ne fait que pré-autoriser ce qui l'est déjà.

⚠️ **Livrable volumineux.** L'artefact est la concaténation des messages texte de l'agent, et **une réponse est plafonnée à ~32k tokens de sortie** ; le disque et les sous-agents ne sont pas capturés. Pour une grosse sortie (longues listes, CSV de centaines de lignes), la consigne doit imposer une production **par lots sur plusieurs tours** (un `echo` Bash entre chaque lot force la clôture du message) — sinon le run échoue avec `response exceeded the output token maximum`. Recette détaillée : [README § Gros livrables](README.md#gros-livrables--produire-par-lots-limite-de-tokens).

### Modifier un pipeline en place

```sh
curl -fsS -X PATCH "$PIPOMETA_URL/pipelines/$PID" \
  -H "Authorization: Bearer $PIPOMETA_API_KEY" -H "Content-Type: application/json" \
  -d '{"system_prompt": "..."}'      # n'importe quel sous-ensemble de name/system_prompt/config
```

### Annuler un run

```sh
curl -fsS -X POST "$PIPOMETA_URL/runs/$RID/cancel" -H "Authorization: Bearer $PIPOMETA_API_KEY"
```

L'orchestrateur envoie un SIGTERM via l'API stop de Scaleway ; le handler du worker positionne un flag d'annulation coopérative et sort proprement.

### Lire l'artefact

Soit via le champ `summary` sur la ligne du run (les ~280 premiers caractères), soit en récupérant l'artefact complet depuis S3 — l'URI est dans `output_uri` sur le run.

```sh
python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='https://s3.fr-par.scw.cloud', region_name='fr-par')
print(s3.get_object(Bucket='pipometa', Key='runs/2026/05/04/hello/.../output.md')['Body'].read().decode())
"
```

## Déployer des changements

**Merger sur `main` déploie.** Le workflow CI (`.github/workflows/ci.yml`), une fois lint/sécurité/docker au vert, construit les images dont les sources ont changé (`orchestrator/`, `worker/`), les pousse en `:latest` et `:<sha>` sur le registry, puis redéploie l'orchestrateur via l'API Scaleway et vérifie `/healthz`. Le worker n'a pas d'étape de déploiement : la job definition tire `:latest` à chaque run. Un `workflow_dispatch` depuis l'onglet Actions rebuild et redéploie tout à la main. Secrets GitHub utilisés : `SCW_SECRET_KEY`, `PIPOMETA_CONTAINER_ID`. Rollback : retagger `:<sha>` en `:latest` puis `scw container container redeploy`.

Les recettes manuelles ci-dessous restent valables en dépannage.

### Image worker

```sh
docker build --platform linux/amd64 \
  -t rg.fr-par.scw.cloud/nova-container-registry/pipometa-worker:latest \
  worker/
docker push rg.fr-par.scw.cloud/nova-container-registry/pipometa-worker:latest
# Le prochain run dispatch tire la nouvelle image automatiquement (la job def pointe sur :latest).
```

### Image orchestrateur

```sh
docker build --platform linux/amd64 \
  -t rg.fr-par.scw.cloud/nova-container-registry/pipometa-orchestrator:latest \
  orchestrator/
docker push rg.fr-par.scw.cloud/nova-container-registry/pipometa-orchestrator:latest
scw container container redeploy $PIPOMETA_CONTAINER_ID region=$PIPOMETA_REGION
```

### Tests

```sh
uv run --locked --directory worker --with pytest-mock pytest -q tests   # ou orchestrator
```

La CI les lance avant tout build ou déploiement.

### Dépendances (lockfiles)

Chaque package (`orchestrator/`, `worker/`, `jobsctl/`) a un `uv.lock` ; les Dockerfiles installent avec `uv sync --locked`, donc l'image embarque exactement les versions verrouillées. Après toute modification de `pyproject.toml` :

```sh
uv lock --directory orchestrator   # ou worker / jobsctl
```

La CI refuse un lockfile désynchronisé (`uv lock --check`). Dependabot ouvre des PRs qui mettent à jour le lockfile ; les merger puis rebuild/push l'image pour que la mise à jour atteigne la prod.

### Schéma DB

Migrations gérées par Alembic (sources : `orchestrator/alembic/versions/`).

```sh
cd orchestrator
# créer une révision après modification de models.py
alembic revision --autogenerate -m "add foo column"
# vérifier le diff puis appliquer
alembic upgrade head
```

`infra/05-init-db.sh` lance `alembic upgrade head` côté ops. Sur une nouvelle base, on part de zéro ; sur une base existante créée hors Alembic, faire `alembic stamp <revision>` une fois pour synchroniser.

Sous le capot, `alembic/env.py` lit `PIPOMETA_DATABASE_URL` et bascule le driver `asyncpg` → `psycopg` (sync) plus le paramètre `ssl=...` → `sslmode=...`, parce que Alembic exécute du DDL synchrone.

### Redimensionner orchestrateur ou worker

```sh
# Orchestrateur (Serverless Container)
scw container container update $PIPOMETA_CONTAINER_ID region=$PIPOMETA_REGION cpu-limit=500 memory-limit=1024
# Worker (Job definition)
scw jobs definition update $PIPOMETA_JOB_DEFINITION_ID region=$PIPOMETA_REGION cpu-limit=2000 memory-limit=4096
```

## Où regarder quand quelque chose ne va pas

Du moins coûteux au plus coûteux à inspecter :

1. **`runs.error_text` et `runs.status`** en Postgres. L'orchestrateur y consigne la cause immédiate de l'échec.
2. **`run_events`** pour ce run. Cherchez les lignes `event_type='error'` ou `claude_error` ; le payload contient en général `cli_stderr`.
3. **Logs Cockpit** pour le run du Job worker. Utilisez `scw jobs run get <job_run_id>` pour le retrouver, puis ouvrez Cockpit Grafana sous "Loki" en filtrant sur le run id.
4. **Logs Cockpit pour le container orchestrateur**. Même chemin, autre label.
5. **`scw jobs run get <job_run_id>`** pour l'état côté Scaleway (`succeeded | failed | interrupted | cancelled`) plus le code de sortie et un éventuel `error_message`.

⚠️ `scw jobs run get` imprime toutes les variables d'environnement en clair, **secrets compris** (token OAuth, clés AWS). Ne collez pas sa sortie n'importe où.

## Conventions et points non-évidents

- **Le token OAuth est lu à chaque dispatch**, pas mis en cache. La rotation est donc instantanée : on écrit une nouvelle version dans Secret Manager et le prochain dispatch la prend. Pas besoin de redémarrer l'orchestrateur.
- **`ANTHROPIC_API_KEY` est explicitement positionné à la chaîne vide** dans l'env du worker. C'est défensif — sans ça, le SDK ou la CLI pourrait basculer silencieusement sur de l'auth API-key et facturer l'API mesurée plutôt que l'abonnement.
- **Le worker tourne sous l'utilisateur `runner` (uid 1000)**, pas root. C'est ce qui autorise `--dangerously-skip-permissions`. Le container est la frontière de sécurité (sandbox gVisor + éphémérité) ; à l'intérieur, l'agent a toute la liberté.
- **`min-scale=1` sur l'orchestrateur ne garde PAS les tâches d'arrière-plan en vie.** Scaleway gèle le CPU entre les requêtes. Dispatch et réconciliation ne tournent que quand une requête HTTP arrive (le cron, le tick best-effort à la création d'un run, ou tout autre appel à `/admin/tick`).
- **La concurrence est hard-codée à 1.** La fenêtre 5h glissante de l'abonnement Max ne tolère pas des runs d'agent en parallèle à côté d'un usage interactif Claude Code. Si vous augmentez `dispatch_concurrency`, attendez-vous à des 429.
- **La clé HMAC dans `runs.hmac_key` est à usage unique.** Générée au dispatch, effacée au statut terminal. Les callbacks du worker doivent inclure `X-Run-Signature: hex(HMAC-SHA256(key, run_id || body))`.
- **Alembic gère le schéma.** Sources dans `orchestrator/alembic/versions/`, configuration dans `orchestrator/alembic.ini`. La base prod a été stampée à `0001` après création initiale en SQL brut ; toute évolution depuis passe par `alembic revision --autogenerate` + `alembic upgrade head`.
- **L'accès Scaleway passe par le SDK officiel** (`scaleway>=2.11`), wrappé dans `asyncio.to_thread` côté `orchestrator/scaleway.py`. Pas d'httpx artisanal.
- **La table `schedules` est réservée pour la v2.** Aujourd'hui, les récurrences se font en attachant un Container cron Scaleway à un endpoint `POST /pipelines/<id>/runs`, comme le tick de dispatch.

## Modèle de sécurité (TL;DR)

- Le token OAuth n'existe que dans Secret Manager, brièvement en mémoire dans l'orchestrateur au dispatch, et comme env injecté sur un Job Scaleway (Scaleway le chiffre au repos, le hash dans les réponses API).
- Les callbacks worker → orchestrateur sont HMAC-SHA256, la clé meurt avec le run.
- L'auth utilisateur → orchestrateur est bearer (la clé API depuis Secret Manager `pipometa-orchestrator-api-key`).
- L'auth cron → orchestrateur est un secret partagé dans le body, validé par `PIPOMETA_CRON_SECRET`.
- ⚠️ `scw jobs run get` imprime les variables d'env, **secrets compris**, dans la console. Traitez sa sortie comme sensible.

## Ce qui n'est *pas* là (coupes v1 volontaires)

- Pas de webhooks de complétion de run (poll l'API)
- Pas de registre de serveurs MCP (les workers sont auto-suffisants — lisent l'input depuis S3 ou vont chercher avec leurs outils intégrés)
- Pas de packaging de skills
- Pas de délégation à de petits modèles externes (les sous-agents Claude natifs, eux, existent : `config.agents`)
- Pas d'UI web
- Pas de re-planification au changement de fenêtre quota 5h
- Pas d'accès multi-locataire (mono-locataire est une hypothèse structurante)

## Là où sont les corps

Quelques arêtes vives qui ont mordu pendant la construction :

- **Scaleway Jobs `start` renvoie `{"job_runs": [...]}`**, pas un objet. `orchestrator/src/orchestrator/scaleway.py:start_job` extrait `[0]`.
- **Le SDK Claude Agent ne pipe pas stderr par défaut.** Sa `ProcessError` contient une chaîne en dur `"Check stderr output for details"`. On a abandonné le SDK et on utilise la CLI directement.
- **`--dangerously-skip-permissions` est refusé pour root**, donc le container worker a un utilisateur `runner`.
- **Les Serverless Containers Scaleway gèlent le CPU entre les requêtes**, donc les tâches asyncio d'arrière-plan ne sont pas fiables. Le `/admin/tick` piloté par cron est le contournement.

## Liens utiles

- [EXPLAINER.md](EXPLAINER.md) — visite guidée en langage clair, à partager
- [ARCHITECTURE.md](ARCHITECTURE.md) — spec système, modèle de données, choix de design
- [N8N.md](N8N.md) — recettes d'intégration pour le client n8n (déclencher, poller, récupérer)
- [RESOURCES.md](RESOURCES.md) — IDs Scaleway vivants et ce qu'ils contiennent
- [`.env.local`](.env.local) — env shell gitignoré pour les opérations
- [`.env.example`](.env.example) — template pour redéploiements
- [`infra/`](infra/) — scripts de provisionnement
- [`jobsctl/src/jobsctl/cli.py`](jobsctl/src/jobsctl/cli.py) — CLI opérateur
