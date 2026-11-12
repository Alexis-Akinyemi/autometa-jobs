# autometa-jobs — Architecture

## Objet

Infrastructure auto-hébergée pour faire tourner des agents Claude autonomes sur Scaleway, authentifiés contre un abonnement Claude Max via OAuth (pas API key). Mono-locataire. Déclenchement par HTTP ou cron, exécution dans un container sandboxé éphémère jusqu'à 24h, livraison d'un seul artefact dans S3.

## Composants

- **Orchestrateur** — Serverless Container (`min-scale=1`), FastAPI. Détient l'API, le pas de dispatch, le pas de réconciliation, l'accès aux secrets. Seul composant qui détient le token OAuth en mémoire (brièvement, au moment du dispatch).
- **Worker** — Job Scaleway. Image container unique, neuve à chaque run. Lance la CLI `claude` directement (pas de SDK), parse la sortie stream-json, remonte les events à l'orchestrateur via callbacks signés HMAC.
- **Postgres** (Managed DB, instance partagée `proto-db`) — `pipelines`, `runs`, `run_events`, `schedules`.
- **Object Storage** — bucket `pipometa`. Bundles d'entrée, artefacts de sortie.
- **Secret Manager** — token OAuth, clé API, mot de passe DB.
- **Container Registry** (espace partagé `nova-container-registry`) — images du worker et de l'orchestrateur.
- **Cockpit** — logs et métriques, accessibles via Grafana.
- **Container cron** — `pipometa-orchestrator-tick` tire `POST /admin/tick` toutes les minutes, pilote dispatch + réconciliation.

Tout en `fr-par`. Projet : `nova` (`<scaleway-project-id>`).

## Modèle de données (Postgres)

- `pipelines(id, name, system_prompt, config_jsonb)` — `config_jsonb` porte `{scaleway_job_definition_id, allowed_tools, max_turns, model, agents, ...}`. `agents` (optionnel) déclare des sous-agents Claude natifs, voir [Sous-agents natifs](#sous-agents-natifs).
- `runs(id, pipeline_id, status, scaleway_job_run_id, input_uri, output_uri, summary, hmac_key, last_heartbeat_at, idempotency_key, ...)`. Statut : `queued | starting | running | completed | failed | cancelled | timed_out | quota_blocked`.
- `run_events(id, run_id, seq, event_type, payload_jsonb)` — journal append-only de la session.
- `schedules(id, pipeline_id, cron, next_run_at, enabled)` — réservée pour la v2. Aujourd'hui, la récurrence se fait avec des Container crons Scaleway qui tapent sur `POST /pipelines/:id/runs`.

Migrations : Alembic. Sources dans `orchestrator/alembic/versions/`, première révision `0001_initial.py`. `alembic/env.py` réutilise `Base.metadata` côté `models.py` et lit `PIPOMETA_DATABASE_URL` (avec normalisation du driver asyncpg → psycopg pour le DDL synchrone).

## Cycle de vie d'un run

1. Un trigger arrive sur `POST /pipelines/:id/runs` → une ligne `runs` est insérée avec `status='queued'`. L'endpoint déclenche aussi un `_dispatch_one()` best-effort en ligne pour que les triggers sensibles à la latence n'attendent pas le cron.
2. **Pas de dispatch** (`dispatch.py:_dispatch_one`, advisory-lock, concurrence 1) : prend un run en file, génère un HMAC par run, récupère le token OAuth depuis Secret Manager, construit l'env du worker, appelle Scaleway Jobs `start`. Met le run à `running` avec le `scaleway_job_run_id` retourné.
3. Le worker boote, matérialise `~/.claude.json` (bypass de l'onboarding), lit l'entrée du run s'il y en a une (voir ci-dessous), lance la CLI avec le system prompt et la config du pipeline, parse les events stream-json.
4. Le worker pousse les events sur `POST /runs/:id/events` (signés HMAC) et un heartbeat toutes les 30s sur `PUT /runs/:id/heartbeat`.
5. Le worker upload l'artefact sur `s3://pipometa/runs/AAAA/MM/JJ/<pipeline>/<run_id>/output.md`, appelle `PUT /runs/:id/result`, sort de `main()`, le container s'éteint.
6. **Pas de réconciliation** (`reconcile.py:_reconcile_once`) : pour tout run en `running`/`starting` sans heartbeat depuis >90s, interroge Scaleway Jobs et, si l'état y est terminal, met le run local en accord. C'est le filet de sécurité qui rattrape les workers sortis sans avoir reporté.

**Contrat d'entrée (`input_uri`).** Optionnel. Deux schémas acceptés :

- `s3://bucket/key`, lu avec les clés S3 du worker (donc sur un bucket qu'elles atteignent, en pratique `pipometa`) ;
- `https://...`, téléchargé tel quel, avec une taille et une durée totale plafonnées, et des redirections suivies seulement vers du https. C'est la voie pour un appelant qui garde ses fichiers sur son propre bucket : il passe une URL présignée.

`http://` en clair et tout autre schéma sont refusés dès le déclenchement (422). Le corps est lu en UTF-8 ; si c'est un objet JSON avec une clé `prompt`, sa valeur (une chaîne non vide) devient le prompt, sinon le corps brut est passé tel quel (JSONL compris). Le prompt n'a pas de limite de taille propre : il est transmis à la CLI sur son entrée standard. La query string qui porte la signature ne ressort ni dans les erreurs, qui ne recopient que `schéma://hôte/chemin`, ni dans les réponses de l'API ; elle reste stockée dans `runs.input_uri` et dans l'env du job worker. La colonne `runs.input_uri` est en `text` (migration `0002`) : une URL présignée dépasse vite les 512 caractères de départ.

Dispatch et réconciliation sont tous deux pilotés par le Container cron à la minute (`POST /admin/tick`), plus le dispatch best-effort en ligne à la création du run.

## Authentification

- **Vous → orchestrateur** : API key bearer (`Authorization: Bearer pmk_...`). La valeur vit dans Secret Manager `pipometa-orchestrator-api-key` ; l'orchestrateur la lit dans son propre env (injecté par Scaleway en secret env).
- **Cron → orchestrateur** : secret partagé dans le body de la requête (`{"action":"tick","token":"..."}`), validé contre `PIPOMETA_CRON_SECRET`.
- **Worker → orchestrateur** : HMAC-SHA256 par run sur `run_id || body`. La clé est générée au dispatch, stockée sur la ligne `runs`, injectée dans l'env du worker, et effacée au statut terminal.
- **Orchestrateur → Anthropic** : `CLAUDE_CODE_OAUTH_TOKEN`, lu dans Secret Manager à chaque dispatch, injecté dans l'env du worker au démarrage du Job. `ANTHROPIC_API_KEY` est explicitement mis à vide dans le worker pour que la CLI/le SDK ne puisse pas basculer silencieusement sur de l'auth API-key.

## Concurrence et quota

Dispatch mono-worker par construction — la fenêtre 5h glissante de l'abonnement Max ne tolère pas des runs d'agent en parallèle à côté d'un usage interactif Claude Code. Le worker attrape les 429, émet un event `quota_hit` et sort proprement ; l'orchestrateur passe le run à `quota_blocked`. La v1 ne re-tente pas automatiquement à la prochaine bordure 5h ; c'est un ajout v2.

## Stack

Python 3.12 pour l'orchestrateur et le worker. FastAPI, SQLAlchemy (async, asyncpg), Alembic (sync, psycopg) pour les migrations, Pydantic, `boto3`, et le SDK officiel Scaleway (`scaleway>=2.11`) pour les appels Jobs + Secret Manager. Le worker lance la CLI `claude` directement (pas de `claude-agent-sdk`) et parse sa sortie stream-json. Provisionnement via scripts shell `scw` idempotents dans `infra/`. `jobsctl` est une petite CLI Click.

## Décisions structurantes

Quatre choix non-évidents, chacun avec une histoire derrière :

1. **Jobs Scaleway pour le worker, pas Containers.** Les Jobs sont batch (≤24h, pas de listener HTTP, file intégrée). Les Containers sont HTTP (≤15min par requête). Les runs longs ont besoin du premier.
2. **Dispatch piloté par cron, pas par tâche asyncio d'arrière-plan.** Les Serverless Containers gèlent le CPU entre requêtes ; les tâches d'arrière-plan ne sont pas fiables en `min-scale=1`. Un Container cron Scaleway à 1 minute pilote `POST /admin/tick`, qui exécute un pas de dispatch + un pas de réconciliation. Il y a aussi un dispatch best-effort en ligne à la création du run pour que la latence en régime établi soit sub-seconde.
3. **Le worker passe par la CLI `claude`, pas par `claude-agent-sdk`.** Le SDK ne pipe pas stderr par défaut et tombe sur une chaîne en dur `"Check stderr output for details"` dans son `ProcessError`. Lancer la CLI directement avec `--output-format stream-json` est plus simple, transparent, et reprend le pattern utilisé en prod par autometa/matometa.
4. **Le worker tourne en non-root.** La CLI refuse `--dangerously-skip-permissions` quand elle tourne en root, et la bonne réponse est de retirer le privilège, pas le flag — gVisor au niveau du container est la frontière de sécurité ; les prompts de permission par outil dans une sandbox éphémère ne seraient que de la friction sans valeur.

## Sous-agents natifs

Un pipeline peut déclarer des sous-agents Claude dans `config.agents`, au format de l'option `--agents` de la CLI. L'agent principal orchestre : il délègue par l'outil `Agent` (anciennement `Task`) une tâche à un sous-agent qui a son propre prompt, son propre contexte, ses propres outils et son propre modèle (Haiku pour extraire ou trier, Sonnet pour raisonner). Le contexte de l'agent principal ne reçoit que le compte rendu final du sous-agent, pas tout son travail intermédiaire. Tout reste dans le container du run, sur l'abonnement Max, sans nouveau secret.

```json
"agents": {
  "extracteur": {"description": "Quand l'utiliser", "prompt": "Son system prompt", "model": "haiku", "tools": ["WebFetch"], "maxTurns": 6}
}
```

- **Clés acceptées** : `description` et `prompt` (obligatoires, non vides), `model` (`haiku | sonnet | opus | inherit`), `tools`, `disallowedTools`, `maxTurns` (entier, 1 à 200). Ce sont des clés lues par la CLI (vérifiées sur la 2.1.322) ; toute autre clé est refusée, parce que la CLI, elle, ignore en silence une clé inconnue (une coquille comme `max_turns` ferait sauter un garde-fou sans bruit).
- **Validation** à la création et à la mise à jour du pipeline (422), puis de nouveau au dispatch, avant de réserver le run (une config peut dater d'avant ou avoir été modifiée en SQL) : noms `[a-z0-9-]` (64 caractères max), 8 sous-agents max, 32 Kio max de JSON compact.
- **Transport** : l'orchestrateur pose `PIPOMETA_AGENTS` (JSON compact) uniquement si `agents` est défini ; le worker ajoute `--agents <json>`. Sans `agents`, l'env et la commande `claude` sont strictement ceux d'avant. Une valeur invalide fait échouer le run avant la lecture de l'entrée et le lancement de la CLI, avec un message clair. Au démarrage de la session, le worker vérifie que la CLI a bien chargé chaque sous-agent déclaré et fait échouer le run sinon. Les noms d'outils d'un sous-agent ne sont pas vérifiés à la création (la CLI ignore en silence un nom inconnu, et sa liste d'outils varie selon la version) : un nom absent de la liste annoncée par la CLI est signalé par un event `agents_warning`, sans faire échouer le run, car certains outils chargés à la demande n'y figurent pas. Le plafond de 32 Kio tient sous les deux limites connues : aucune limite par variable n'est documentée pour les Jobs Scaleway (les Serverless Functions documentent 64 Kio par variable), et Linux plafonne une chaîne d'argv ou d'env à 128 Kio (`MAX_ARG_STRLEN`).
- **Traçabilité** : les messages et résultats d'outil produits dans un sous-agent remontent dans `run_events` avec `parent_tool_use_id` (l'id de l'appel `Agent` qui les a délégués). Le résultat de cet appel n'est qu'un accusé de lancement ; le compte rendu revient à l'agent principal à la fin de la tâche. Les events `system` `task_started` / `task_updated` / `task_notification` gardent `task_id`, `subagent_type`, `status`, `summary`, `usage` (les events de progression ne sont pas stockés) ; l'event `init` garde les sous-agents et outils chargés. Un run avec sous-agents en arrière-plan se clôt sur plusieurs `result` : le `token_usage` du run les cumule et porte `subagent_stats` et `modelUsage` (consommation par modèle).
- **Artefact** : c'est le texte de l'agent principal, pour tout run. Le texte d'un sous-agent n'y entre pas, ni ce que l'agent principal écrit dans le message qui lance un sous-agent ou pendant que des sous-agents tournent.
- **Arrière-plan** : avec la CLI actuelle, un sous-agent est lancé en arrière-plan par défaut (`background: false` n'y change rien) ; l'agent principal reçoit une notification à la fin et reprend la main. En mode `-p`, la CLI attend la fin des sous-agents avant de sortir. L'agent principal a tendance à écrire un message d'attente (« l'analyste travaille encore… ») : le worker l'écarte de l'artefact (voir ci-dessus).

**Garde-fous de coût, et ce qui reste ouvert.** `--max-turns` (le `max_turns` du pipeline) ne borne que l'agent principal. Chaque sous-agent est borné par son propre `maxTurns` et restreint par ses `tools` ; le choix d'un modèle plus léger par tâche est le principal levier. La CLI n'offre pas de borne globale fiable sur le nombre de sous-agents lancés dans un run : `CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION=1` n'a rien plafonné dans nos essais (2.1.322), on ne l'utilise donc pas, et aucun mécanisme maison n'est ajouté. Restent comme bornes externes : le timeout du Job (≤24h), la fenêtre de quota 5h de l'abonnement, et la consigne. Les sous-agents intégrés de la CLI (`general-purpose`, `Explore`, `Plan`) étaient déjà disponibles avant cette option ; `agents` ajoute des sous-agents spécialisés à côté.

**À savoir** : sous `--dangerously-skip-permissions` (mode `bypassPermissions`), `--allowedTools` n'enlève aucun outil à l'agent, il ne fait qu'autoriser sans demande ce qui l'est de toute façon. Restreindre réellement l'agent principal passerait par `--disallowedTools` ou `--tools` ; pour un sous-agent, `tools` restreint bien sa boîte à outils.

## Hors-sujet (v1)

Multi-locataire. Webhooks de complétion (on poll). Registre de serveurs MCP. Packaging de skills. Délégation à de petits modèles externes (relais Ollama et assimilés : hors de ce dépôt, voir l'issue Autometa #292 ; les sous-agents Claude natifs, eux, sont pris en charge, voir ci-dessus). UI web. Reprogrammation au changement de fenêtre quota 5h.

## Coût (estimation, mensuel)

- Container orchestrateur en `min-scale=1`, 250 mvCPU / 512 MiB : ~5 €
- Runs worker : ~0,02 € par run de 30 min au-delà du free tier partagé (200k vCPU-s + 400k GB-s/mois). Gratuit en faible volume.
- Postgres `proto-db` : 0 € marginal (instance pré-existante).
- Object Storage, Secret Manager, Cockpit, Registry : centimes.

**Coût marginal total : ~5–6 €/mois en idle.**

## Modèle de sécurité

Le token OAuth donne les clés de tout l'abonnement — il n'y a pas de révocation par token au-delà d'en regénérer un nouveau, et un token fuité reste valable jusqu'à révocation manuelle ou expiration (~1 an). Trois règles :

1. Le token n'existe que dans Secret Manager, brièvement en mémoire dans l'orchestrateur au moment du dispatch, et comme env injecté sur un Job Scaleway.
2. L'orchestrateur est le seul composant autorisé à lire le secret. Le worker reçoit le token via injection d'env secret par Scaleway ; il ne le demande jamais à l'orchestrateur.
3. **`scw jobs run get` imprime toutes les variables d'environnement en clair, secrets compris.** Ne collez jamais sa sortie. Si vous l'avez fait, faites une rotation.

Pour rotater : `claude setup-token` sur une machine de confiance, puis `infra/02-secrets.sh` pour pousser la nouvelle valeur dans Secret Manager. L'orchestrateur la prend au prochain dispatch (pas besoin de redémarrer). Pour que le token fuité soit effectivement invalidé, **révoquez-le à la main** dans les paramètres de votre compte Anthropic — `setup-token` émet un nouveau token mais n'invalide pas les précédents.
