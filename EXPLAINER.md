# autometa-jobs, en clair

autometa-jobs est un petit bout d'infrastructure qui permet de dire :

> « Quand ceci est déclenché, lance un agent Claude autonome dans une sandbox vierge aussi longtemps qu'il faut (jusqu'à 24h), puis dépose le résultat dans S3. »

C'est un équivalent auto-hébergé de Claude Managed Agents, posé sur Scaleway, facturé sur un abonnement Claude Max plutôt que via l'API au token.

## La forme générale, en cinq cases

```
   vous (HTTP, cron, ou jobsctl)
            │
            ▼
   ┌──────────────────────┐
   │   orchestrateur      │   FastAPI sur un Serverless Container.
   │                      │   Détient l'API, l'état des runs en Postgres,
   │                      │   et le token OAuth (en mémoire au moment du dispatch uniquement).
   └──────────┬───────────┘
              │ démarre un Job, injecte l'env
              ▼
   ┌──────────────────────┐
   │   worker             │   Job Scaleway. Container neuf à chaque run,
   │                      │   ≤24h, sandbox gVisor. Lance la CLI
   │                      │   `claude` avec le prompt du pipeline.
   └──────────┬───────────┘
              │ callbacks signés HMAC (events, heartbeat, résultat)
              ▼
   l'orchestrateur enregistre chaque étape → Postgres
   l'artefact final            → S3 (s3://pipometa/runs/...)
```

C'est tout. Cinq composants, chacun fait une seule chose.

## Ce qu'on appelle un « pipeline »

Un pipeline est une ligne en Postgres avec trois choses :

- **un nom** (`hello`, `metabase-weekly`, …)
- **un system prompt** (qui est l'agent, comment il doit se comporter)
- **une config** (max_turns, outils autorisés, modèle, l'identifiant de la job-definition du worker, et en option des sous-agents)

Déclencher un pipeline crée un *run*. Un run a un statut (`queued → running → completed / failed / cancelled / timed_out / quota_blocked`), un journal d'événements en append-only, et exactement un artefact à la fin.

**Sous-agents.** Un pipeline peut déclarer dans sa config (`agents`) des sous-agents Claude natifs : chacun a une description (quand l'utiliser), un prompt, un modèle et ses outils. L'agent principal orchestre et leur délègue des morceaux de travail, par exemple Haiku pour extraire ou trier, Sonnet pour raisonner. Intérêt : le contexte de l'agent principal ne se remplit que des comptes rendus, et chaque tâche tourne sur le modèle qui lui convient. Tout reste dans le container, sur l'abonnement. Détails et limites : [ARCHITECTURE.md § Sous-agents natifs](ARCHITECTURE.md#sous-agents-natifs) ; exemple : `pipelines/subagents-demo.json`.

La même image worker sert tous les pipelines. Le comportement spécifique à chaque pipeline tient uniquement aux variables d'environnement que l'orchestrateur injecte au démarrage du Job. Donc ajouter un nouveau pipeline ne demande pas de rebuilder l'image.

## Ce pour quoi c'est bon

Le système est optimisé pour cette forme :

- **Bursty.** Les runs durent des minutes à des heures, pas en continu. Vous ne payez rien entre deux runs.
- **Mono-objectif par run.** Chaque run consomme une petite entrée structurée (ou aucune) et produit un seul artefact.
- **Auto-suffisant.** Un worker lit ses entrées depuis un bucket (vous y avez déposé un brief, un jeu de questions, une liste d'URLs) ou va chercher lui-même ce qu'il lui faut avec ses outils intégrés (Bash, Read, WebFetch). Pas de service externe précâblé, pas de dépendance MCP — l'ensemble des outils de l'agent est le contrat.
- **Mono-locataire.** L'authentification OAuth contre l'abonnement implique une concurrence à 1 par construction — ce ne sont pas des workers en fan-out, c'est une file avec un seul consommateur qui boit dedans.

Ça tombe bien pour des choses comme :

- un digest Metabase hebdomadaire qui exécute un jeu de requêtes fixe et écrit un mémo
- un job de scrape-et-synthèse périodique (papiers, annonces, articles)
- un agent de maintenance qui passe en SSH sur votre infra et ne vous ping qu'en cas d'anomalie
- une tâche ponctuelle « voici ce dataset, produis ce rapport » qui dure 20 minutes
- la prépa d'une réunion (voici les participants, voici le sujet, écris-moi un brief)

Le motif : petite entrée structurée → agent autonome → un seul artefact que vous lirez vraiment.

## Ce pour quoi ce n'est *pas* bon

- **Charges temps-réel / interactives.** Le démarrage à froid prend 10–30s, le run peut prendre des minutes. Pour le chat, utilisez l'API normale.
- **Forte concurrence.** La boucle de dispatch est mono-worker. Si vous fan-outez, vous vous battez pour le même quota Max.
- **Multi-locataire.** C'est *votre* abonnement. Ne l'exposez pas à d'autres sans passer à de l'auth API-key.
- **Ce qui demande du contrôle à la milliseconde.** C'est un système batch.

## Le mémo pratique

```sh
URL=https://<orchestrator-host>
API_KEY=...           # dans Secret Manager, aussi dans /tmp/pipometa_api_key.txt en local

# 1) Définir un pipeline (une fois)
curl -X POST "$URL/pipelines" -H "Authorization: Bearer $API_KEY" -d '{
  "name": "metabase-weekly",
  "system_prompt": "Tu es analyste DDETS. Pour chaque question Metabase fournie, exécute la requête, lis le résultat, et synthétise un constat.",
  "config": {
    "scaleway_job_definition_id": "<worker-job-definition-id>",
    "max_turns": 20,
    "allowed_tools": ["Bash", "Read", "WebFetch"]
  }
}'

# 2) Déclencher un run
curl -X POST "$URL/pipelines/<id>/runs" -H "Authorization: Bearer $API_KEY" -d '{
  "input_uri": "s3://pipometa/inputs/metabase-weekly/2026-w18.json"
}'
# input_uri accepte aussi une URL https:// (par ex. présignée sur votre propre bucket) ; http:// est refusé
# → renvoie {id: <run_id>, status: "queued"}

# 3) Surveiller
curl "$URL/runs/<run_id>" -H "Authorization: Bearer $API_KEY"
curl "$URL/runs/<run_id>/events" -H "Authorization: Bearer $API_KEY"

# 4) Lire le résultat
# Soit dans runs.summary, soit en récupérant s3://pipometa/runs/AAAA/MM/JJ/<pipeline>/<run_id>/output.md
```

Pour planifier un run récurrent, attachez un Container cron Scaleway à un `POST /pipelines/<id>/runs`, exactement comme le tick de dispatch est câblé aujourd'hui. La table `schedules` en base est réservée à un scheduler interne à l'orchestrateur, qui sera v2.

## Les décisions structurantes

- **OAuth, pas API key.** Le coût marginal par run est nul ; la seule jauge ce sont les fenêtres 5h et hebdomadaire de l'abonnement Max. On échange « scaler à l'argent » contre « scaler au quota ». C'est le bon arbitrage pour un système solo qui fait quelques jobs par jour.
- **Jobs Scaleway pour le worker, pas Containers.** Les Jobs sont conçus pour le batch (≤24h, pas de listener HTTP, file d'attente intégrée). Les Containers sont conçus pour HTTP (≤15min par requête). Les runs longs ont besoin du premier.
- **Dispatch piloté par cron, pas par tâche d'arrière-plan.** Les Serverless Containers gèlent le CPU entre les requêtes ; donc `asyncio.create_task` ne tourne pas en continu. Un Container cron Scaleway à 1 minute pilote `/admin/tick`.
- **Le worker ne passe pas par le SDK.** Il lance la CLI `claude` directement et parse le stream-json. Le SDK avale stderr par défaut et ajoute une couche d'opacité qu'on ne veut pas pour un pipeline non-supervisé.
- **Le worker tourne en non-root.** La CLI refuse `--dangerously-skip-permissions` quand elle tourne en root, et la bonne réponse est de retirer le privilège, pas le flag — gVisor au niveau du container est la vraie frontière de sécurité ; les prompts de permission par outil dans une sandbox éphémère ne seraient que de la friction.
- **Postgres comme file.** Un writer, un consumer, advisory locks. NATS ou Scaleway Messaging serait un système de plus à opérer. À revoir quand on aura besoin de fan-out, ce qui n'est pas le cas aujourd'hui.

## Choses à se rappeler quand on rouvrira ça dans 6 mois

1. **Où vit le token OAuth** : Secret Manager `pipometa-claude-oauth-token`. Rotation via `claude setup-token`, on pousse la nouvelle valeur avec `infra/02-secrets.sh`.
2. **Où se passe le dispatch** : `orchestrator/src/orchestrator/dispatch.py:_dispatch_one`. Un run en file par appel, atomique, advisory-locked.
3. **Ce que le worker lit dans l'env** : `PIPOMETA_RUN_ID`, `PIPOMETA_RUN_HMAC_KEY`, `PIPOMETA_ORCHESTRATOR_URL`, `PIPOMETA_INPUT_URI`, `PIPOMETA_OUTPUT_BUCKET`, `PIPOMETA_SYSTEM_PROMPT`, `PIPOMETA_ALLOWED_TOOLS`, `PIPOMETA_MAX_TURNS`, `PIPOMETA_MODEL`, `PIPOMETA_AGENTS` (seulement si le pipeline déclare des sous-agents), `CLAUDE_CODE_OAUTH_TOKEN`, `AWS_*`. Tout est injecté au démarrage du Job.
4. **Où regarder quand quelque chose ne va pas** : Postgres `runs.error_text` et `run_events` d'abord. Cockpit pour les logs complets du container. Console Scaleway → Jobs → run id pour le code de sortie du worker.
5. **Ce qui coûte de l'argent en continu** : seulement le container de l'orchestrateur (~5 €/mois) et l'instance Postgres existante (déjà payée). Les workers coûtent quelques centimes par run au-delà du free tier.

## Ce qui est volontairement absent

Webhooks, registre de serveurs MCP, packaging de skills, délégation à de petits modèles externes (voir Autometa #292), UI web, multi-locataire, reprogrammation au changement de fenêtre quota. Tout ça est raisonnable comme add-on v2 ; rien ne bloque l'usage actuel.

Si vous vous trouvez à vouloir plus que ça, la bonne suite est probablement (a) un vrai deuxième pipeline qui justifie une nouvelle feature, ou (b) basculer sur Claude Managed Agents tout court, qui a tout ce qui précède. autometa-jobs gagne sa place parce qu'il vous appartient et qu'il tourne sur l'abonnement, pas sur l'API. Si l'abonnement cesse d'être structurant, l'arbitrage build-vs-buy s'inverse.
