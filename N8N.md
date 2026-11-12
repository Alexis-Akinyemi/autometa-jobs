# Utiliser autometa-jobs depuis n8n

autometa-jobs expose une API HTTP simple (déclencher un run, suivre son état, lire l'artefact). n8n en est un client comme un autre : quelques nœuds **HTTP Request** suffisent, aucun nœud communautaire n'est nécessaire. Ce document décrit les recettes d'intégration.

## Authentification

n8n dispose de sa propre clé API (secret Scaleway `pipometa-n8n-api-key`), distincte de la clé principale. Elle se révoque indépendamment : il suffit de la retirer de `PIPOMETA_EXTRA_API_KEYS` sur le container orchestrateur.

Dans n8n, créez un credential **Header Auth** :

- **Name** : `Authorization`
- **Value** : `Bearer pmk_n8n_...`

et attachez-le à chaque nœud HTTP Request ci-dessous. Ne collez jamais la clé en dur dans les paramètres d'un nœud — les workflows n8n s'exportent en JSON et la clé partirait avec.

L'URL de base est celle de l'orchestrateur (voir `RESOURCES.local.md` / `.env.local`, variable `PIPOMETA_URL`). Stockez-la dans une variable n8n (`$vars.PIPOMETA_URL`) plutôt que de la répéter.

## Recette de base : déclencher, attendre, récupérer

Il n'y a **pas de webhook de complétion** en v1 : n8n doit poller. Le motif standard est une boucle Wait → GET → IF.

```
[Trigger] → [HTTP: POST run] → [Wait 30s] → [HTTP: GET run] → [IF terminal ?]
                                    ▲                               │ non
                                    └───────────────────────────────┘
                                                                    │ oui
                                                       [HTTP: GET output] → [suite…]
```

### 1. Déclencher le run

- **Méthode** : `POST`
- **URL** : `{{$vars.PIPOMETA_URL}}/pipelines/<pipeline_id>/runs`
- **Body** (JSON) : `{}` ou `{"input_uri": "s3://pipometa/inputs/.../in.json"}`

La réponse contient l'`id` du run. Un dispatch best-effort a lieu dans la même requête : le run passe généralement à `running` en moins d'une seconde.

### 2. Poller jusqu'à l'état terminal

- **Wait** : 30 à 60 secondes entre deux sondages. Un run d'agent dure de quelques minutes à plusieurs heures ; poller plus vite n'apporte rien.
- **GET** `{{$vars.PIPOMETA_URL}}/runs/{{$json.id}}`
- **IF** : le champ `status` est terminal s'il vaut `completed`, `failed`, `cancelled`, `timed_out` ou `quota_blocked`. Sinon (`queued`, `starting`, `running`), reboucler sur le Wait.

Prévoyez un garde-fou de boucle (compteur d'itérations ou nœud Wait avec « Limit ») : un run peut durer jusqu'à 24 h, et un workflow n8n qui poll indéfiniment consomme une exécution active.

### 3. Récupérer le résultat

Deux options selon la taille :

- **`summary`** sur le run lui-même (~280 premiers caractères) — suffisant pour une notification.
- **GET** `{{$vars.PIPOMETA_URL}}/runs/{{$json.id}}/output` — l'artefact complet, servi avec le bon content-type (`output.md`, `output.csv`, `output.json`… selon l'`output_format` du pipeline). Dans le nœud HTTP Request, activez « Response Format: File » pour un CSV à passer à un nœud Spreadsheet, ou laissez en texte pour du Markdown.
- **GET** `.../output?presign=1` renvoie `{"url", "expires_in"}` — une URL S3 signée de courte durée, pratique pour insérer un lien de téléchargement dans un mail ou un message Slack sans faire transiter le contenu par n8n.

En cas de `failed`, le champ `error_text` du run donne la cause ; `GET /runs/:id/events` fournit le journal détaillé de la session.

## Autres recettes

### Planifier des récurrences

Le nœud **Schedule Trigger** de n8n peut remplacer un Container cron Scaleway pour les pipelines récurrents : cron n8n → POST `/pipelines/<id>/runs`. C'est la voie recommandée dès que la récurrence doit être suivie d'un post-traitement (diffusion du rapport, écriture dans un tableur, etc.) — tout vit dans le même workflow.

### Fournir un input

Pour passer des données au run, uploadez d'abord un fichier sur `s3://pipometa/inputs/<pipeline>/...` (nœud S3 de n8n, endpoint `https://s3.fr-par.scw.cloud`) puis passez son URI dans `input_uri` au déclenchement. Si le fichier vit sur un autre bucket, passez plutôt une URL `https://` présignée (le worker la télécharge telle quelle ; `http://` est refusé). Le worker le lit en début de session.

### Annuler

`POST /runs/:id/cancel` — utile derrière un nœud n8n de type « approbation humaine » ou un timeout métier.

### Créer / ajuster des pipelines à la volée

`POST /pipelines` et `PATCH /pipelines/:id` sont accessibles avec la même clé. Un workflow n8n peut donc générer un `system_prompt` (par exemple à partir d'un formulaire) et créer le pipeline avant de le déclencher. À réserver aux cas où le prompt varie vraiment ; sinon, un pipeline stable + `input_uri` est plus simple.

## Contraintes à garder en tête

- **Concurrence = 1.** Un seul run à la fois côté orchestrateur (limite de la fenêtre 5 h de l'abonnement Max). Les runs supplémentaires restent en `queued` et partent dès que la voie se libère — n8n peut en empiler, mais ils s'exécutent en série. Ne déclenchez pas des dizaines de runs en rafale.
- **`quota_blocked`** est un état terminal : le quota Claude Max était épuisé. Le workflow n8n doit le traiter (retenter plus tard, alerter), pas le confondre avec `failed`.
- **Sortie plafonnée à ~32k tokens par réponse.** Pour de gros livrables, c'est le `system_prompt` du pipeline qui doit imposer une production par lots — voir [README § Gros livrables](README.md#gros-livrables--produire-par-lots-limite-de-tokens). n8n n'y peut rien côté client.
- **Polling only.** Pas de webhooks de complétion en v1 ; n'essayez pas d'exposer un webhook n8n en callback, l'orchestrateur ne saura pas l'appeler.
