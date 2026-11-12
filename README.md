# autometa-jobs

Agents autonomes auto-hébergés sur Scaleway. On déclenche un *pipeline* → un container sandboxé éphémère lance l'agent via CLI aussi longtemps que la tâche le demande (jusqu'à 24h) → le résultat atterrit dans S3.

> Mono-locataire par construction. Concurrence 1. Pensé pour un usage solo. Équivalent auto-hébergé de [Claude Managed Agents](https://www.anthropic.com/engineering/claude-managed-agents).

## Concepts fondamentaux

- **Pipeline** — une tâche réutilisable : une consigne (le *system prompt*) + une config (outils autorisés, nombre de tours max…). On le définit une fois.
- **Run** — une exécution d'un pipeline. On en déclenche autant qu'on veut ; chacun tourne dans son propre container, isolé des autres, et produit un artefact.

Un pipeline s'utilise de deux manières, selon que la tâche change ou non d'un run à l'autre :

- **One-shot** — instructions *et* données dans la consigne. Le run est lancé sans entrée. Pour une tâche autonome qu'on relance telle quelle.
- **Réutilisable** — la consigne décrit la *méthode* (stable) ; on fournit une *entrée* différente à chaque run (un fichier sur S3, via `input_uri`). Même pipeline, données qui varient.

## Avant de commencer

Les exemples ci-dessous utilisent `jobsctl`, la CLI fournie dans le dépôt. Un administrateur vous fournit deux valeurs d'accès (l'adresse de l'orchestrateur et une clé). Renseignez-les en haut de [`bin/setup.sh`](bin/setup.sh), puis lancez-le une fois ([uv](https://docs.astral.sh/uv/) requis) :

```sh
./bin/setup.sh
```

Le script stocke les accès dans `~/.config/autometa-jobs/config.env` et installe un lanceur `jobsctl` dans `~/.local/bin`. Plus rien à exporter : `jobsctl` retrouve les accès tout seul à chaque appel.

## Lancer un job

### 1. Voir les pipelines disponibles

```sh
jobsctl pipelines
```

Notez l'`id` du pipeline qui vous intéresse.

### 2. Déclencher un run

```sh
jobsctl trigger <pipeline-id>
```

La sortie contient l'`id` du run ; c'est lui qu'on suit.

> Sans option, le run démarre en mode **one-shot** : la tâche entière est dans la consigne du pipeline. Pour un pipeline **réutilisable** qui attend une entrée, ajoutez `--input-uri s3://pipometa/inputs/.../mon-fichier.json` — ce fichier (un JSON avec une clé `prompt`, sinon son contenu brut) est la donnée que l'agent traite. `input_uri` accepte aussi une URL `https://`, par exemple une URL présignée sur votre propre bucket ; `http://` en clair est refusé.

Le run démarre généralement dans la seconde (`queued` → `running`) ; sinon une tâche de fond le rattrape dans la minute.

### 3. Suivre l'avancement

```sh
jobsctl status <run-id>       # statut, début, fin, résumé
jobsctl events <run-id>       # déroulé détaillé, événement par événement
```

Le champ `status` passe par :

| Statut | Signification |
|--------|---------------|
| `queued` | en attente de démarrage |
| `starting` / `running` | en cours |
| `completed` | terminé avec succès |
| `failed` | échec — voir `error_text` |
| `cancelled` | annulé manuellement |
| `timed_out` | a dépassé la durée max |
| `quota_blocked` | quota momentanément épuisé, à relancer plus tard |

### 4. Lire le résultat

Quand le run est `completed`, `jobsctl status <run-id>` expose :

- `summary` — le début de l'artefact, souvent suffisant.
- `output_uri` — l'adresse S3 de l'artefact complet, à récupérer depuis S3 si vous y avez accès.

## Annuler un run

```sh
jobsctl cancel <run-id>
```

L'arrêt est propre : le run sort dès qu'il peut et passe en `cancelled`.

## Créer ou modifier un pipeline

Un pipeline, c'est un nom, une consigne et une config. La consigne étant souvent longue, le plus simple est de la mettre dans un fichier :

```sh
# consigne.txt contient le system_prompt (voir le template ci-dessous)
jobsctl pipeline-create \
  --name brief-hebdo \
  --system-prompt-file consigne.txt \
  --config '{"scaleway_job_definition_id": "<demandez-cet-id>", "max_turns": 20, "allowed_tools": ["Bash", "Read", "WebFetch"]}'
```

- `--system-prompt` / `--system-prompt-file` — la consigne permanente de l'agent (voir le template ci-dessous).
- `--config` / `--config-file` — la config JSON. `allowed_tools` : des outils pré-autorisés, sans effet restrictif (l'agent tourne sans demande de permission, il a déjà accès à tous ses outils) ; `scaleway_job_definition_id` : identifiant technique constant, demandez-le une fois à qui gère l'infra ; `agents` (optionnel) : des sous-agents Claude natifs, voir ci-dessous.

### Sous-agents (optionnel)

`config.agents` déclare des sous-agents Claude natifs : l'agent principal orchestre et leur délègue des tâches, chacune sur le modèle qui convient (Haiku pour extraire, Sonnet pour raisonner). Format de l'option `--agents` de la CLI :

```json
"agents": {
  "extracteur": {
    "description": "Lit une page web et en extrait les faits utiles. À utiliser pour chaque URL.",
    "prompt": "Tu rends une liste de faits courts, chacun avec sa citation source.",
    "model": "haiku",
    "tools": ["WebFetch"],
    "maxTurns": 6
  }
}
```

`description` et `prompt` sont obligatoires ; `model` vaut `haiku`, `sonnet`, `opus` ou `inherit` ; `tools`, `disallowedTools` et `maxTurns` (entier, 1 à 200) sont optionnels. Toute autre clé est refusée ; un nom d'outil inconnu ne l'est pas, mais le run le signale (event `agents_warning`). 8 sous-agents max, 32 Kio max. `max_turns` du pipeline ne borne que l'agent principal : bornez chaque sous-agent par son `maxTurns`. Les sous-agents tournent en arrière-plan ; ce que l'agent principal écrit en les lançant ou en les attendant est écarté du livrable. Exemple complet : [`pipelines/subagents-demo.json`](pipelines/subagents-demo.json). Détails : [ARCHITECTURE.md § Sous-agents natifs](ARCHITECTURE.md#sous-agents-natifs).

Pour ajuster un pipeline existant, ne passez que ce qui change :

```sh
jobsctl pipeline-update <pipeline-id> --system-prompt-file consigne.txt
```

(`jobsctl pipeline-get <pipeline-id>` affiche un pipeline en détail.)

### Template de consigne (`system_prompt`)

C'est le cœur du pipeline : il dicte le comportement de l'agent à chaque run. Une consigne efficace tient en cinq blocs. Copiez ce squelette et remplissez-le :

```text
RÔLE
Tu es <expertise / posture : ex. analyste DDETS, rédacteur de veille>.

OBJECTIF
À chaque exécution, tu dois produire <le livrable précis, en une phrase>.

ENTRÉE
<one-shot> : les données à traiter sont fournies ci-dessous / sont à aller
chercher avec tes outils.
<réutilisable> : tu reçois une entrée différente à chaque run ; traite-la
telle quelle.

MÉTHODE
1. <première étape concrète>
2. <deuxième étape>
3. <…jusqu'au livrable>

FORMAT DE SORTIE
Rends <la structure attendue : Markdown avec ces sections, un tableau,
un JSON avec ces champs…>. <Longueur, ton, langue.>

CONTRAINTES
- <ce qu'il ne faut surtout pas faire>
- <limites : ne pas inventer de chiffres, citer les sources, etc.>
- Si le livrable est volumineux : produis-le par lots sur plusieurs tours,
  jamais tout en une seule réponse. Voir « Gros livrables » ci-dessous.
```

### Gros livrables : produire par lots (limite de tokens)

L'artefact final (`output.md` sur S3) est la **concaténation de tous les messages texte de l'agent**, dans l'ordre. Deux conséquences à garder en tête dès que le livrable est volumineux (longues listes, gros tableaux, CSV de centaines de lignes) :

- **Une seule réponse de l'agent est plafonnée à ~32 000 tokens de sortie** (`CLAUDE_CODE_MAX_OUTPUT_TOKENS`). Si la consigne pousse l'agent à tout cracher d'un coup, le run échoue avec `response exceeded the output token maximum` — et part souvent en timeout après de longues minutes bloquées.
- **Seul le texte des messages est capturé.** Un fichier écrit sur le disque du container (un `.csv`, etc.) n'est **pas** récupéré ; la sortie d'un **sous-agent** (`Agent` / `Task`) non plus — elle revient comme résultat d'outil, hors artefact.

Pour un gros livrable, dites-le donc explicitement dans la consigne :

- **Produire par lots, sur plusieurs tours.** Ex. « traite les éléments par lots de 12 ; après chaque lot, lance un `echo` Bash trivial avant d'enchaîner ». Ce point de contrôle clôt le message courant et garde chaque réponse sous le plafond ; le worker recolle tout.
- **Émettre le contenu dans le texte des messages**, pas sur le disque. Pour un CSV, un bloc ` ```csv ` ouvert au premier lot et fermé au dernier (les lots intermédiaires n'ajoutent que des lignes, sans rouvrir la balise ni répéter l'en-tête) se concatène en un bloc continu et propre.
- **Interdire les sous-agents** si leur production doit finir dans l'artefact : « ne délègue pas à un sous-agent, produis tout toi-même ». Ou, avec des sous-agents déclarés (`agents`), faire recopier par l'agent principal ce qui doit figurer dans le livrable.
- Dimensionnez `max_turns` en conséquence (1 tour d'en-tête + 1 par lot + une marge).

Le bloc **ENTRÉE** dépend du mode visé (voir [Concepts fondamentaux](#concepts-fondamentaux)) :

**Mode one-shot** — instructions *et* données dans la consigne, run lancé sans entrée :

```text
Tu es analyste DDETS. Produis un constat synthétique sur la question
suivante : « Quelle est l'évolution des CDD d'usage dans l'hôtellerie en
Bretagne sur 2024-2025 ? ». Méthode : recherche, recoupe, rédige. Rends un
Markdown de 300 mots max, structuré en « Constat / Chiffres clés /
Recommandation », en français. N'invente aucun chiffre : si une donnée
manque, dis-le explicitement.
```

**Mode réutilisable** — la consigne décrit la méthode, la question change à chaque run via `input_uri` :

```text
Tu es analyste DDETS. La question à traiter t'est fournie en entrée.
Méthode : recherche, recoupe, rédige. Rends un Markdown de 300 mots max,
structuré en « Constat / Chiffres clés / Recommandation », en français.
N'invente aucun chiffre : si une donnée manque, dis-le explicitement.
```

Ce second pipeline se déclenche avec une entrée, où la clé `prompt` porte la question du run :

```sh
# in.json  →  {"prompt": "Évolution des CDD d'usage dans l'hôtellerie en Bretagne 2024-2025 ?"}
jobsctl trigger <pipeline-id> --input-uri s3://pipometa/inputs/ddets/in.json
```

Une consigne précise sur le **format de sortie** et les **contraintes** donne des artefacts plus réguliers d'un run à l'autre. Itérez avec `PATCH` jusqu'au rendu voulu.

## Arborescence

```
autometa-jobs/
├── orchestrator/   # l'API : déclenchement, suivi, réconciliation des runs
├── worker/         # ce qui tourne dans le container éphémère + dépose l'artefact
├── jobsctl/        # la CLI : pipelines, trigger, status, events, cancel
├── bin/            # setup.sh : stocke les accès + installe le lanceur jobsctl
├── infra/          # scripts de provisionnement Scaleway
└── *.md            # doc interne (conception, ressources d'infra)
```
