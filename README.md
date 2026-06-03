# monkey-island-rw-cleaner

Safe cleanup service for old expired RWMS subscriptions.

The service never deletes rows from the SQLAlchemy `users` table. It only deletes
RWMS users after checking both RWMS and the local database.

## Safety Model

Deletion is allowed only when all checks pass:

- RWMS user has `expire_at`;
- RWMS `expire_at` is older than `GRACE_DAYS`;
- local `users` row exists by `username`, unless `--include-orphans` is explicitly used;
- local `users.expire_at` exists;
- local `users.expire_at` is in the past and older than `GRACE_DAYS`;
- user has no recent pending YooKassa payment;
- user has no non-expired WATA invoice without a paid transaction;
- delete count is below `MAX_DELETIONS`;
- the RWMS user is re-read immediately before deletion and `username`/`expire_at`
  still match the candidate snapshot.

Default mode is dry-run.

## Environment

Required:

```bash
DATABASE_URL=postgresql+psycopg2://user:password@host:5432/db
RWMS_HOST=127.0.0.1
RWMS_PORT=50052
```

Optional:

```bash
GRACE_DAYS=30
PENDING_PAYMENT_GRACE_HOURS=48
PAGE_SIZE=500
MAX_DELETIONS=25
EXECUTE=false
INCLUDE_ORPHANS=false
USERNAME_ALLOWLIST=
```

`GRACE_DAYS` cannot be lower than 30.

## Run

Dry-run:

```bash
python main.py
```

Real deletion:

```bash
python main.py --execute
```

Very conservative first production run:

```bash
python main.py --execute --max-deletions 3
```

Test one known username only:

```bash
python main.py --username-allowlist 4f1d... --execute --max-deletions 1
```

## Tests

```bash
pytest
```

