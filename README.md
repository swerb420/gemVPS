# gemVPS

This repository contains the source code for the gemVPS project. Environment
variables are loaded using `pydantic` models defined in `gemVPS/utils/config.py`.

## Database pool settings

Two optional settings control the size of the asyncpg connection pool:

- `DB_POOL_MIN_SIZE` – minimum number of connections to maintain. Default is `1`.
- `DB_POOL_MAX_SIZE` – maximum number of connections allowed under load. Default is `5`.

Provide these values in your `.env` file if you need to override the defaults.
