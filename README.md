# falasca_ETL_python_API_ML
Processo de ETL para integração de dados do projeto do ML para extração de anúncios da Falasca

## Estrutura

```
config/       configuração via variáveis de ambiente (config/settings.py)
drivers/      integrações externas: HTTP client ML, OAuth, Postgres (schema.sql)
src/
  extraction/     dreno da fila de alterações (webhook), scan completo do catálogo (bootstrap),
                  multiget de controle, enrich por mlb_id
  transformation/ cálculo do delta NEW/UPDATED/REACTIVATED/DELETED
  load/           staging + upsert em bronze/prata
  pipeline.py     orquestra run() (incremental) e run_full_scan() (bootstrap)
utils/        helpers pequenos (logging, dedupe)
main.py            entrypoint do fluxo incremental (webhook), roda em cron/Task Scheduler
main_full_scan.py  entrypoint do bootstrap (scan completo), roda uma única vez
tests/             suíte pytest (doubles de psycopg2, sem banco real)
```

## Integração com o webhook

Esta pipeline não varre mais os anúncios ativos a cada run. Em vez disso, ela lê a tabela
`ml_item_changes`, alimentada pelo webhook do Mercado Livre no repositório **API_FALASCA**:

```
Mercado Livre → webhook (API_FALASCA) → INSERT em ml_item_changes → esta pipeline drena e processa só esses ids
```

Pré-requisitos para isso funcionar:

- O webhook (`API_FALASCA`) precisa estar rodando, com o mesmo `ML_USER_ID` e apontando para o
  **mesmo banco** desta pipeline (`POSTGRES_*`).
- A tabela `ml_item_changes` precisa existir (está em `drivers/schema.sql`, ver Setup abaixo).

Como o run agora é barato quando não há nada pendente (um `DELETE ... RETURNING` vazio, sem
chamadas à API do ML), rode-o com frequência -- a cada 1-5 minutos via Task Scheduler/cron -- em vez
do intervalo maior que fazia sentido para um scan completo.

## Setup

1. `pip install -r requirements.txt`
2. Copie `.env.example` para `.env` e preencha com os valores reais (o `.env`
   está no `.gitignore` e nunca deve ser commitado).
3. Aplique `drivers/schema.sql` manualmente no Postgres (uma vez, com um
   usuário que tenha privilégio de CREATE) -- o worker nunca cria
   schema/tabelas em runtime, só espera que já existam. Isso inclui
   `ml_item_changes`, então não é preciso aplicar o schema do webhook
   separadamente (mas tanto faz qual dos dois repositórios aplica primeiro:
   ambos definem a tabela do mesmo jeito, com `CREATE TABLE IF NOT EXISTS`).
4. Rode o bootstrap **uma única vez**: `python main_full_scan.py` (ver seção
   "Scan completo (bootstrap)" abaixo).
5. Dali em diante, só o incremental: `python main.py`.

Cada execução de `main.py` faz: dreno da fila `ml_item_changes` -> multiget de controle apenas dos ids
drenados -> delta contra `bronze_anuncios` (NEW/UPDATED/REACTIVATED por `status`, DELETED quando o
`status` não é mais `active`) -> enrich (SKU/Classe/Frete/Comissão/Status Catálogo) apenas do que é
NEW/UPDATED/REACTIVATED -> upsert em bronze -> flag de DELETED -> projeção bronze -> prata
(novos/atualizados são upsertados, deletados são removidos da prata) -> log em `etl_run_log`.

Se a fila estiver vazia, o run só grava o log e termina -- sem chamadas à API do Mercado Livre.

Para reprocessar um único anúncio sob demanda, chame
`src.extraction.enrich.enrich_item(client, config, mlb_id)` diretamente.

### Bronze sempre atualiza, prata só quando algo relevante mudou

Um anúncio pode entrar no delta como UPDATED (o `last_updated` do ML avançou) sem que nenhum dos 7 campos
rastreados (`sku, frete, classe, comissao, taxa_fixa, tipo_anuncio, status_catalogo` -- `TRACKED_FIELDS` em
`src/load/warehouse.py`) realmente tenha mudado (ex.: só o estoque mudou). Nesse caso:

- **Bronze é gravada mesmo assim.** Isso não é opcional: se `last_updated` não avançar na bronze, o mesmo
  anúncio voltaria a aparecer como "desatualizado" em todo run futuro, mesmo para notificações totalmente
  alheias aos 7 campos -- viraria um reprocessamento permanente em vez de pontual.
- **Prata só recebe o id se algo realmente relevante mudou** (`src/pipeline.py::_process_ids` compara os
  `TRACKED_FIELDS` contra um snapshot da bronze tirado antes do upsert). Itens novos e reativados sempre
  propagam pra prata, mesmo com os 7 campos idênticos ao que já existia.
- **Prata nunca guarda anúncios deletados.** Diferente da bronze (que mantém o registro com
  `coluna_deletada = true` para histórico), a prata não tem essa coluna -- quando um anúncio é deletado, a
  linha é removida de vez (`src/load/warehouse.py::remove_from_prata`), assim como a linha da planilha.

## Scan completo (bootstrap)

`main.py` só processa o que passa pela fila do webhook (`ml_item_changes`) -- um anúncio que já existe e
nunca sofrer uma alteração nova nunca apareceria em `bronze_anuncios_mercado_livre` sozinho. Por isso existe
`main_full_scan.py`: busca todos os anúncios ativos do vendedor via `GET /users/{seller_id}/items/search`
(modo `scan`, paginado por `scroll_id`, sem o teto de 1000 resultados do offset/limit tradicional -- ver
`src/extraction/full_scan.py`) e roda o mesmo pipeline de control/delta/enrich/bronze/prata que o
incremental usa.

Rode uma vez para popular a base a partir do catálogo atual. Não agende `main_full_scan.py` no cron/Task
Scheduler -- só `main.py` deve rodar recorrentemente. Fica no repo apenas para o caso raro de precisar
reconstruir bronze/prata do zero no futuro.

### Limitação conhecida: sem rede de segurança recorrente

Depois do bootstrap, o sistema confia 100% no webhook para saber de anúncios novos ou alterados -- não há
scan periódico de segurança. Se uma notificação de um anúncio **novo** se perder (falha de escrita no
Postgres do lado do webhook, API do Mercado Livre fora do ar no momento da notificação), esse anúncio
específico não ganha SKU/Frete/Classe/Comissão automaticamente, e nada avisa disso -- o objetivo do projeto
(eliminar o preenchimento manual da planilha para anúncios novos) fica sem cumprir para esse caso pontual
até alguém notar.

Quando isso acontecer, corrija rodando manualmente:

```python
from drivers import db
from drivers.ml_auth import get_valid_access_token
from drivers.ml_client import MLClient
from config.settings import load_config
from src.extraction.enrich import enrich_item
from src.load.warehouse import upsert_bronze, project_to_prata

config = load_config()
conn = db.connect(config.database_url, config.postgres_schema)
client = MLClient(get_valid_access_token(conn, config))

row = enrich_item(client, config, "MLB1234567890")
upsert_bronze(conn, [row])
project_to_prata(conn, [row["mlb"]])
conn.commit()
```

## Saída em planilha (Google Sheets)

Além de bronze/prata, todo run também espelha os mesmos ids "realmente mudados" (mesma lista que vai pra
prata) na aba `MATRIZ` da planilha Google **"EXTRAÇÃO DE ANÚNCIOS MLB"** -- `drivers/google_sheets_output.py`,
upsert por MLB (atualiza linha existente, cria se for novo, remove se o anúncio for deletado).

Autenticação via service account do Google Cloud:

- `GOOGLE_SERVICE_ACCOUNT_FILE`: caminho local do JSON da chave da service account.
- `GOOGLE_SHEETS_SPREADSHEET_ID`: ID da planilha (trecho da URL entre `/d/` e `/edit`).
- `GOOGLE_SHEETS_WORKSHEET_NAME`: nome da aba, default `MATRIZ`.

A planilha precisa estar compartilhada com o `client_email` da service account (do próprio JSON) com
permissão de **Editor**. Qualquer uma das duas primeiras variáveis vazia desativa a escrita
(`NullGoogleSheetsClient`, ver `build_google_sheets_client`).

## Log de execução e alerta de falha

Todo run (sucesso, skip por lock concorrente, ou falha) grava uma linha em `public.etl_execution_logs`
(`drivers/execution_log.py`) -- log simples e portável, compartilhado com outros processos de ETL da
empresa (schema fixo, definido fora deste repositório), independente do `POSTGRES_SCHEMA` configurado,
complementando o `etl_run_log` interno (que guarda as métricas de delta). Colunas: `id, project_name,
operation ('incremental'|'full_scan'), status, error_reason, start_time, end_time`.

Em caso de falha (tanto no incremental quanto no full scan), `drivers/email_alert.py::send_failure_alert`
envia um e-mail de verdade via `utils/log_mail_message.py` (SMTP da Email em Nuvem), com assunto fixo
`[FALHA ENGENHARIA] Falasca - Python ETL` e corpo `Falha na <mensagem de erro>`. Isso é independente do
registro em `etl_run_log`/`etl_execution_logs`, que acontece sempre. Credenciais em `EMAIL_SENDER`,
`SENDER_PASSWORD` (senha de app) e `EMAIL_RECEIVER` no `.env` -- sem elas o envio falha e só loga o erro
(ver `utils/log_mail_message.py::send_email`).
