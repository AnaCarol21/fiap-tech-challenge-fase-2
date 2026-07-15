# Modelo de Dados

Fonte: **Base dos Dados** (projeto BigQuery `basedosdados`), dataset `br_inep_avaliacao_alfabetizacao`, projeto de billing `tech-challenge-fase-2-502101`.

As seis tabelas abaixo são extraídas na camada **Bronze**, tratadas na **Silver** e servem de insumo para os datasets analíticos da **Gold**.

---

## Tabela: `uf`

Descrição: indicadores de alfabetização agregados por Unidade da Federação.

Colunas:

| Coluna | Descrição |
|---|---|
| `ano` | Ano de referência da avaliação |
| `sigla_uf` | Sigla da UF (chave) |
| `sigla_uf_nome` | Nome completo da UF (via join com diretório `br_bd_diretorios_brasil.uf`) |
| `serie` | Série/ano escolar avaliado (descrição, via dicionário) |
| `rede` | Rede de ensino (Federal, Estadual, Municipal, Privada, Total) |
| `taxa_alfabetizacao` | Percentual de estudantes alfabetizados |
| `media_portugues` | Proficiência média em português (escala Saeb) |
| `proporcao_aluno_nivel_0` a `proporcao_aluno_nivel_8` | Proporção de alunos em cada nível de proficiência (0 a 8) |

---

## Tabela: `municipio`

Descrição: mesmo indicador da tabela `uf`, no nível de município.

Colunas:

| Coluna | Descrição |
|---|---|
| `ano` | Ano de referência da avaliação |
| `id_municipio` | Código IBGE do município (chave) |
| `id_municipio_nome` | Nome do município (via join com diretório `br_bd_diretorios_brasil.municipio`) |
| `serie` | Série/ano escolar avaliado (descrição, via dicionário) |
| `rede` | Rede de ensino (Federal, Estadual, Municipal, Privada, Total) |
| `taxa_alfabetizacao` | Percentual de estudantes alfabetizados |
| `media_portugues` | Proficiência média em português (escala Saeb) |
| `proporcao_aluno_nivel_0` a `proporcao_aluno_nivel_8` | Proporção de alunos em cada nível de proficiência (0 a 8) |

---

## Tabela: `meta_alfabetizacao_brasil`

Descrição: metas nacionais de alfabetização até 2030, por rede de ensino.

Colunas:

| Coluna | Descrição |
|---|---|
| `ano` | Ano de referência |
| `rede` | Rede de ensino |
| `taxa_alfabetizacao` | Taxa de alfabetização observada no ano |
| `meta_alfabetizacao_2024` a `meta_alfabetizacao_2030` | Metas anuais definidas pelo Compromisso Nacional Criança Alfabetizada |
| `percentual_participacao` | Percentual de participação/cobertura da avaliação |

---

## Tabela: `meta_alfabetizacao_uf`

Descrição: mesma estrutura de metas da tabela nacional, aberta por UF.

Colunas:

| Coluna | Descrição |
|---|---|
| `ano` | Ano de referência |
| `sigla_uf` | Sigla da UF (chave) |
| `sigla_uf_nome` | Nome completo da UF |
| `rede` | Rede de ensino |
| `taxa_alfabetizacao` | Taxa de alfabetização observada no ano |
| `meta_alfabetizacao_2024` a `meta_alfabetizacao_2030` | Metas anuais por UF |
| `percentual_participacao` | Percentual de participação/cobertura da avaliação |

---

## Tabela: `meta_alfabetizacao_municipio`

Descrição: mesma estrutura de metas, aberta por município.

Colunas:

| Coluna | Descrição |
|---|---|
| `ano` | Ano de referência |
| `id_municipio` | Código IBGE do município (chave) |
| `id_municipio_nome` | Nome do município |
| `rede` | Rede de ensino |
| `taxa_alfabetizacao` | Taxa de alfabetização observada no ano |
| `meta_alfabetizacao_2024` a `meta_alfabetizacao_2030` | Metas anuais por município |
| `nivel_alfabetizacao` | Classificação/nível de alfabetização do município |
| `percentual_participacao` | Percentual de participação/cobertura da avaliação |

---

## Tabela: `alunos`

Descrição: microdados individuais da avaliação: granularidade mais fina do modelo, uma linha por aluno avaliado.

Colunas:

| Coluna | Descrição |
|---|---|
| `ano` | Ano de referência da avaliação |
| `id_municipio` | Código IBGE do município do aluno |
| `id_municipio_nome` | Nome do município |
| `id_escola` | Código INEP da escola |
| `id_aluno` | Identificador único do aluno (chave) |
| `caderno` | Caderno de prova aplicado |
| `serie` | Série/ano escolar do aluno |
| `rede` | Rede de ensino da escola |
| `presenca` | Situação de presença do aluno na avaliação |
| `preenchimento_caderno` | Situação de preenchimento do caderno de respostas |
| `alfabetizado` | Classificação se o aluno atingiu o ponto de corte de alfabetização |
| `proficiencia` | Proficiência individual do aluno na escala Saeb |
| `peso_aluno` | Peso amostral do aluno (para ponderação estatística) |


---

## Metadados técnicos por camada

Além das colunas de negócio acima, cada camada adiciona metadados de controle:

**Bronze** (adicionados em `construir_bronze`):
- `_ingestion_timestamp` — timestamp UTC da ingestão
- `_ingestion_date` — data da ingestão
- `_source_dataset` — dataset de origem (`br_inep_avaliacao_alfabetizacao`)
- `_source_table` — tabela de origem
- `_record_hash` — hash MD5 do registro, para rastreabilidade e detecção de duplicidade

**Silver** (`construir_silver`):
- Remove os metadados de controle da Bronze (não fazem sentido para análise)
- Aplica `strip()` em colunas de texto
- Adiciona `_silver_processed_at` — timestamp de processamento da Silver

**Gold**:
- Adiciona `_gold_processed_at` — timestamp de processamento da Gold

---