# Modelo de Dados

Fonte: **Base dos Dados** (projeto BigQuery `basedosdados`), projeto de billing `tech-challenge-fase-2-502101`. As tabelas vêm de datasets diferentes conforme indicado em cada seção (ver `DATASET_POR_TABELA` no notebook Bronze).

Onze tabelas são extraídas na camada **Bronze**: as 6 originais da Fase 2 (dataset `br_inep_avaliacao_alfabetizacao`) e 4 novas de enriquecimento externo, adicionadas na Fase 3 (seção "Enriquecimento Externo (Fase 3)" abaixo). Todas são tratadas na **Silver** e servem de insumo para os datasets analíticos da **Gold**.

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

# Enriquecimento Externo (Fase 3)

As 4 tabelas abaixo foram adicionadas na Fase 3, para dar suporte às
variáveis educacionais, territoriais e socioeconômicas exigidas pelo
modelo supervisionado. Todas passam pela mesma pipeline Bronze → Silver
já existente (conversão de tipo, dedup, checagem de qualidade,
particionamento, observabilidade) — só foram adicionadas entradas nos
dicionários de configuração (`TABELAS`, `QUERIES`, `CHECKS`,
`TIPOS_COLUNAS`, `COLUNAS_ESSENCIAIS`, `CHAVE_NEGOCIO`), nenhuma lógica
nova precisou ser escrita para o tratamento genérico dessas tabelas.

**Escopo de anos na extração (Bronze)**: diferente das 6 tabelas
originais (que preservam todo o histórico disponível), estas 4 fontes
têm dado desde 1991 no BigQuery — puxar tudo seria caro e desnecessário
para o escopo do projeto. `pib_municipio`, `populacao_municipio` e
`inse_escola` são filtradas para `ano = 2023`; `indicadores_educacionais_municipio`
inclui também `2022`, necessário para o defasamento de 1 ano (ver abaixo).

## Tabela: `pib_municipio`

Fonte: `basedosdados.br_ibge_pib.municipio`. Filtro de extração: `ano = 2023`.

Descrição: PIB e composição do valor adicionado por município.

⚠️ **Limitação encontrada com dado real**: só a coluna `pib` (total) tem
cobertura completa para 2023. As colunas de detalhamento setorial
(`impostos_liquidos`, `va`, `va_agropecuaria`, `va_industria`,
`va_servicos`, `va_adespss`) vieram **100% nulas** nos 5.570 municípios
— o detalhamento setorial do PIB tem defasagem de divulgação maior que
o total agregado. Por isso **não são levadas para a Gold**.

Colunas usadas: `ano`, `id_municipio`, `pib`.

Join com a base de alunos: `ano + id_municipio`. Na Gold, `pib_per_capita`
é calculado (`pib / populacao`) para não misturar tamanho do município
com riqueza — `pib` bruto sozinho é maior em cidades grandes só por
serem grandes, não necessariamente mais ricas per capita.

## Tabela: `indicadores_educacionais_municipio`

Fonte: `basedosdados.br_inep_indicadores_educacionais.municipio`. Filtro
de extração: `ano IN (2022, 2023)` + `LOWER(localizacao) = 'total' AND
LOWER(rede) = 'total'`.

⚠️ **Bug real encontrado**: a fonte muda a **capitalização** dos valores
de `localizacao`/`rede` entre anos (`"total"` minúsculo em 2021/2022,
`"Total"` maiúsculo em 2023, incluindo acentuação diferente em
`"publica"` → `"Pública"`). Um filtro sensível a maiúsculo/minúsculo
descartava 2022 inteiro silenciosamente. Corrigido comparando com
`LOWER()`.

Descrição: indicadores de qualidade docente e desempenho, no nível de
município.

⚠️ **Risco de data leakage**: `tdi_ef_2_ano`, `taxa_aprovacao_ef_2_ano`,
`taxa_reprovacao_ef_2_ano` e `taxa_abandono_ef_2_ano` do MESMO ano são
resultado do mesmo processo educacional que o modelo tenta prever
(agregado por município). Por isso, na Gold, essas 4 colunas são unidas
com **defasamento de 1 ano** (indicador de 2022 vira contexto do aluno
de 2023) e renomeadas com sufixo `_ano_anterior`.

Colunas:

| Coluna | Descrição | Uso na Gold |
|---|---|---|
| `ano`, `id_municipio`, `id_municipio_nome`, `localizacao`, `rede` | Identificação | — |
| `atu_ef_anos_iniciais` | Alunos por turma (anos iniciais) | Estrutural, mesmo ano |
| `had_ef_anos_iniciais` | Horas-aula diária (anos iniciais) | Estrutural, mesmo ano |
| `dsu_ef_anos_iniciais` | % docentes com curso superior | Estrutural, mesmo ano |
| `afd_ef_anos_iniciais_grupo_1` | Adequação da formação docente (grupo mais adequado) | Estrutural, mesmo ano |
| `ird_alta`, `ird_baixa_regularidade` | Regularidade docente (rotatividade) | Estrutural, mesmo ano |
| `icg_nivel_1` a `icg_nivel_6` | Complexidade de gestão da escola | Estrutural, mesmo ano (uso no modelo ainda em avaliação) |
| `tdi_ef_2_ano` | Distorção idade-série (2º ano) | **Defasado (ano-1)** → `tdi_ef_2_ano_anterior` |
| `taxa_aprovacao_ef_2_ano` | Taxa de aprovação (2º ano) | **Defasado (ano-1)** → `taxa_aprovacao_ef_2_ano_anterior` |
| `taxa_reprovacao_ef_2_ano` | Taxa de reprovação (2º ano) | **Defasado (ano-1)** → `taxa_reprovacao_ef_2_ano_anterior` |
| `taxa_abandono_ef_2_ano` | Taxa de abandono (2º ano) | **Defasado (ano-1)** → `taxa_abandono_ef_2_ano_anterior` |

Join com a base de alunos: `ano + id_municipio` (estruturais) e
`(ano-1) + id_municipio` (defasados).

## Tabela: `populacao_municipio`

Fonte: `basedosdados.br_ibge_populacao.municipio`. Filtro de extração: `ano = 2023`.

Descrição: população estimada por município/ano. Variável de controle demográfico (município grande x pequeno).

Colunas: `ano`, `sigla_uf`, `sigla_uf_nome`, `id_municipio`, `id_municipio_nome`, `populacao`.

Join com a base de alunos: `ano + id_municipio`. Usada também para calcular `pib_per_capita`.

## Tabela: `inse_escola`

Fonte: `basedosdados.br_inep_indicador_nivel_socioeconomico.escola`. Filtro de extração: `ano = 2023`.

Descrição: Indicador de Nível Socioeconômico (INSE), calculado pelo INEP a partir do questionário socioeconômico respondido pelos alunos (bens da família, escolaridade dos pais etc.), por escola. Não é data leakage: reflete características da família, não é calculado a partir do resultado de alfabetização — não precisa de defasamento.

⚠️ **Descoberta com dado real — `id_escola` incompatível**: o `id_escola`
da tabela `alunos` **não é o código INEP oficial de escola**. Comparando
os 2 primeiros dígitos do código (que no padrão INEP correspondem ao
código de UF): alunos têm `id_escola` começando em prefixos como `60`
(não é um código de UF válido — o maior código real é `53`), enquanto
`inse_escola`/Censo Escolar usam o código oficial (ex.: `31` = Minas
Gerais). São sistemas de identificação diferentes — **não é possível
fazer join por escola** entre `alunos` e fontes que usam o código INEP
oficial.

**Solução adotada**: agregar o INSE por `(ano, id_municipio)` — média do
`inse` das escolas do município (`inse_medio`) + contagem de escolas
usadas na média (`quantidade_escolas_inse`, indicador de cobertura, não
usado como feature). Perde granularidade de escola, mas preserva o
sinal socioeconômico — documentado como limitação do projeto.

**Consequência para o Censo Escolar**: pelo mesmo motivo (mesmo esquema
de `id_escola` incompatível), o Censo Escolar foi **removido** da
arquitetura de enriquecimento e substituído pelo PIB dos Municípios, que
já nasce na granularidade de município, sem precisar de agregação.

Colunas: `ano`, `id_municipio`, `id_escola`, `inse`, `classificacao`
(`classificacao` não é usada na agregação da Gold — é uma categorização
derivada do próprio `inse`, redundante depois de agregado por média).

Join com a base de alunos: `ano + id_municipio` (agregado).

---

## Tabela (Silver): `alunos_integrado`

Construída em `construir_silver_alunos_integrado`, integra `alunos` + `municipio` + `uf`: cada aluno ganha `sigla_uf` (derivada do código IBGE do município) e contexto agregado municipal/estadual (`taxa_alfabetizacao_municipio`, `taxa_alfabetizacao_uf` etc.). É o insumo de `alunos_alfabetizacao` (Gold).

⚠️ **Bug real encontrado na chave de negócio**: o dedup dessa tabela e
das tabelas `uf`/`municipio` usava chaves incompletas
(`ano+sigla_uf`/`ano+id_municipio`, ignorando `serie`/`rede`), e a
própria tabela `alunos` deduplicava só por `id_aluno` (ignorando `ano`).
Rodando com dado real, isso causava perda silenciosa: `uf` perdia ~66%
das linhas (múltiplas redes colapsadas em uma só), e `alunos` perdia
~39% das linhas (o mesmo `id_aluno` se repete entre 2023 e 2024 — não é
o mesmo estudante, é um identificador reaproveitado a cada edição da
avaliação). Corrigido usando a chave de negócio completa em cada tabela
(`CHAVE_NEGOCIO` no notebook Silver).

---

## Tabela (Gold): `alunos_alfabetizacao`

Base final para a modelagem supervisionada da Fase 3. Construída em `gold_alunos_alfabetizacao`, a partir de `alunos_integrado` + as 4 tabelas de enriquecimento (PIB, Indicadores Educacionais, População, INSE).

**Escopo**: só ano-base **2023** (filtro aplicado na Gold, não na extração — ver `ANO_BASE_MODELAGEM` na função). Motivo: o modelo é uma classificação transversal (não previsão temporal), e não há alunos em comum entre 2023 e 2024 na base (o `id_aluno` não persiste entre anos).

**Alvo**: `alfabetizado` (Sim/Não). Registros sem esse valor definido (aluno ausente/caderno não preenchido) são descartados.

⚠️ **Colunas com risco de data leakage** (mantidas na tabela por transparência, mas **não podem** ser usadas como feature no treino — ver `COLUNAS_RISCO_DATA_LEAKAGE` no notebook):
- `proficiencia` — define o próprio alvo (corte de proficiência)
- `taxa_alfabetizacao_municipio`, `taxa_alfabetizacao_uf` — agregados que já incluem o resultado do próprio aluno
- `media_portugues_municipio`, `media_portugues_uf` — mesmo mecanismo acima

**Validação**: cada join de enriquecimento passa por `checar_integridade_referencial` (mesma função usada na Silver para o join alunos+município+UF), reportando `PASS`/`WARN` conforme percentual de órfãos.

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
- **Preserva** os metadados de controle da Bronze (`_record_hash`, `_source_dataset`, `_source_table`, `_ingestion_timestamp`, `_ingestion_date`) — atravessam a Silver propositalmente, para permitir auditoria de ponta a ponta (padrão Medalhão). A limpeza de schema analítico acontece só na Gold.
- Aplica `strip()` em colunas de texto, conversão de tipo (`TIPOS_COLUNAS`), normalização de chave (`sigla_uf`) e dedup por **chave de negócio completa** (`CHAVE_NEGOCIO` — não linha inteira, nem coluna única; ver nota abaixo)
- Adiciona `_silver_processed_at` — timestamp de processamento da Silver

**Gold**:
- Adiciona `_gold_processed_at` — timestamp de processamento da Gold

---