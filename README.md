# Tech Challenge – Fase 2

## Pipeline Híbrida para Análise da Alfabetização no Brasil

Projeto desenvolvido para o Tech Challenge da Pós-Tech FIAP (Fase 2), com o objetivo de construir uma pipeline híbrida de dados (Batch + Streaming) para integração, tratamento e disponibilização de dados públicos sobre alfabetização infantil no Brasil, seguindo a Arquitetura Medalhão (Bronze, Silver, Gold) em ambiente de nuvem.

---

## 1. Contexto do problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico do país. Nesse cenário, o **Compromisso Nacional Criança Alfabetizada** mobiliza União, estados, Distrito Federal e municípios com o objetivo de garantir que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental.

Para apoiar essa política, o **INEP (Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira)** realizou em 2023 a **Pesquisa Alfabetiza Brasil**, que definiu o ponto de corte de **743 pontos** na escala de proficiência do Saeb, a partir do qual uma criança é considerada alfabetizada. Com base nesse parâmetro, foi criado o **Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que atingem esse patamar. A meta nacional é alfabetizar 100% das crianças até 2030.

Compreender os fatores que influenciam a alfabetização exige integrar diferentes fontes de dados: metas nacionais, estaduais e municipais, dados territoriais, microdados educacionais e indicadores de desempenho, em vez de olhar cada indicador isoladamente.

## 2. O desafio

Construir uma pipeline híbrida (batch + streaming) que:

- integre as seis entidades de dados da plataforma **Base dos Dados** referentes à alfabetização (UF, Município, Meta Alfabetização Brasil, Meta Alfabetização por UF, Meta Alfabetização por Município e Dados de Alunos);
- padronize, trate e valide essas informações;
- disponibilize uma camada analítica confiável (Gold) para dashboards, análises estatísticas e modelos de machine learning;
- rode em nuvem (AWS), com foco em escalabilidade, qualidade de dados e controle de custos (FinOps).

> **Extensão para a Fase 3**: a mesma pipeline foi estendida com **4 fontes externas de enriquecimento** (PIB dos Municípios, Indicadores Educacionais, População e INSE), formando a base de dados (`alunos_alfabetizacao`) usada para treinar o modelo supervisionado de previsão de alfabetização. Ver seção 3.1.

## 3. Arquitetura da solução

A pipeline segue a **Arquitetura Medalhão**, com ingestão híbrida convergindo para as três camadas:

**Fluxo de dados:**

1. **Ingestão Batch** — extração das 6 tabelas da Base dos Dados (metas nacionais/estaduais/municipais, município, UF, alunos), carregadas como Parquet em `s3://.../raw/`.
2. **Ingestão Streaming** — um Producer Kafka simula eventos quase em tempo real (atualização de indicadores, novas medições de desempenho, atualização de metas), consumidos por um Consumer que persiste os eventos em `bronze_stream.parquet`.
3. **Camada Bronze** — dados brutos das duas ingestões, sem transformação significativa, com histórico completo preservado. No AWS Glue, um job (`glue_etl_bronze`) lê cada dataset de `raw/` e grava em `bronze/<dataset>/`, com logging de contagem de registros e tratamento de falha por dataset.
4. **Camada Silver** — limpeza, padronização de tipos e nomes, tratamento de nulos, remoção de duplicidade e **integração entre as bases** (joins entre alunos, município e UF). Executada localmente em Pandas (protótipo) e replicada em PySpark no Glue (`glue_etl_silver`).
5. **Camada Gold** — datasets analíticos prontos para consumo: ranking de UFs, ranking de municípios, evolução temporal do indicador por UF, resumo agregado por rede de ensino, comparação entre meta e resultado, e (Fase 3) a base de alunos enriquecida para modelagem. Executada em Pandas localmente e em PySpark no Glue (`glue_etl_gold`), com checks de qualidade de dados antes da persistência.
6. **Consumo** — a camada Gold alimenta dashboards de BI e o modelo supervisionado de previsão de alfabetização (Fase 3).

### Camadas em detalhe

**Bronze: dados brutos (raw)**
- Sem transformação de negócio, apenas padronização estrutural (schema, tipos básicos).
- Histórico completo preservado (nenhum dado é descartado nesta camada).
- Hash de registros para rastreabilidade e detecção de duplicidade futura.

**Silver: dados tratados**
- Limpeza (remoção de duplicidade por **chave de negócio completa** — não linha inteira nem coluna única; ver seção 5), tratamento de nulos essenciais, conversão de tipo.
- Padronização de nomes de colunas e tipos.
- Normalização de chaves territoriais (`sigla_uf`) para permitir joins consistentes.
- **Preserva os metadados de rastreabilidade da Bronze** (`_record_hash`, `_source_dataset`, `_ingestion_timestamp` etc.) — a limpeza de schema analítico só acontece na Gold.
- Integração **territorial** entre as bases (alunos + município + UF), com checagem de integridade referencial e consistência.
- Cada uma das 10 tabelas passa pela mesma pipeline genérica de tratamento — adicionar uma tabela nova (como as 4 de enriquecimento da Fase 3) não exige código novo, só entradas nos dicionários de configuração.

**Gold: camada analítica**
- `ranking_uf`: ranking de UFs por taxa de alfabetização, por ano e rede de ensino.
- `ranking_municipio`: mesmo ranking, no nível de município.
- `evolucao_uf`: série histórica da taxa de alfabetização e proficiência em português por UF.
- `resumo_rede`: agregados (médias e contagem de UFs) por ano e rede de ensino (Federal, Estadual, Municipal, Privada).
- `comparacao_meta_uf` / `comparacao_meta_municipio`: taxa observada vs. meta definida para o mesmo ano, com indicador de atingimento.
- `alunos_alfabetizacao` **(Fase 3)**: base no nível de aluno para o modelo supervisionado, unindo `alunos_integrado` (Silver) às 4 fontes de enriquecimento externo. É aqui — não na Silver — que a curadoria de negócio acontece: quais colunas entram, qual escopo de ano, qual granularidade de junção. Ver seção 3.1.

### 3.1 Enriquecimento externo (Fase 3)

A base `alunos_alfabetizacao` (Gold) é o insumo do modelo supervisionado da Fase 3 (previsão de `alfabetizado`). Ela une `alunos_integrado` (Silver) a 4 fontes externas, todas do mesmo ecossistema Base dos Dados/BigQuery, seguindo a mesma pipeline Bronze → Silver que as 6 tabelas originais:

| Fonte | Dataset BigQuery | Granularidade | Join | Escopo de ano |
|---|---|---|---|---|
| PIB dos Municípios | `br_ibge_pib` | Município | `ano + id_municipio` | 2023 |
| Indicadores Educacionais | `br_inep_indicadores_educacionais` | Município | `ano + id_municipio` (estrutural) e `(ano-1) + id_municipio` (histórico) | 2022 e 2023 |
| População | `br_ibge_populacao` | Município | `ano + id_municipio` | 2023 |
| INSE (Nível Socioeconômico) | `br_inep_indicador_nivel_socioeconomico` | Escola, agregado para município | `ano + id_municipio` | 2023 |

**Por que só 2023 (e não 2023+2024)**: o modelo é uma classificação transversal (não uma previsão temporal) — treino e teste acontecem dentro do mesmo ano-base. O filtro de escopo é aplicado na **Gold** (função `gold_alunos_alfabetizacao`), não na extração, porque `alunos` é uma tabela original da Fase 2 e deve manter histórico completo como as demais.

**Defasamento de 1 ano (tratamento de data leakage)**: `tdi_ef_2_ano`, `taxa_aprovacao_ef_2_ano`, `taxa_reprovacao_ef_2_ano` e `taxa_abandono_ef_2_ano` são indicadores de **resultado** do mesmo processo educacional que o modelo tenta prever — usá-los do mesmo ano seria leakage direto (agregado por município, mas ainda assim circular). Por isso essas 4 colunas são unidas com o valor do **ano anterior** (2022 → aluno de 2023), renomeadas com sufixo `_ano_anterior`.

**Por que INSE é agregado por município, e não por escola**: o `id_escola` da tabela `alunos` (avaliação de alfabetização) **não é o código INEP oficial de escola** — é um identificador de outro esquema (descoberto comparando os 2 primeiros dígitos do código, que deveriam ser um código de UF válido e não são). Não existe join possível por escola entre `alunos` e Censo Escolar/INSE. Solução: agregar o INSE por `(ano, id_municipio)` (média do `inse` das escolas do município), perdendo granularidade de escola mas preservando o sinal socioeconômico — documentado como limitação do projeto.

**Por que PIB substituiu o Censo Escolar**: o Censo Escolar (infraestrutura da escola) tinha o mesmo problema de `id_escola` incompatível do INSE. Em vez de agregar mais uma fonte por município (com a mesma perda de granularidade), optamos por substituí-lo pelo PIB dos Municípios, que já nasce na granularidade de município — sem necessidade de agregação. Usamos `pib_per_capita` (calculado a partir de `pib / populacao`), não `pib` bruto, para não misturar tamanho do município com riqueza. As colunas de detalhamento setorial do PIB (`va_agropecuaria`, `va_industria` etc.) vieram 100% nulas para 2023 na fonte e foram removidas do Gold.

**Colunas de risco de data leakage** (mantidas na tabela por transparência, mas que **não podem** ser usadas como feature no treino — ver `COLUNAS_RISCO_DATA_LEAKAGE` no notebook Gold):
- `proficiencia` — define o próprio alvo (corte de 743 pontos)
- `taxa_alfabetizacao_municipio` / `taxa_alfabetizacao_uf` — agregados que já incluem o resultado do próprio aluno
- `media_portugues_municipio` / `media_portugues_uf` — mesmo mecanismo acima

## 4. Tecnologias utilizadas

| Tecnologia | Uso no projeto | Justificativa |
|---|---|---|
| **Python + Pandas** | Protótipo local das camadas Bronze/Silver/Gold | Iteração rápida e validação da lógica de negócio antes de portar para Spark, sem custo de cluster |
| **PySpark (AWS Glue)** | Execução em nuvem das mesmas camadas | Processamento distribuído, nativo ao ambiente Glue, sem gerenciar infraestrutura de cluster |
| **Apache Kafka** | Ingestão streaming (Producer/Consumer) | Padrão de mercado para eventos quase em tempo real; simula atualizações de indicadores e metas |
| **Amazon S3** | Data Lake (raw, bronze, silver, gold) | Armazenamento barato, durável e particionável em Parquet, desacoplado do processamento |
| **AWS Glue** | Orquestração e execução das transformações em nuvem | Serverless (paga por uso), integração nativa com S3 e IAM, sem provisionar servidores |
| **Parquet** | Formato de persistência em todas as camadas | Colunar, comprimido, com leitura seletiva de colunas, reduz custo de storage e de leitura |
| **IAM Roles** | Controle de acesso (`AWSGlueServiceRole-TechChallenge`) | Princípio do menor privilégio: acesso restrito ao bucket do projeto e ao serviço Glue |

## 5. Decisões arquiteturais (trade-offs)

**Batch vs. Streaming**
Optamos por um modelo híbrido em vez de escolher só um dos dois. As metas e microdados educacionais (INEP/Base dos Dados) são publicados em ciclos (anual/periódico) batch é suficiente e mais barato para essas fontes. Já a simulação de atualizações de indicadores e resultados exige baixa latência de disponibilização, daí o uso de Kafka para essa fatia do problema, sem forçar todo o pipeline a rodar em streaming (o que encareceria a solução sem necessidade real).

**GCP vs. AWS (arquitetura multi-cloud)**
A extração dos dados foi feita via BigQuery (GCP), consultando a Base dos Dados através da biblioteca `basedosdados` com um projeto de billing próprio, enquanto toda a persistência e processamento (raw/bronze/silver/gold) acontece no S3/Glue (AWS). Essa combinação configura, portanto, uma arquitetura **multi-cloud**.

- **Por que GCP na extração?** A Base dos Dados disponibiliza o dataset `br_inep_avaliacao_alfabetizacao` nativamente no BigQuery. Não existe uma via equivalente e oficial dessa fonte dentro do ecossistema AWS, então consultar o BigQuery é o caminho direto até a fonte pública, em vez de replicar/hospedar esses dados manualmente antes.
- **Por que persistir em S3, e não deixar os dados no BigQuery?** Porque o restante da stack do projeto (Glue, Kafka, camadas Bronze/Silver/Gold, futuros dashboards e modelos) foi decidido em AWS. Trazer os dados para o S3 logo após a extração evita fragmentar a arquitetura em duas nuvens de forma permanente — o GCP é usado apenas como *ponte* de ingestão, não como parte do Data Lake.
- **Custo:** as consultas via `basedosdados`/`bd.read_sql` utilizam a cota gratuita de processamento de queries do BigQuery (não há custo de armazenamento ou de cluster ficando ativo no GCP); o projeto de billing é usado apenas para autenticação/quota da extração pontual, sem gerar cobrança recorrente.
- **Trade-off explícito:** manter duas nuvens aumenta a superfície de configuração e de controle de acesso (uma conta/IAM na AWS e um projeto de billing no GCP), mas evita o custo de reimplementar, dentro da AWS, uma fonte de dados que já existe pronta, pública e mantida por terceiros no BigQuery.

**Data Lake vs. Data Warehouse**
Escolhemos Data Lake (S3 + Parquet) em vez de um Data Warehouse gerenciado. Justificativa: o volume de dados do projeto é pequeno/médio e não justifica o custo fixo de um DW; o S3 permite consumo tanto por Glue/Spark quanto por ferramentas de BI ou notebooks de ML diretamente sobre os arquivos, sem duplicar dados.

**Chave de negócio completa no dedup da Silver (lição aprendida rodando com dado real)**
Inicialmente o dedup da Silver usava chaves incompletas em duas tabelas: `uf`/`municipio` deduplicavam só por `ano+sigla_uf`/`ano+id_municipio` (ignorando `serie`/`rede`), e `alunos` deduplicava só por `id_aluno` (ignorando `ano`). Rodando com dado real, isso causou perda silenciosa de dado de verdade: `uf` perdia ~66% das linhas (múltiplas redes por UF colapsadas em uma só), e `alunos` perdia ~39% das linhas (o mesmo `id_aluno` se repete entre 2023 e 2024 — não é o mesmo estudante, é um identificador reaproveitado a cada edição da avaliação). Corrigido usando a chave de negócio **completa** em cada tabela (`CHAVE_NEGOCIO` no notebook Silver), validada com testes automatizados que reproduzem o cenário de perda antes/depois da correção.

**Custo vs. Performance**
- Worker `G.1X` (o menor perfil disponível no Glue) foi suficiente para o volume atual, evita pagar por capacidade computacional ociosa.
- Parquet particionado reduz custo de leitura em queries futuras (leitura seletiva de colunas/partições).
- Jobs Bronze processam todos os datasets em um único job (loop), em vez de um job por dataset — reduz overhead de start-up do Glue (cada job tem custo mínimo de inicialização).

## 6. Regras de qualidade de dados (Data Quality)

Cada tabela, em cada camada (Bronze, Silver e Gold), passa por checks antes da persistência:

- **Verificação de duplicidade**, por chave de negócio completa (não linha inteira nem coluna única).
- **Detecção de valores ausentes** em colunas-chave (`sigla_uf`, `id_municipio`, `ano`, `ranking`).
- **Validação de intervalo** (`taxa_alfabetizacao` e `media_portugues` entre 0 e 100).
- **Validação de volume mínimo** (contagem mínima de registros por dataset, calculada a partir da cardinalidade real dos dados).
- **Integridade referencial** (`checar_integridade_referencial`): valida se toda chave estrangeira (ex.: `id_municipio` de um aluno) existe na tabela de referência. Roda no mesmo lugar onde o join acontece — na Silver, para o join territorial (alunos+município+UF); na Gold, para os joins de enriquecimento externo (Fase 3).
- **Consistência entre tabelas** (`checar_consistencia_territorial`): valida se a UF derivada de um município bate com uma UF real da tabela de referência.

Cada check tem um campo `critico` (`True`/`False`): falhas críticas interrompem o job (`raise`); falhas não-críticas viram alerta (`WARN`) no log e o pipeline segue — usado para casos em que um percentual pequeno de inconsistência é esperado (ex.: alguns municípios sem cobertura de INSE) e não deve travar toda a execução.

## 7. Monitoramento

- **Logging estruturado** em todas as camadas (local com `logging`, em nuvem com o logger nativo do Glue/CloudWatch), registrando: início/fim de cada etapa, contagem de registros processados, sucesso/falha por dataset.
- **Métricas em formato campo=valor** (`log_metrica`), consultáveis via CloudWatch Logs Insights (ex.: `filter evento = "tabela_processada"`), com latência e volume por tabela — não só texto solto em linha de log.
- **Alertas** (`emitir_alerta`): toda falha (por tabela ou agregada ao fim do pipeline) é logada em nível `ERROR` e, se um tópico SNS estiver configurado (`SNS_TOPIC_ARN`), publica uma notificação real.
- **Isolamento de falha por tabela**: cada tabela roda dentro de um `try/except` individual nas 3 camadas — a falha de uma tabela não impede o processamento das demais.
- Os logs ficam disponíveis no **CloudWatch**, permitindo rastrear volume processado e detectar falhas de ingestão sem acesso direto ao cluster.

## 8. FinOps — otimização de custos

- **Parquet + particionamento**: reduz volume de I/O e custo de storage frente a formatos não comprimidos (CSV/JSON).
- **Glue serverless com worker mínimo (G.1X)**: paga-se apenas pelo tempo de execução, sem cluster fixo, e o perfil de worker foi dimensionado para o volume real de dados (evitando superdimensionamento).
- **Um job por camada, processando todos os datasets em loop**: menos jobs = menos overhead de start-up do Glue = menor custo agregado.
- **Camada Bronze como cache intermediário**: evita reprocessar a extração da fonte original a cada rodada de Silver/Gold, economizando chamadas repetidas à fonte de dados.

## 9. Aplicação em IA

A camada Gold foi desenhada para servir de base a análises mais avançadas:

- **Modelo supervisionado de previsão de alfabetização (Fase 3, já implementado)**: `alunos_alfabetizacao` combina dados do aluno, território, PIB, indicadores educacionais e nível socioeconômico (INSE) para prever `alfabetizado`, com tratamento explícito de data leakage. Ver seção 3.1.
- **Análise de desigualdade educacional**: comparações entre redes de ensino (`resumo_rede`) e entre UFs (`ranking_uf`), e entre meta e resultado (`comparacao_meta_uf`/`comparacao_meta_municipio`), para embasar políticas públicas com evidências.
- **Clusters de vulnerabilidade educacional**: agrupando municípios por padrões de desempenho, PIB per capita e nível socioeconômico.

## 10. Estrutura do repositório

```
docs/
    apresentacao/
        video.mp4
        apresentacao.ppt
    evidencias/
        fotos tiradas do S3 para evidências do uso do AWS
  modelo_dados.md
notebooks/
  cloud/
    tech_challenge_aws_etl_bronze.ipynb
    tech_challenge_aws_etl_silver.ipynb
    tech_challenge_aws_etl_gold.ipynb
  kafka/
    tech_challenge_kafka_streaming.ipynb
infra/
  README.md            # infraestrutura como código (Terraform) - ver nota abaixo
  *.tf
  glue_scripts/
  diagnostico_*.sql, diagnostico_*.py   # scripts de diagnóstico usados durante o desenvolvimento
README.md
```

> **Nota sobre `infra/`**: os scripts em `infra/glue_scripts/` são gerados a partir dos notebooks em `notebooks/cloud/` e podem ficar desatualizados se os notebooks forem editados depois. Regenerar antes de aplicar o Terraform (`jupyter nbconvert --to script`).