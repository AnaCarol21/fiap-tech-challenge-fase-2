# Tech Challenge – Fase 2

## Pipeline Híbrida para Análise da Alfabetização no Brasil

Projeto desenvolvido para o Tech Challenge da Pós-Tech FIAP (Fase 2), com o objetivo de construir uma pipeline híbrida de dados (Batch + Streaming) para integração, tratamento e disponibilização de dados públicos sobre alfabetização infantil no Brasil, seguindo a Arquitetura Medalhão (Bronze, Silver, Gold) em ambiente de nuvem.

---

## 1. Contexto do problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico do país. Nesse cenário, o **Compromisso Nacional Criança Alfabetizada** mobiliza União, estados, Distrito Federal e municípios com o objetivo de garantir que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental.

Para apoiar essa política, o **INEP (Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira)** realizou em 2023 a **Pesquisa Alfabetiza Brasil**, que definiu o ponto de corte de **743 pontos** na escala de proficiência do Saeb — a partir do qual uma criança é considerada alfabetizada. Com base nesse parâmetro, foi criado o **Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que atingem esse patamar. A meta nacional é alfabetizar 100% das crianças até 2030.

Compreender os fatores que influenciam a alfabetização exige integrar diferentes fontes de dados: metas nacionais, estaduais e municipais, dados territoriais, microdados educacionais e indicadores de desempenho, em vez de olhar cada indicador isoladamente.

## 2. O desafio

Construir uma pipeline híbrida (batch + streaming) que:

- integre as seis entidades de dados da plataforma **Base dos Dados** (UF, Município, Meta Alfabetização Brasil, Meta Alfabetização por UF, Meta Alfabetização por Município e Dados de Alunos);
- padronize, trate e valide essas informações;
- disponibilize uma camada analítica confiável (Gold) para dashboards, análises estatísticas e futuros modelos de machine learning;
- rode em nuvem (AWS), com foco em escalabilidade, qualidade de dados e controle de custos (FinOps).

## 3. Arquitetura da solução

A pipeline segue a **Arquitetura Medalhão**, com ingestão híbrida convergindo para as três camadas:

**Fluxo de dados:**

1. **Ingestão Batch** — extração das 6 tabelas da Base dos Dados (metas nacionais/estaduais/municipais, município, UF, alunos), carregadas como Parquet em `s3://.../raw/`.
2. **Ingestão Streaming** — um Producer Kafka simula eventos quase em tempo real (atualização de indicadores, novas medições de desempenho, atualização de metas), consumidos por um Consumer que persiste os eventos em `bronze_stream.parquet`.
3. **Camada Bronze** — dados brutos das duas ingestões, sem transformação significativa, com histórico completo preservado. No AWS Glue, um job (`glue_etl_bronze`) lê cada dataset de `raw/` e grava em `bronze/<dataset>/`, com logging de contagem de registros e tratamento de falha por dataset.
4. **Camada Silver** — limpeza, padronização de tipos e nomes, tratamento de nulos, remoção de duplicidade e **integração entre as bases** (joins entre alunos, município e UF). Executada localmente em Pandas (protótipo) e replicada em PySpark no Glue (`glue_etl_silver`).
5. **Camada Gold** — datasets analíticos prontos para consumo: ranking de UFs, ranking de municípios, evolução temporal do indicador por UF e resumo agregado por rede de ensino. Executada em Pandas localmente e em PySpark no Glue (`glue_etl_gold`), com checks de qualidade de dados antes da persistência.
6. **Consumo** — a camada Gold está pronta para alimentar dashboards de BI e, futuramente, modelos preditivos de machine learning.

### Camadas em detalhe

**Bronze — dados brutos**
- Sem transformação de negócio, apenas padronização estrutural (schema, tipos básicos).
- Histórico completo preservado (nenhum dado é descartado nesta camada).
- Hash de registros para rastreabilidade e detecção de duplicidade futura.

**Silver — dados tratados**
- Limpeza (remoção de duplicidade, tratamento de nulos essenciais).
- Padronização de nomes de colunas e tipos.
- Normalização de chaves (`sigla_uf`, `id_municipio`) para permitir joins consistentes.
- Integração entre as bases (alunos + município + UF).

**Gold — camada analítica**
- `ranking_uf`: ranking de UFs por taxa de alfabetização, por ano e rede de ensino.
- `ranking_municipio`: mesmo ranking, no nível de município.
- `evolucao_uf`: série histórica da taxa de alfabetização e proficiência em português por UF.
- `resumo_rede`: agregados (médias e contagem de UFs) por ano e rede de ensino (Federal, Estadual, Municipal, Privada).

## 4. Tecnologias utilizadas

| Tecnologia | Uso no projeto | Justificativa |
|---|---|---|
| **Python + Pandas** | Protótipo local das camadas Bronze/Silver/Gold | Iteração rápida e validação da lógica de negócio antes de portar para Spark, sem custo de cluster |
| **PySpark (AWS Glue)** | Execução em nuvem das mesmas camadas | Processamento distribuído, nativo ao ambiente Glue, sem gerenciar infraestrutura de cluster |
| **Apache Kafka** | Ingestão streaming (Producer/Consumer) | Padrão de mercado para eventos quase em tempo real; simula atualizações de indicadores e metas |
| **Amazon S3** | Data Lake (raw, bronze, silver, gold) | Armazenamento barato, durável e particionável em Parquet, desacoplado do processamento |
| **AWS Glue** | Orquestração e execução das transformações em nuvem | Serverless (paga por uso), integração nativa com S3 e IAM, sem provisionar servidores |
| **Parquet** | Formato de persistência em todas as camadas | Colunar, comprimido, com leitura seletiva de colunas — reduz custo de storage e de leitura |
| **IAM Roles** | Controle de acesso (`AWSGlueServiceRole-TechChallenge`) | Princípio do menor privilégio: acesso restrito ao bucket do projeto e ao serviço Glue |

## 5. Decisões arquiteturais (trade-offs)

**Batch vs. Streaming**
Optamos por um modelo híbrido em vez de escolher só um dos dois. As metas e microdados educacionais (INEP/Base dos Dados) são publicados em ciclos (anual/periódico) batch é suficiente e mais barato para essas fontes. Já a simulação de atualizações de indicadores e resultados exige baixa latência de disponibilização, daí o uso de Kafka para essa fatia do problema, sem forçar todo o pipeline a rodar em streaming (o que encareceria a solução sem necessidade real).

**Data Lake vs. Data Warehouse**
Escolhemos Data Lake (S3 + Parquet) em vez de um Data Warehouse gerenciado. Justificativa: o volume de dados do projeto é pequeno/médio e não justifica o custo fixo de um DW; o S3 permite consumo tanto por Glue/Spark quanto por ferramentas de BI ou notebooks de ML diretamente sobre os arquivos, sem duplicar dados.

**Custo vs. Performance**
- Worker `G.1X` (o menor perfil disponível no Glue) foi suficiente para o volume atual, evita pagar por capacidade computacional ociosa.
- Parquet particionado reduz custo de leitura em queries futuras (leitura seletiva de colunas/partições).
- Jobs Bronze processam todos os datasets em um único job (loop), em vez de um job por dataset — reduz overhead de start-up do Glue (cada job tem custo mínimo de inicialização).

## 6. Regras de qualidade de dados (Data Quality)

Cada camada Gold passa por checks antes da persistência:

- **Verificação de duplicidade**
- **Detecção de valores ausentes** em colunas-chave (`sigla_uf`, `id_municipio`, `ano`, `ranking`).
- **Validação de intervalo** (`taxa_alfabetizacao` e `media_portugues` entre 0 e 100).
- **Validação de volume mínimo** (contagem mínima de registros por dataset, calculada dinamicamente a partir da cardinalidade real dos dados, não um valor fixo).

Falhas de qualidade interrompem o job (`assert`), evitando que dados inconsistentes cheguem à camada Gold.

## 7. Monitoramento

- **Logging estruturado** em todas as camadas (local com `logging`, em nuvem com o logger nativo do Glue/CloudWatch), registrando: início/fim de cada etapa, contagem de registros processados, sucesso/falha por dataset.
- Na Bronze em nuvem, falhas em um dataset específico não derrubam o job inteiro — o erro é capturado, logado e reportado ao final, permitindo diagnosticar rapidamente qual fonte falhou.
- Os logs ficam disponíveis no **CloudWatch**, permitindo rastrear volume processado e detectar falhas de ingestão sem acesso direto ao cluster.

## 8. FinOps — otimização de custos

- **Parquet + particionamento**: reduz volume de I/O e custo de storage frente a formatos não comprimidos (CSV/JSON).
- **Glue serverless com worker mínimo (G.1X)**: paga-se apenas pelo tempo de execução, sem cluster fixo, e o perfil de worker foi dimensionado para o volume real de dados (evitando superdimensionamento).
- **Um job por camada, processando todos os datasets em loop**: menos jobs = menos overhead de start-up do Glue = menor custo agregado.
- **Camada Bronze como cache intermediário**: evita reprocessar a extração da fonte original a cada rodada de Silver/Gold, economizando chamadas repetidas à fonte de dados.

## 9. Aplicação em IA

A camada Gold foi desenhada para servir de base a análises mais avançadas:

- **Modelos preditivos de alfabetização por município**: usando `evolucao_uf` e `ranking_municipio` como base histórica para prever a evolução do indicador.
- **Clusters de vulnerabilidade educacional**: agrupando municípios por padrões de desempenho e infraestrutura (especialmente se combinado com fontes externas como Censo Escolar, IBGE e Cadastro Único).
- **Análise de desigualdade educacional**: comparações entre redes de ensino (`resumo_rede`) e entre UFs (`ranking_uf`) para embasar políticas públicas com evidências.

## 10. Estrutura do repositório

```
docs/
    apresentacao/
        video.mp4
        apresentacao.ppt
    evidencias/
        fotos tiradas do S3 para evidências do uso do AWS
  modelo_dados.md         # documentação das tabelas de dados
notebooks/
  cloud/
    tech_challenge_aws_etl_bronze.ipynb
    tech_challenge_aws_etl_silver.ipynb
    tech_challenge_aws_etl_gold.ipynb
  kafka/
    tech_challenge_kafka_streaming.ipynb
README.md
```