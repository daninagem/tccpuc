# Projeto de analise exploratória dos dados PNAD-C do IBGE para o curso: "Psicologia organizacional e gestão de pessoas" - PUCRS/2026" 📊
---
# TCCPUC
Repositórios e documentos do trabalho
![Banner PAI](https://github.com/daninagem/tccpuc/blob/main/docs/banner.png)

## 📌 Descrição do Projeto
---
Este repositório reúne os dados, scripts e análises desenvolvidos no contexto do Trabalho de Conclusão de Curso (TCC) “Diversidade, Inclusão e Equidade nas Organizações Brasileiras: preparação para a força de trabalho futura sob a lente da interseccionalidade”, Artigo Científico desenvolvido por Daniele Silva da Cruz Nagem (PUC-RS).

A base analítica e todo o processo de tratamento, modelagem e análise exploratória de dados (EDA) foram conduzidos com o objetivo de fornecer evidência empírica sólida, própria e baseada em dados oficiais, utilizando a PNAD Contínua (IBGE).

O estudo investiga como desigualdades de sexo e raça/cor se manifestam no mercado de trabalho brasileiro, evitando ao máximo dependência de estudos secundários e reduzindo vieses interpretativos, ao priorizar dados públicos, metodologia transparente e análise própria.

---
## 📓 Notebook do Projeto

Acesse o notebook completo

[![Ver Notebook](https://img.shields.io/badge/Notebook-PNADC%20IBGE-blue?logo=jupyter)](https://github.com/daninagem/tccpuc/blob/d17fc1bda71fced8e0f75250e9928e87356d7cb1/notebook/notebook_jupyter.ipynb)

Ou você também pode abrir no Colab

[![Você também pode abrir no Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/daninagem/tccpuc/blob/main/notebook/notebook_jupyter.ipynb) <br>

## 🌐 Experiência Interativa

Acesse o notebook completo com execução online via Binder:

  <a href="https://mybinder.org/v2/gh/daninagem/tccpuc/main?labpath=notebook/notebook_jupyter2.ipynb">
    <img src="https://mybinder.org/badge_logo.svg" alt="Abrir no Binder">
  </a>

## 🎯 Objetivo
---
O principal objetivo deste projeto é corroborar, por meio de dados oficiais e análise quantitativa, as discussões do TCC na área de Psicologia Organizacional e Gestão de Pessoas, trazendo uma base concreta para temas frequentemente tratados de forma subjetiva.

A análise busca:

- Identificar padrões de desigualdade interseccional (sexo × raça/cor)
- Avaliar impactos em diferentes dimensões do mercado de trabalho
- Apoiar a construção de argumentos com evidência estatística robusta
- Reduzir viés político, opinativo ou não fundamentado
- Fortalecer a qualidade científica e a propriedade intelectual do trabalho

## ⚙️ Arquitetura do Projeto
---
O projeto foi estruturado em duas etapas principais:

## 🧱 1. Construção da Base Analítica
Extração de dados da PNAD Contínua (BigQuery) <br>
Padronização de schema (tratamento de aliases entre anos) <br>
Limpeza e validação de dados essenciais <br>
Aplicação de deflator (renda real) <br>
Criação de variáveis analíticas: grupo interseccional (sexo × raça/cor), escolaridade ocupação (CBO), setor (CNAE) <br>
Consolidação em dataset único (parquet) <br>
Geração de manifesto técnico por ano (controle de qualidade) <br>

✔ Garantia de:

Consistência entre anos <br>
Rastreabilidade dos dados <br>
Integridade da variável renda (ajustada por inflação) <br>

## 📊 2. Análise Exploratória e Teste de Hipóteses (EDA)
Leitura otimizada da base consolidada <br>
Uso de pesos amostrais (IBGE) em todos os cálculos <br>
Geração de: tabelas analíticas (CSV), gráficos (PNG), relatório textual automático <br>

✔ Características da análise:

Baseada em médias e proporções ponderadas <br>
Separação entre análise real e ajustes visuais <br>
Estrutura modular por hipótese <br>

## 📊 Perguntas feitas a hipóteses Investigadas
---
🔷 H1 — Renda por escolaridade <br>
📌 A educação aumenta a renda para todos os grupos da mesma forma, ou a desigualdade persiste mesmo em níveis mais altos de escolaridade? <br>

🔷 H2 — Retorno da escolaridade <br>
📌 O retorno da escolaridade (ganho adicional de renda por nível educacional) é igual entre os grupos, ou se torna mais desigual nos níveis mais altos? <br>

🔷 H3 — Estrutura ocupacional <br>
📌 Diferentes grupos estão distribuídos de forma semelhante entre as ocupações, ou existe segregação ocupacional? <br>

🔷 H4 — Formalização <br>
📌 O aumento da escolaridade leva à formalização do trabalho de forma igual para todos os grupos, ou a desigualdade permanece? <br>

🔷 H5 — Estrutura ocupacional + renda <br>
📌 As diferenças de renda entre grupos são explicadas pelo tipo de ocupação, ou persistem mesmo dentro das mesmas ocupações? <br>

🔷 H6 — Jovens no mercado de trabalho <br>
📌 Os Jovens de hoje ainda enfrentam as mesmas desigualdades do passado? <br>

🔷 H7 — Cargos de liderança <br>
📌 O acesso a cargos de liderança é distribuído de forma proporcional entre os grupos, ou existe concentração em grupos específicos? <br>

## 📂 Raiz do Repositório
---
├── 📂 dataset/ → #Bases de dados consolidadas <br> 
├── 📂 notebook/📓notebook_jupyter.ipynb → [![Você também pode abrir no Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/daninagem/tccpuc/blob/main/notebook/notebook_jupyter.ipynb) <br>
├── 📂 dicionario/ → #Documentos importantes para consolidar os dados <br> 
├── 📂 doc/ → # Documentos do projeto <br>
├── 📂 outputs/ → # Saída de tabelas, figuras e relatórios da analise exploratória de dados <br>
├── 📜 README.md/ → # Descrição geral do projeto <br>
├── 📜 requirements.txt/ → # Dependências do projeto <br>
├── 🐍 build_base_download.py/ → # Baixa e prepara a base consolidada para o script de analise exploratória <br>
├── 🐍 eda.py/ → # Analisa a base consolidada <br>

## Estrutura principal do Repositório
---
├── dataset/ <br>
│   └── base_consolidada.parquet <br>
│ <br>
├── output/ <br>
│   ├── tables/ <br>
│   ├── figures/ <br>
│   ├── reports/ <br>
│   └── relatorio_principais_achados.txt <br>
│ <br>
├── notebook/ <br>
│   └── notebook_jupyter.ipynb <br>
│ <br>
├── dicionario/ <br>
│   └── dicionario_PNADC_microdados_trimestral.xls <br>
│   └── Estrutura_Atividade_CNAE_Domiciliar_2_0.xls <br>
│   └── Estrutura_Ocupacao_COD.xls <br>
│   └── deflator_PNADC_2025_trimestral_070809.xls <br>
│   └── input_PNADC_trimestral.sas <br>
│ <br>
├── build_base_download.py <br>
├── eda.py <br>
├── README.md <br>
├── requirements.txt <br>


## 🛠 Tecnologias que estamos usando:
---
- Python
- Pandas
- NumPy
- Matplotlib
- Jupyter Notebook
- Git/GitHub
- Google BigQuery
- Parquet
- Pathlib
- Json
- Argparse

## 🚀 Como Usar
---
1. Clonar este repositório:  
   ```bash
   #git clone https://github.com/daninagem/tccpuc.git

2. Dependências   

- pandas
- numpy
- matplotlib
- pyarrow
- google-cloud-bigquery
- db-dtypes
- openpyxl
- tqdm

3. Resolver dependências   
   ```bash
   #pip install -r requirements.txt

## 📈 Fonte de Dados
---
IBGE. Pesquisa Nacional por Amostra de Domicílios Contínua (PNAD Contínua). Rio de janeiro: IBGE, [2025] 

## 🔗 Link da base
(https://www.ibge.gov.br/estatisticas/sociais/trabalho/9173-pesquisa-nacional-por-amostra-de-domicilios-continua.html?=&t=downloads)

## 🧠 Diferencial do Projeto
---
Este trabalho se diferencia por:

Construção de base própria (não derivada de estudos terceiros)
Uso exclusivo de dados oficiais
Transparência total da metodologia
Separação clara entre:
dado
interpretação
evidência

✔ Resultado:
Uma análise replicável, auditável e tecnicamente sustentada

## 🚀 Finalidade
---
Este repositório serve como:

Base técnica do TCC
Suporte quantitativo para pesquisa em ciências humanas
Exemplo de aplicação de ciência de dados em temas sociais
Referência para estudos sobre desigualdade no Brasil

## 🛤 Roadmap do Projeto
---
Este roadmap apresenta as principais fases do projeto e seus marcos importantes.

### 📌 Etapas do Projeto
---
1️⃣ Etapa 1: Definição do Problema e Planejamento 📋 (Jan/2026)
- Definição do tema: diversidade, inclusão e equidade no mercado de trabalho
- Escolha da base de dados: PNAD Contínua (IBGE)
- Definição das variáveis iniciais (sexo, raça/cor, escolaridade, renda e região)
- Estruturação do objetivo analítico do projeto
---
2️⃣ Etapa 2: Construção da Base Analítica ⚙️ (Jan → Fev/2026)
- Extração de dados via BigQuery
- Padronização de schema entre diferentes anos
- Tratamento e limpeza dos dados
- Aplicação de deflator (renda real)
- Criação de variáveis analíticas (grupo interseccional, ocupação, formalização)
- Consolidação da base em formato parquet
---
3️⃣ Etapa 3: Análise Exploratória Inicial (EDA v1) 🔍 (Fev/2026)
- Análise inicial de renda, ocupação e formalização
- Geração de gráficos e tabelas preliminares
- Identificação de padrões e desigualdades iniciais
- Avaliação da variável região → menor poder explicativo
- Redirecionamento do foco para sexo × raça/cor (interseccionalidade)
---
4️⃣ Etapa 4: Formulação e Refinamento das Hipóteses 🧠 (Fev → Mar/2026)
- Estruturação das hipóteses H1–H7
- Ajustes conceituais com orientação acadêmica
- Apoio metodológico da equipe da biblioteca
- Organização da análise por hipóteses
---
5️⃣ Etapa 5: Refinamento Metodológico Crítico 🔧 (Mar/2026)
- Revisão da análise ocupacional
- Identificação de distorção: agrupamento de gerência + diretoria
- Separação dos níveis hierárquicos
- Criação da hipótese específica sobre cargos de liderança (H7)
- Melhoria da precisão analítica dos resultados
---
6️⃣ Etapa 6: Consolidação dos Resultados 📊 (Mar/2026)
- Geração de tabelas analíticas (CSV)
- Geração de gráficos (PNG)
- Criação de relatório textual automatizado
- Validação e coerência dos resultados entre hipóteses
---
7️⃣ Etapa 7: Integração com Referencial Teórico 📚 (Mar/2026)
- Inclusão de estudos complementares (ex.: governança corporativa – Spencer Stuart)
- Contextualização dos achados sobre liderança
- Integração entre análise quantitativa e discussão acadêmica
---
8️⃣ Etapa 8: Finalização e Documentação 🚀 (Mar/2026)
- Estruturação do repositório no GitHub
- Criação e refinamento do README
- Organização dos scripts e outputs
- Preparação para entrega e defesa do TCC
---
## 🎯 Milestones Importantes
- 📅 Jan/2026 → Primeira versão do projeto submetida
- 📅 Jan/2026 → Definição da PNAD Contínua como base oficial
- 📅 Fev/2026 → Base analítica consolidada em parquet
- 📅 Fev/2026 → Identificação de menor impacto da variável região
- 📅 Fev/2026 → Redefinição do foco para análise interseccional (sexo × raça/cor)
- 📅 Mar/2026 → Estruturação final das hipóteses (H1–H7)
- 📅 Mar/2026 → Ajuste crítico na análise ocupacional (separação gerência vs diretoria)
- 📅 Mar/2026 → Criação da hipótese de cargos de liderança (H7)
- 📅 Mar/2026 → Consolidação dos outputs (tabelas, gráficos e relatório)
- 📅 Mar/2026 → Integração com literatura (governança corporativa)
- 📅 Mar/2026 → Repositório final estruturado e documentado
