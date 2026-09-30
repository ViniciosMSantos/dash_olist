<img width="1921" height="941" alt="Captura de tela 2026-09-30 123617" src="https://github.com/user-attachments/assets/5e6c1b8d-a4d8-44ad-8254-70bec209b9ab" />
# Dashboard Olist

Dashboard de operações da Olist (marketplace de e-commerce brasileiro) construído como **Google Apps Script Web
App**, consultando dados diretamente do Databricks. Cobre visão geral do negócio, vendas, logística/entrega e
satisfação/avaliações dos clientes.

**[🔴 Ver dashboard ao vivo](https://script.google.com/macros/s/AKfycbygFktd1khKSfrZB0dK_w9R0jw80RJuQ5Qg1ZGBkCe6mLsEObQAUn96wl5-_uQ48-cBPw/exec)**

## Estado atual do dashboard

5 páginas implementadas: **Visão Geral → Receita & Performance → Vendas → Logística → Satisfação**. Cada página é
guiada por uma sequência de perguntas (menu lateral) e responde com KPIs + gráficos.

### Visão Geral
*Como está a saúde geral do negócio agora, e o que merece atenção?*

- **KPIs**: Faturamento, Margem, Ticket médio, Nota média, Taxa de atraso, Taxa de cancelamento.
- **Perguntas**: Como está o negócio? · O que merece atenção? · Como está o resultado? · O que está gerando o
  resultado? · A operação sustenta as vendas? · Como o cliente percebe isso? · Onde investigar agora?
- **Visualizações**: ranking de principais pontos de atenção (score por magnitude × peso × persistência);
  evolução mensal de faturamento, custo de frete e margem; volume × ticket (o que explicou a variação do
  faturamento); evolução de pedidos e taxa de recompra; envio × entrega × atraso; taxa de atraso × nota média;
  resumo por área.

### Receita & Performance
*Como o resultado financeiro evoluiu e o que está por trás disso?*

- **KPIs**: Faturamento total, Receita líquida, Custo de frete, Margem %, Ticket médio.
- **Perguntas**: Como está a evolução financeira? · Quais categorias estão movimentando o faturamento? · Quais
  categorias sustentam o faturamento e como? · Onde estão as maiores mudanças? · Como está a composição do
  faturamento? · Como as categorias geram receita?
- **Visualizações**: receita líquida e custo de frete ao longo do tempo; receita líquida ÷ faturamento total;
  categorias e cidades que mais mudaram o faturamento; curva de Pareto de faturamento por categoria; composição
  do faturamento por categoria e por cidade; pedidos × ticket por categoria.

### Vendas
*O que está gerando as vendas e a receita?*

- **KPIs**: Vendedores ativos, Clientes ativos, Novos clientes, Clientes recorrentes, Pedidos, Pedidos por
  vendedor.
- **Perguntas**: Como está o cenário comercial? · Como a geração de vendas está distribuída? · Como os vendedores
  estão performando entre si? · Como as carteiras de clientes estão distribuídas? · Como está o comportamento da
  base de clientes? · Onde estão os principais mercados de venda? · Como diferentes perfis de clientes contribuem
  para as vendas? · Quão concentrado está o resultado comercial? · Onde investigar?
- **Visualizações**: distribuição da geração de vendas entre vendedores; clientes atendidos × pedidos por
  vendedor; tamanho e concentração da carteira dos vendedores; frequência de compra × intervalo entre compras;
  onde a operação vende (pedidos e clientes por mercado); pedidos por perfil (novos × recorrentes); curva de
  concentração do resultado comercial; painel de sinais comerciais.

### Logística
*A operação consegue sustentar o volume de vendas?*

- **KPIs**: Pedidos entregues, % no prazo, Tempo total (mediana), Tempo total (P90), Frete por pedido, Pedidos em
  andamento.
- **Perguntas**: Como a operação está evoluindo? · Estamos cumprindo o que prometemos? · Onde está o gargalo do
  processo? · Onde a operação vai melhor e pior? · Quanto custa entregar e o que o cliente recebe? · Há pedidos
  parados agora? · Onde investigar?
- **Visualizações**: evolução da operação (volume, velocidade e cumprimento); entrega real × data estimada;
  prazo prometido × tempo realizado por região; tempo em cada etapa (no prazo × atrasados); distribuição do
  tempo total de entrega; onde a operação vai melhor e pior (por região); frete e tempo de entrega por região;
  nota do cliente por nível de cumprimento; pedidos parados e pedidos perdidos; painel de sinais logísticos.

### Satisfação
*Como o cliente percebe a experiência de compra?*

- **KPIs**: Nota média, % positivas (4–5★), % insatisfeitas (1–3★), Clientes insatisfeitos, Clientes avaliadores,
  % com comentário.
- **Perguntas**: Estamos melhorando ou piorando? · Como estão distribuídas as experiências? · Onde está a
  insatisfação? · O que está associado à percepção do cliente? · Qual é o tamanho do problema? · O que CX precisa
  explicar?
- **Visualizações**: evolução da satisfação; distribuição das notas; onde a insatisfação se concentra;
  insatisfeitas por nível de prazo; o que os clientes insatisfeitos escrevem (comentários); tamanho do problema;
  o que CX precisa explicar.

Todas as páginas seguem o mesmo padrão de leitura: KPI com variação vs. mês anterior → gráfico de evolução →
tabela com o dado do gráfico → painel de sinais/pontos de atenção da área. Comparações usam sempre o mês
selecionado vs. o mês anterior (ou os mesmos dias, quando o mês está em andamento).

### Planejado, ainda não implementado

- **Como ler o dashboard**: página de mapa/onboarding explicando a sequência das páginas e como ler cada
  componente (KPI, tooltip, tabela) — hoje é conteúdo estático, sem dado envolvido.
- **Insights**: página de síntese cruzando as demais (maiores variações do período, problemas persistentes em
  3+ meses, concentração de problemas por categoria/região, relações entre indicadores como atraso × nota) —
  apresenta sinais e perguntas de investigação, sem prescrever ação.

## Limitações dos dados

As 4 tabelas gold (`pedidos`, `itens_pedidos`, `pagamentos`, `avaliacoes`) cobrem 2016-09 a 2018-08, com volume
real a partir de 2017-01 — por isso o menu de período do dash só mostra os 2 meses mais recentes com pelo menos
100 pedidos, e comparação com o mesmo mês do ano anterior só é possível pros últimos meses do histórico.

O que os dados **não têm** (não inventar ao interpretar um número):

- **Custo do produto (COGS)** — não existe margem de lucro real, só a composição faturamento/frete (ver
  [GENIE_CONTEXTO.md](GENIE_CONTEXTO.md) para as definições exatas de margem/receita líquida/custo de frete).
- **Custo real de logística/entrega** — "custo de frete" é o frete que o cliente pagou, não o custo do Olist.
- **Nome de produto** (só categoria) e **nome de cliente/vendedor** (só ids).
- **Causalidade** — os dados mostram associação (ex.: atraso × nota), não causa.
- **Nota por categoria** de forma direta — um pedido pode ter itens de várias categorias; a atribuição da nota
  por categoria exige uma regra (por item), não é 1:1.

## Stack

- **Frontend**: Google Apps Script HTML Service (`index.html`, `Stylesheet.html`, `JavaScript.html`), servidos via
  `include()`. SPA simples com seções `.view` e navegação por `data-view` na sidebar.
- **Backend**: `Code.gs` (Apps Script / V8). Conecta ao Databricks via Statement Execution API, com cache em
  `CacheService`.
- **Fonte de dados**: tabelas gold no Databricks (`dados_prod.gold.gld_olist_*`: pedidos, itens de pedidos,
  pagamentos, avaliações).
- **Gráficos**: [Chart.js 4](https://www.chartjs.org/) + `chartjs-plugin-datalabels`, via CDN.
- **Fonte tipográfica**: [Inter](https://fonts.google.com/specimen/Inter) (Google Fonts).
- **Deploy**: [`clasp`](https://github.com/google/clasp), webapp com acesso restrito a `MYSELF`.

Existe também um protótipo/paralelo em Power BI (`Dash Power bi.pbix`, fora do controle de versão — ver
`.gitignore`).

## Estrutura do repositório

```
Code.gs              Backend Apps Script: queries ao Databricks, cache, montagem das respostas por página
index.html            Shell da SPA (sidebar, seções .view)
Stylesheet.html        Design tokens e estilos (fonte de referência de layout para outros dashboards)
JavaScript.html        Lógica de front-end: navegação, fetch dos dados, renderização dos gráficos
appsscript.json         Manifesto do Apps Script
.clasp.json             Configuração do clasp (script id do projeto)
dados/
  import.py             Baixa as 4 tabelas gold do Databricks para Parquet local (exploração)
  .env.example           Template das variáveis de ambiente do Databricks (copiar para .env, nunca commitar)
CLAUDE.md               Guia do projeto para desenvolvimento assistido por IA (stack, padrões, gotchas)
GENIE_CONTEXTO.md        Glossário e regras de negócio para o espaço Genie (Databricks) responder perguntas sobre os dados
GENIE_PERGUNTAS_TESTE.md Perguntas de aprofundamento para testar o espaço Genie além do que o dashboard já mostra
```

## Desenvolvimento local

O Apps Script não roda localmente. Para explorar os dados antes de implementar uma query nova:

```bash
cd dados
cp .env.example .env   # preencher DATABRICKS_SERVER_HOSTNAME / HTTP_PATH / TOKEN
python import.py       # baixa as 4 tabelas gold para .parquet
```

`dados/.env` nunca é commitado (está no `.gitignore`).

## Deploy

```bash
clasp push
```

O webapp é implantado com acesso `MYSELF` (uso pessoal).

## Definições de negócio

Nomes e fórmulas usados no dashboard (faturamento, receita líquida, custo de frete, margem, taxa de atraso, taxa
de recompra, etc.) estão documentados em [GENIE_CONTEXTO.md](GENIE_CONTEXTO.md), que serve tanto de referência de
negócio quanto de contexto para o espaço Genie do Databricks.
