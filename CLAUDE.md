# dash_olist

Dashboard de operações da Olist (e-commerce) construído como Google Apps Script Web App. Cobre visão geral, vendas, logística/entrega e satisfação/avaliações.

**Papel deste projeto**: além do dashboard em si, este repo é a base de referência de layout, design e padrões de insight para os outros dashs do usuário. Decisões de estilo, paleta e estrutura de página tomadas aqui devem ser tratadas como o "template" a reaproveitar nos próximos projetos — não são só CSS local.

## Stack

- **Frontend**: Google Apps Script HTML Service (`index.html`, `Stylesheet.html`, `JavaScript.html`), servidos via `include()`. SPA simples com `.view` sections e navegação por `data-view` na sidebar.
- **Backend**: `Code.gs` (Apps Script/V8). Conecta ao Databricks via Statement Execution API (`queryDatabricks`, `queryDatabricksChunked_`), com cache em `CacheService` (`cacheSet_`/`cacheGet_`).
- **Fonte de dados**: tabelas gold no Databricks (`dados_prod.gold.gld_olist_*`: pedidos, itens_pedidos, pagamentos, avaliacoes).
- **Exploração local**: `dados/import.py` baixa as 4 tabelas gold para Parquet local (usa `dados/.env`, nunca commitado) para explorar dados e decidir KPIs antes de implementar as queries definitivas no Apps Script.
- **Gráficos**: Chart.js 4 + `chartjs-plugin-datalabels`, via CDN.
- **Fonte**: Inter (Google Fonts).
- **Deploy**: `clasp` (`.clasp.json`), webapp com acesso `MYSELF`.
- Existe também um `Dash Power bi.pbix` (protótipo/paralelo em Power BI, fora do controle de versão — ver `.gitignore`).

## Documentação do projeto

- **`README.md`**: fonte de verdade do estado atual do dashboard — por página, quais KPIs/perguntas/visualizações
  já existem hoje. **Sempre que uma página ganhar ou perder um gráfico/KPI, atualizar a seção "Estado atual do
  dashboard" do README junto com o código.** (Existia um `MATRIZ_COBERTURA.md` com esse tracking em formato
  pergunta × status; foi consolidado no README e removido — não recriar.)
- **`GENIE_CONTEXTO.md`**: glossário e regras de negócio para o espaço Genie (Databricks) responder perguntas
  sobre os dados, incluindo o contrato de resposta do comentário automático por gráfico (1 frase de
  gargalo/problema + 1 de contexto opcional, sem causalidade, sem prescrever ação).
- **`GENIE_PERGUNTAS_TESTE.md`**: perguntas de aprofundamento que o Genie deve responder além do que o dashboard
  já mostra (não repetir KPI/gráfico existente).

## Padrões de layout e design (referência para outros dashs)

- Design tokens em CSS custom properties no `:root` de `Stylesheet.html` (cores de superfície, texto, grid, séries de gráfico, status, sidebar). Qualquer novo dash deve começar copiando esse bloco de tokens e ajustando os valores, não hardcoding cores soltas.
- Paleta: fundo neutro claro (`--page-plane` bege-claro, `--surface-1` branco), sidebar escura (`--sidebar-bg` quase-preto azulado), cor de marca azul (`--brand`), séries categóricas (`--series-1..2`, `--cat-3..5`, `--cat-outros`), cores de status (`--status-good`, `--status-critical`, `--delta-up-good`).
- Tipografia: Inter, `font-weight: 400` predominante (títulos não usam bold pesado), tamanhos pequenos e hierarquia por cor/uppercase+letter-spacing em labels (`.kpi-label`, `.evolucao-metric-label`).
- Componentes reutilizáveis já padronizados: `.kpi-card`/`.kpi-row` (KPIs com sparkline), `.chart-card`/`.chart-grid` (cards de gráfico, um por linha — cada `chart-card` ocupa a largura toda do `.content`, empilhados verticalmente), `.toggle-group`/`.toggle-btn` (alternância de métrica/dimensão), `.table-toggle` + `.data-table` (ver dado do gráfico em tabela), skeleton loading (`.kpi-card.skeleton`).
- No `.kpi-card-header`, a ordem é sempre `.kpi-label` (nome do KPI) e depois `.kpi-spark-wrap` (sparkline) — o sparkline fica à direita do nome, nunca antes. Preservar essa ordem em novos KPIs/dashs.
- Labels em uppercase (`.kpi-label`, `.evolucao-metric-label`) usam `letter-spacing: 0.07em` — tracking mais largo do que o padrão do navegador, dá o ar "técnico" do dashboard.
- Espaçamento generoso entre blocos: gap de 32px entre `chart-card`s no `.chart-grid`, 18px entre KPIs no `.kpi-row`, 32px abaixo do `.page-header` e do `.kpi-row`. Espaço em branco é a ferramenta de hierarquia, não bordas/sombras pesadas.
- Layout: sidebar fixa 220px + conteúdo com scroll próprio (`.app` flex, `height: 100vh`).
- Exceção deliberada ao "um gráfico por linha": `.chart-row-split` (grid 2 colunas, colapsa pra 1 em telas ≤760px) agrupa um par de gráficos comparáveis lado a lado como um único item dentro do `.chart-grid` (ex.: Faturado vs. Frete). Usar só quando os dois gráficos formam par/comparação direta — não é pra virar padrão geral.
- Padrão "valor + variação MoM": gráfico de barra (valor do mês) + linha tracejada de variação percentual mês a mês, verde (`--status-good`) se subiu / vermelho (`--status-critical`) se caiu, com cor por segmento (`segment.borderColor` do Chart.js). Implementado como helper único (`renderComboValorMoMChart` em `JavaScript.html`) reaproveitado pelos 3 gráficos de evolução — qualquer novo gráfico "valor ao longo do tempo" deveria reusar esse helper em vez de duplicar a config do Chart.js.

## Definições de negócio (nomes no dash)

- Colunas: `valor_total_pedido` = produtos + frete (o total); `valor_total_produtos` = só o preço dos produtos; `valor_total_frete` = frete cobrado do cliente (tratado como custo logístico; não é o custo real da entrega, e não há custo do produto nos dados). Na Visão Geral o card "Faturamento" é o total (pedido); na página Receita & Performance os cards "Receita líquida" (= valor dos produtos, ou seja, faturamento − frete) e "Custo de frete" usam produtos e frete separados (a "margem" é produtos − frete). Evitar o termo "receita" para o total.
- Valores por categoria/cidade (Receita) e por vendedor (Vendas) são vendas de produtos.
- Mês aberto compara com os dias 1–N do mês anterior (`CORTE_MES_ABERTO_`); menu de período mostra só 2 meses; cache de 6h reabastecido por gatilhos (funções do editor: `atualizarDadosDashboard`, `configurarAtualizacaoAutomatica`; funções com `_` no fim não aparecem no editor).

## Fluxo de trabalho

- **Antes de dar uma mudança de layout/estilo como pronta ou de sugerir `clasp push`, sempre gerar e abrir uma prévia estática (HTML standalone com os tokens/CSS reais + dados de exemplo nos gráficos) para o usuário validar visualmente.** O Apps Script não roda localmente, então essa prévia é a única forma de conferir antes do deploy. Confirmado como fluxo preferido pelo usuário.
- A prévia estática não roda o JS real do projeto (é HTML/CSS de exemplo), então erros de runtime do `JavaScript.html`/`Code.gs` só aparecem depois do `clasp push`. Depois de qualquer push, checar `node -e "new Function(fs.readFileSync('JavaScript.html'...))"` (ou similar) pega erros de sintaxe, mas não pega erros de runtime como o do Chart.js abaixo — esses só o usuário vendo o dash ao vivo detecta.

## Gotchas técnicos

- **`montarOverview_` (Code.gs) cacheia a resposta inteira no `CacheService` sob a chave `overview_<tipo>_v<N>_<periodo>`.** Sempre que o formato de `janelaMensal`/`janelaAnual`/`kpis`/etc. mudar (novo campo, campo renomeado/removido), incrementar o `vN` da chave — senão o cache antigo (até `CACHE_EXPIRATION_SECONDS` = 6h) é servido de volta sem o campo novo, e o front-end quebra com "Cannot read properties of undefined" em algum formatter. Já aconteceu (campo `receitaProdutos` novo, cache ainda em `v5`) — corrigido bumping pra `v6`.
- **Chart.js: eixo customizado sem `type` quebra o gráfico inteiro.** Qualquer scale com id fora do padrão (`y1`, `y2`, etc. — diferente de `x`/`y`) precisa de `type: 'linear'` (ou o tipo correto) explícito nas `options.scales`. Sem isso o Chart.js lança um erro na criação do gráfico. Como `renderEvolucaoChart` roda primeiro dentro de `renderCharts` (ver `JavaScript.html`), um erro nele impede TODOS os gráficos seguintes de renderizar (a exceção não tratada interrompe a função síncrona). Se "os gráficos não aparecem" depois de mexer em eixos/escalas, checar primeiro se cada `scales.<id>` custom tem `type` definido.
- **Rótulo de dado some sem erro nenhum se esquecer `display: true`.** O `JavaScript.html` tem `Chart.defaults.set('plugins.datalabels', { display: false })` global — cada dataset/label precisa reativar explicitamente (`datalabels: { display: true, ... }`), inclusive dentro de configs nomeadas (`datalabels.labels.<nome>`). Não dá erro no console, o rótulo simplesmente não aparece. Sempre conferir isso ao criar um gráfico novo com `chartjs-plugin-datalabels`.

## Ideia em andamento

Extrair esses tokens/componentes para uma **skill de "dash"** reutilizável (template de estilo: cores, fontes, componentes de layout) para acelerar a criação de novos dashboards com a mesma identidade visual.

## Segurança

- `dados/.env` contém credenciais do Databricks — nunca ler/expor o conteúdo real, nunca commitar (já está no `.gitignore`).
- `appsscript.json` define acesso do webapp como `MYSELF` — manter assim a menos que o usuário peça explicitamente para mudar.

## Versionamento

Repositório git local, pensado para subir no GitHub. `.gitignore` mantém fora do controle de versão (mas
presentes localmente): `dados/.env` (credenciais), `dados/*.parquet`/`*.csv` (cache de dados baixados) e
`Dash Power bi.pbix` (binário grande, protótipo paralelo). Não commitar sem confirmar com o usuário.
