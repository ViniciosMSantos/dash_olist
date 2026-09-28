// ===== Configuração =====

const TABLES = {
  PEDIDOS: 'dados_prod.gold.gld_olist_pedidos',
  ITENS_PEDIDOS: 'dados_prod.gold.gld_olist_itens_pedidos',
  PAGAMENTOS: 'dados_prod.gold.gld_olist_pagamentos',
  AVALIACOES: 'dados_prod.gold.gld_olist_avaliacoes'
};

// Faturamento (receita/produtos vendidos) só conta pedidos nesses status —
// as demais métricas (taxa de atraso/cancelamento, status dos pedidos) continuam olhando todos os pedidos.
const STATUS_FATURAMENTO = ['Entregue', 'Faturado'];
const CONDICAO_FATURAMENTO = 'status_pedido IN (' + STATUS_FATURAMENTO.map(function (s) { return '\'' + s + '\''; }).join(', ') + ')';

const POLL_INTERVAL_MS = 1000;
const POLL_MAX_ATTEMPTS = 30;

const QUERY_PAGE_SIZE = 20000; // linhas por página ao buscar tabelas inteiras do Databricks
const CACHE_EXPIRATION_SECONDS = 6 * 60 * 60; // 6h, máximo permitido pelo CacheService
const CACHE_CHUNK_CHAR_LIMIT = 45000; // margem de segurança abaixo do limite de 100KB por chave do CacheService (caracteres acentuados ocupam até 2 bytes em UTF-8)

function getDatabricksConfig_() {
  const props = PropertiesService.getScriptProperties();
  const host = props.getProperty('DATABRICKS_HOST');
  const token = props.getProperty('DATABRICKS_TOKEN');
  const warehouseId = props.getProperty('DATABRICKS_WAREHOUSE_ID');
  if (!host || !token || !warehouseId) {
    throw new Error('Configure DATABRICKS_HOST, DATABRICKS_TOKEN e DATABRICKS_WAREHOUSE_ID em Project Settings > Script Properties.');
  }
  return { host: host, token: token, warehouseId: warehouseId };
}

// ===== Conector Databricks (Statement Execution API) =====

function queryDatabricks(sql) {
  if (!sql) {
    throw new Error('queryDatabricks precisa receber uma string SQL. Não rode esta função direto pelo menu Executar — use testConnection() ou describeTodasAsTabelas().');
  }
  // Modo "coleta" (1ª passada de comConsultasEmParalelo_): só anota a consulta e devolve uma linha em branco.
  if (CONSULTAS_COLETA_) {
    CONSULTAS_COLETA_.push(sql);
    return [CONSULTA_LINHA_EM_BRANCO_];
  }
  // Resultado já buscado em paralelo para este mesmo SQL.
  if (CONSULTAS_MEMO_ && Object.prototype.hasOwnProperty.call(CONSULTAS_MEMO_, sql)) {
    return CONSULTAS_MEMO_[sql];
  }
  const inicioConsulta = Date.now();
  const config = getDatabricksConfig_();
  const baseUrl = 'https://' + config.host + '/api/2.0/sql/statements';
  const headers = {
    Authorization: 'Bearer ' + config.token,
    'Content-Type': 'application/json'
  };

  const startResponse = UrlFetchApp.fetch(baseUrl, {
    method: 'post',
    headers: headers,
    payload: JSON.stringify({
      warehouse_id: config.warehouseId,
      statement: sql,
      wait_timeout: '10s'
    }),
    muteHttpExceptions: true
  });

  let result = JSON.parse(startResponse.getContentText());
  if (result.error_code) {
    throw new Error('Databricks error: ' + JSON.stringify(result));
  }

  const statementId = result.statement_id;
  let attempts = 0;
  while (result.status && (result.status.state === 'PENDING' || result.status.state === 'RUNNING')) {
    if (attempts >= POLL_MAX_ATTEMPTS) {
      throw new Error('Timeout aguardando resultado da query no Databricks.');
    }
    Utilities.sleep(POLL_INTERVAL_MS);
    const pollResponse = UrlFetchApp.fetch(baseUrl + '/' + statementId, {
      method: 'get',
      headers: headers,
      muteHttpExceptions: true
    });
    result = JSON.parse(pollResponse.getContentText());
    attempts++;
  }

  if (result.status.state !== 'SUCCEEDED') {
    throw new Error('Query falhou: ' + JSON.stringify(result.status));
  }

  Logger.log('[db] ' + (Date.now() - inicioConsulta) + ' ms (em fila) · ' + String(sql).replace(/\s+/g, ' ').slice(0, 90));
  return rowsToObjects_(result);
}

// ===== Consultas em paralelo =====
// As funções montar…_ (Visão Geral, Vendas, Receita, Logística) fazem 3 a 13 consultas independentes. Em vez de
// esperar cada uma (em fila), rodamos a função duas vezes: na 1ª passada só coletamos os SQLs (queryDatabricks devolve
// uma linha em branco e nada é gravado no cache); depois disparamos todos juntos com UrlFetchApp.fetchAll; na 2ª
// passada queryDatabricks devolve o resultado já pronto. Qualquer SQL que não tenha sido coletado ou que falhe em
// paralelo roda em fila como antes — no pior caso o comportamento é o de sempre.
let CONSULTAS_COLETA_ = null;
let CONSULTAS_MEMO_ = null;
const CONSULTA_LINHA_EM_BRANCO_ = new Proxy({}, { get: function () { return '0'; } });
const POLL_INTERVAL_PARALELO_MS = 700;
const POLL_MAX_ATTEMPTS_PARALELO = 200;

function executarConsultasEmParalelo_(sqls) {
  const resultados = {};
  if (!sqls.length) return resultados;
  const config = getDatabricksConfig_();
  const baseUrl = 'https://' + config.host + '/api/2.0/sql/statements';
  const headers = { Authorization: 'Bearer ' + config.token, 'Content-Type': 'application/json' };

  // wait_timeout '0s': a API devolve o statement_id na hora, sem esperar o resultado.
  const respostas = UrlFetchApp.fetchAll(sqls.map(function (sql) {
    return {
      url: baseUrl, method: 'post', headers: headers, muteHttpExceptions: true,
      payload: JSON.stringify({ warehouse_id: config.warehouseId, statement: sql, wait_timeout: '0s' })
    };
  }));

  let pendentes = [];
  respostas.forEach(function (resp, i) {
    const r = JSON.parse(resp.getContentText());
    if (r.error_code) throw new Error('Databricks error: ' + JSON.stringify(r));
    const estado = r.status && r.status.state;
    if (estado === 'SUCCEEDED') resultados[sqls[i]] = rowsToObjects_(r);
    else if (estado === 'PENDING' || estado === 'RUNNING') pendentes.push({ sql: sqls[i], id: r.statement_id });
    else throw new Error('Query falhou: ' + JSON.stringify(r.status));
  });

  let tentativas = 0;
  while (pendentes.length) {
    if (tentativas >= POLL_MAX_ATTEMPTS_PARALELO) throw new Error('Timeout aguardando as consultas em paralelo no Databricks.');
    Utilities.sleep(POLL_INTERVAL_PARALELO_MS);
    const polls = UrlFetchApp.fetchAll(pendentes.map(function (p) {
      return { url: baseUrl + '/' + p.id, method: 'get', headers: headers, muteHttpExceptions: true };
    }));
    const seguem = [];
    polls.forEach(function (resp, i) {
      const r = JSON.parse(resp.getContentText());
      const estado = r.status && r.status.state;
      if (estado === 'PENDING' || estado === 'RUNNING') seguem.push(pendentes[i]);
      else if (estado === 'SUCCEEDED') resultados[pendentes[i].sql] = rowsToObjects_(r);
      else throw new Error('Query falhou: ' + JSON.stringify(r.status));
    });
    pendentes = seguem;
    tentativas++;
  }
  return resultados;
}

function comConsultasEmParalelo_(rotulo, fn) {
  const inicio = Date.now();
  // 1ª passada: só coleta (erros aqui não importam: o que já foi coletado é aproveitado).
  CONSULTAS_COLETA_ = [];
  try { fn(); } catch (e) { Logger.log('[db] coleta ' + rotulo + ' parou: ' + e); }
  const vistos = {};
  const sqls = CONSULTAS_COLETA_.filter(function (s) { if (vistos[s]) return false; vistos[s] = true; return true; });
  CONSULTAS_COLETA_ = null;

  try {
    CONSULTAS_MEMO_ = executarConsultasEmParalelo_(sqls);
    Logger.log('[db] ' + rotulo + ': ' + sqls.length + ' consultas em paralelo em ' + (Date.now() - inicio) + ' ms');
  } catch (e) {
    Logger.log('[db] paralelo falhou em ' + rotulo + ' (' + e + '); seguindo em fila');
    CONSULTAS_MEMO_ = {};
  }
  try {
    return fn();
  } finally {
    CONSULTAS_MEMO_ = null;
    Logger.log('[db] ' + rotulo + ' pronto em ' + (Date.now() - inicio) + ' ms');
  }
}

function rowsToObjects_(result) {
  const columns = result.manifest.schema.columns.map(function (c) { return c.name; });
  const rows = (result.result && result.result.data_array) || [];
  return rows.map(function (row) {
    const obj = {};
    columns.forEach(function (colName, i) {
      obj[colName] = row[i];
    });
    return obj;
  });
}

// ===== Paginação (chunking) das consultas ao Databricks =====

// Busca uma tabela/consulta inteira em páginas de QUERY_PAGE_SIZE linhas,
// evitando payloads gigantes numa única chamada (timeout/limite do UrlFetchApp).
function queryDatabricksChunked_(baseSql, pageSize) {
  pageSize = pageSize || QUERY_PAGE_SIZE;
  let offset = 0;
  let allRows = [];
  let page = 0;

  while (true) {
    const pageSql = baseSql + ' LIMIT ' + pageSize + ' OFFSET ' + offset;
    const pageRows = queryDatabricks(pageSql);
    allRows = allRows.concat(pageRows);
    page++;
    Logger.log('Chunk ' + page + ': ' + pageRows.length + ' linhas (offset ' + offset + ')');
    if (pageRows.length < pageSize) break;
    offset += pageSize;
  }

  return allRows;
}

// ===== Cache (fatiado para respeitar o limite de 100KB por chave) =====

function cacheSet_(key, rows) {
  const cache = CacheService.getScriptCache();
  const json = JSON.stringify(rows);
  const totalBytes = Utilities.newBlob(json).getBytes().length;

  const chunks = [];
  for (let start = 0; start < json.length; start += CACHE_CHUNK_CHAR_LIMIT) {
    chunks.push(json.slice(start, start + CACHE_CHUNK_CHAR_LIMIT));
  }

  const payload = {};
  payload[key + '__meta'] = JSON.stringify({ chunkCount: chunks.length, bytes: totalBytes });
  chunks.forEach(function (chunk, i) {
    payload[key + '__chunk_' + i] = chunk;
  });
  cache.putAll(payload, CACHE_EXPIRATION_SECONDS);

  return { bytes: totalBytes, kb: +(totalBytes / 1024).toFixed(1), chunkCount: chunks.length };
}

function cacheGet_(key) {
  const cache = CacheService.getScriptCache();
  const metaRaw = cache.get(key + '__meta');
  if (!metaRaw) return null;

  const meta = JSON.parse(metaRaw);
  const chunkKeys = [];
  for (let i = 0; i < meta.chunkCount; i++) chunkKeys.push(key + '__chunk_' + i);
  const chunkMap = cache.getAll(chunkKeys);

  let json = '';
  for (let i = 0; i < meta.chunkCount; i++) {
    const part = chunkMap[key + '__chunk_' + i];
    if (part === undefined) return null; // algum chunk expirou -> trata como cache miss
    json += part;
  }
  return { rows: JSON.parse(json), bytes: meta.bytes, kb: +(meta.bytes / 1024).toFixed(1), chunkCount: meta.chunkCount };
}

function clearTableCache_(tableKey) {
  const cache = CacheService.getScriptCache();
  const cacheKey = 'tbl_' + tableKey;
  const metaRaw = cache.get(cacheKey + '__meta');
  if (!metaRaw) return;
  const meta = JSON.parse(metaRaw);
  const keysToRemove = [cacheKey + '__meta'];
  for (let i = 0; i < meta.chunkCount; i++) keysToRemove.push(cacheKey + '__chunk_' + i);
  cache.removeAll(keysToRemove);
}

// ===== Leitura de tabelas com cache =====

// Retorna as linhas da tabela, servindo do cache quando disponível.
// O resultado sempre informa quanta memória de cache está sendo usada (source: 'cache' ou 'databricks').
function getTableData(tableKey, forceRefresh) {
  const table = TABLES[tableKey];
  if (!table) throw new Error('Tabela desconhecida: ' + tableKey);

  const cacheKey = 'tbl_' + tableKey;

  if (!forceRefresh) {
    const cached = cacheGet_(cacheKey);
    if (cached) {
      Logger.log('Cache HIT (' + table + '): ' + cached.kb + ' KB em ' + cached.chunkCount + ' chunk(s)');
      return { rows: cached.rows, source: 'cache', cacheKB: cached.kb, chunkCount: cached.chunkCount };
    }
  }

  const rows = queryDatabricksChunked_('SELECT * FROM ' + table);
  const cacheInfo = cacheSet_(cacheKey, rows);
  Logger.log('Cache MISS (' + table + '): ' + rows.length + ' linhas consultadas, ' +
    cacheInfo.kb + ' KB armazenados em cache (' + cacheInfo.chunkCount + ' chunk(s) de até 100KB cada)');

  return { rows: rows, source: 'databricks', cacheKB: cacheInfo.kb, chunkCount: cacheInfo.chunkCount };
}

function getAllTablesData(forceRefresh) {
  const summary = {};
  Object.keys(TABLES).forEach(function (key) {
    summary[key] = getTableData(key, forceRefresh);
  });
  return summary;
}

// ===== Web app =====

function doGet() {
  return HtmlService.createTemplateFromFile('index')
    .evaluate()
    .setTitle('Dashboard Olist')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

// Identifica quem está vendo o dashboard. O nome "de verdade" exigiria habilitar
// a Google People API (permissão extra); por ora ele é derivado do e-mail.
function getUsuarioAtual() {
  const email = Session.getActiveUser().getEmail() || Session.getEffectiveUser().getEmail() || '';
  const nome = derivarNomeDoEmail_(email);
  return { email: email, nome: nome, iniciais: obterIniciais_(nome) };
}

function derivarNomeDoEmail_(email) {
  if (!email) return 'Usuário';
  const local = email.split('@')[0];
  const partes = local.split(/[._-]+/).filter(Boolean);
  return partes.map(function (p) {
    const semNumeros = p.replace(/[0-9]+$/, '') || p;
    return semNumeros.charAt(0).toUpperCase() + semNumeros.slice(1);
  }).join(' ');
}

function obterIniciais_(nome) {
  const partes = nome.trim().split(/\s+/).filter(Boolean);
  if (partes.length === 0) return '?';
  const primeira = partes[0].charAt(0);
  const ultima = partes.length > 1 ? partes[partes.length - 1].charAt(0) : '';
  return (primeira + ultima).toUpperCase();
}

// ===== Visão Geral =====

// Os dados do Databricks são atualizados 1–2 vezes por dia. O cache dura o máximo do CacheService (6h) e é reabastecido
// por gatilho às 6h e 12h (ver configurarAtualizacaoAutomatica). Para forçar a atualização na hora, rode
// atualizarDadosDashboard no editor do Apps Script — não há botão nem função pública para isso no dashboard.
const AGGREGATE_CACHE_SECONDS = 6 * 60 * 60; // 6h (máximo do CacheService)

// "Geração" do cache: entra em todas as chaves. Trocar o número (atualizarDadosDashboard) invalida tudo de uma vez.
function geracaoCache_() {
  return '_g' + (PropertiesService.getScriptProperties().getProperty('cache_geracao') || '0');
}
const MESES_CACHE_SECONDS = 6 * 60 * 60; // 6h — lista de meses muda raramente
const MESES_NO_MENU = 2; // o seletor de período mostra só o mês mais recente e o anterior
// Meses com poucos pedidos (a base termina com meses quase vazios: 16 e 4 pedidos) não entram no menu: uma comparação com eles
// não significa nada. Só contam meses com pelo menos este número de pedidos.
const MESES_MIN_PEDIDOS = 100;

// Lista os meses (yyyy-MM) com pedidos, do mais recente para o mais antigo.
function getMesesDisponiveis() {
  const cache = CacheService.getScriptCache();
  const cacheKey = 'meses_disponiveis_v3' + geracaoCache_();
  const cached = cache.get(cacheKey);
  if (cached) return JSON.parse(cached);

  const rows = queryDatabricks(
    'SELECT date_format(data_hora_pedido, \'yyyy-MM\') AS mes ' +
    'FROM ' + TABLES.PEDIDOS + ' ' +
    'GROUP BY 1 HAVING COUNT(*) >= ' + MESES_MIN_PEDIDOS + ' ' +
    'ORDER BY 1 DESC'
  );
  const meses = rows.map(function (r) { return r.mes; }).slice(0, MESES_NO_MENU); // só o mês mais recente e o anterior
  cache.put(cacheKey, JSON.stringify(meses), MESES_CACHE_SECONDS);
  return meses;
}

// Lista os anos (yyyy) com pedidos, do mais recente para o mais antigo.
function getAnosDisponiveis() {
  const cache = CacheService.getScriptCache();
  const cacheKey = 'anos_disponiveis' + geracaoCache_();
  const cached = cache.get(cacheKey);
  if (cached) return JSON.parse(cached);

  const rows = queryDatabricks(
    'SELECT DISTINCT date_format(data_hora_pedido, \'yyyy\') AS ano ' +
    'FROM ' + TABLES.PEDIDOS + ' ' +
    'ORDER BY 1 DESC'
  );
  const anos = rows.map(function (r) { return r.ano; });
  cache.put(cacheKey, JSON.stringify(anos), MESES_CACHE_SECONDS);
  return anos;
}

function mesAnterior_(mes) {
  return mesesAntes_(mes, 1);
}

// Retorna o mês (yyyy-MM) N meses antes de `mes`.
function mesesAntes_(mes, n) {
  const partes = mes.split('-');
  let ano = parseInt(partes[0], 10);
  let mesNum = parseInt(partes[1], 10) - n;
  while (mesNum <= 0) { mesNum += 12; ano -= 1; }
  return ano + '-' + (mesNum < 10 ? '0' + mesNum : mesNum);
}

// Monta a condição SQL de período (mês yyyy-MM ou ano yyyy) para uma coluna de data.
function condicaoPeriodo_(coluna, tipo, valor) {
  const formato = tipo === 'ano' ? 'yyyy' : 'yyyy-MM';
  return 'date_format(' + coluna + ', \'' + formato + '\') = \'' + valor + '\'';
}

function periodoAnterior_(tipo, valor) {
  return tipo === 'ano' ? String(parseInt(valor, 10) - 1) : mesAnterior_(valor);
}

// ===== Mês aberto: comparação com o mesmo período do mês anterior =====
// Se o mês selecionado é o último com pedidos e ainda não terminou (o último dia com pedido é anterior ao fim do mês),
// o mês anterior é cortado no mesmo dia (ex.: dias 1–18 × dias 1–18). Só os KPIs/comparações "vs. mês anterior" usam o corte;
// as séries de 12 meses continuam com o mês anterior inteiro. Fora disso (mês fechado) nada muda.
let CORTE_MES_ABERTO_ = null; // { mes, dia, anterior } enquanto uma montar…_ roda com mês aberto
const ULTIMA_DATA_CACHE_SECONDS = 60 * 60;

function obterCorteMesAberto_(valor) {
  try {
    const cache = CacheService.getScriptCache();
    const formatoData = /^\d{4}-\d{2}-\d{2}$/;
    const chaveUltima = 'ultima_data_pedido' + geracaoCache_();
    let ultima = cache.get(chaveUltima);
    if (!ultima) {
      const rows = queryDatabricks('SELECT date_format(MAX(data_hora_pedido), \'yyyy-MM-dd\') AS ultima FROM ' + TABLES.PEDIDOS);
      ultima = String(rows[0].ultima || '');
      if (formatoData.test(ultima)) cache.put(chaveUltima, ultima, ULTIMA_DATA_CACHE_SECONDS);
    }
    if (!formatoData.test(ultima) || ultima.substring(0, 7) !== valor) return null;
    const dia = parseInt(ultima.substring(8, 10), 10);
    const diasNoMes = new Date(parseInt(valor.substring(0, 4), 10), parseInt(valor.substring(5, 7), 10), 0).getDate();
    if (dia >= diasNoMes) return null;
    return { mes: valor, dia: dia, anterior: mesAnterior_(valor) };
  } catch (e) {
    Logger.log('[corte] não foi possível checar o mês aberto: ' + e);
    return null;
  }
}

function comCorte_(valor, fn) {
  CORTE_MES_ABERTO_ = valor ? obterCorteMesAberto_(valor) : null;
  try { return fn(); } finally { CORTE_MES_ABERTO_ = null; }
}

// Sufixo das chaves de cache: o mesmo mês aberto tem dados diferentes a cada dia.
function sufixoCorte_() { return geracaoCache_() + (CORTE_MES_ABERTO_ ? '_c' + CORTE_MES_ABERTO_.dia : ''); }

// Mês anterior limitado aos mesmos dias do mês aberto.
function condAnteriorParcial_(coluna) {
  return 'date_format(' + coluna + ', \'yyyy-MM\') = \'' + CORTE_MES_ABERTO_.anterior + '\' AND dayofmonth(' + coluna + ') <= ' + CORTE_MES_ABERTO_.dia;
}

function comparacaoInfo_() {
  return CORTE_MES_ABERTO_ ? { parcial: true, dia: CORTE_MES_ABERTO_.dia, mes: CORTE_MES_ABERTO_.mes, anterior: CORTE_MES_ABERTO_.anterior } : { parcial: false };
}

// Retorna { valorAtual, valorAnterior, deltaPct } — deltaPct é null se não houver período anterior com dado.
function calcularDelta_(atual, anterior) {
  if (anterior === null || anterior === undefined || isNaN(anterior) || anterior === 0) {
    return null;
  }
  return (atual - anterior) / anterior;
}

// Roda as agregações de KPI (receita, faturamento, margem, pedidos, clientes, entrega, atraso,
// cancelamento, avaliações) para um único período (mês ou ano). Usada tanto para o período
// selecionado quanto para o anterior (comparação).
function getKpisDoPeriodo_(tipo, valor, ateDia) {
  // ateDia: só os dias 1..N do mês (comparação com um mês aberto)
  const cortaDia = function (coluna) { return ateDia ? ' AND dayofmonth(' + coluna + ') <= ' + ateDia : ''; };
  const condPeriodo = condicaoPeriodo_('data_hora_pedido', tipo, valor) + cortaDia('data_hora_pedido');

  // Faturamento (receita, produtos, frete, ticket médio, clientes) só conta Entregue/Faturado.
  const faturamentoRow = queryDatabricks(
    'SELECT ' +
    '  SUM(valor_total_pedido) AS receita_total, ' +
    '  SUM(valor_total_produtos) AS receita_produtos, ' +
    '  SUM(valor_total_frete) AS custo_frete, ' +
    '  COUNT(*) AS total_pedidos, ' +
    '  COUNT(DISTINCT id_cliente_unico) AS clientes_distintos, ' +
    '  AVG(valor_total_pedido) AS ticket_medio ' +
    'FROM ' + TABLES.PEDIDOS + ' ' +
    'WHERE ' + condPeriodo + ' AND ' + CONDICAO_FATURAMENTO
  )[0];

  // Taxa de recompra do período: % de clientes com mais de 1 pedido faturado dentro do próprio
  // período selecionado (não olha histórico anterior — ver nota em PREOCUPACOES_CONFIG).
  const recompraRow = queryDatabricks(
    'SELECT SUM(CASE WHEN qtd_pedidos > 1 THEN 1 ELSE 0 END) AS clientes_recompra ' +
    'FROM (' +
    '  SELECT id_cliente_unico, COUNT(*) AS qtd_pedidos ' +
    '  FROM ' + TABLES.PEDIDOS + ' ' +
    '  WHERE ' + condPeriodo + ' AND ' + CONDICAO_FATURAMENTO + ' ' +
    '  GROUP BY id_cliente_unico' +
    ')'
  )[0];

  // Métricas operacionais olham todos os pedidos do período (cancelamento/atraso precisam do total real).
  const operacionalRow = queryDatabricks(
    'SELECT ' +
    '  AVG(dias_entrega) AS tempo_medio_entrega, ' +
    '  AVG(datediff(data_entrega_transportadora, data_hora_pedido)) AS tempo_ate_envio, ' +
    '  SUM(CASE WHEN pedido_entrega_atrasado THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN pedido_entregue THEN 1 ELSE 0 END), 0) AS taxa_atraso, ' +
    '  SUM(CASE WHEN pedido_cancelado THEN 1 ELSE 0 END) / COUNT(*) AS taxa_cancelamento ' +
    'FROM ' + TABLES.PEDIDOS + ' ' +
    'WHERE ' + condPeriodo
  )[0];

  const avaliacoesRow = queryDatabricks(
    'SELECT AVG(a.nota_avaliacao) AS nota_media, ' +
    '  SUM(CASE WHEN a.nota_avaliacao <= 2 THEN 1 ELSE 0 END) / COUNT(*) AS avaliacoes_negativas, ' +
    '  SUM(CASE WHEN a.nota_avaliacao = 5 THEN 1 ELSE 0 END) / COUNT(*) AS avaliacoes_cinco_estrelas ' +
    'FROM ' + TABLES.AVALIACOES + ' a ' +
    'JOIN ' + TABLES.PEDIDOS + ' p ON p.id_pedido = a.id_pedido ' +
    'WHERE ' + condicaoPeriodo_('p.data_hora_pedido', tipo, valor) + cortaDia('p.data_hora_pedido')
  )[0];

  const receitaTotal = parseFloat(faturamentoRow.receita_total) || 0;
  const receitaProdutos = parseFloat(faturamentoRow.receita_produtos) || 0;
  const custoFrete = parseFloat(faturamentoRow.custo_frete) || 0;
  const clientesDistintos = parseInt(faturamentoRow.clientes_distintos, 10) || 0;
  const clientesRecompra = parseInt(recompraRow.clientes_recompra, 10) || 0;

  return {
    receitaTotal: receitaTotal,
    receitaProdutos: receitaProdutos,
    custoFrete: custoFrete,
    // Margem = receita líquida (valor dos produtos) ÷ faturamento total (produtos + frete): que fatia do que o cliente paga
    // fica nos produtos. O frete já está fora de valor_total_produtos, então não se subtrai de novo.
    // Não é margem de lucro real — as tabelas gold não têm custo de produto (COGS).
    margemLiquida: receitaTotal > 0 ? receitaProdutos / receitaTotal : 0,
    freteReceita: receitaProdutos > 0 ? custoFrete / receitaProdutos : 0,
    totalPedidos: parseInt(faturamentoRow.total_pedidos, 10) || 0,
    ticketMedio: parseFloat(faturamentoRow.ticket_medio) || 0,
    receitaPorCliente: clientesDistintos > 0 ? receitaTotal / clientesDistintos : 0,
    taxaRecompra: clientesDistintos > 0 ? clientesRecompra / clientesDistintos : 0,
    tempoMedioEntrega: parseFloat(operacionalRow.tempo_medio_entrega) || 0,
    tempoAteEnvio: parseFloat(operacionalRow.tempo_ate_envio) || 0,
    taxaAtraso: parseFloat(operacionalRow.taxa_atraso) || 0,
    taxaCancelamento: parseFloat(operacionalRow.taxa_cancelamento) || 0,
    notaMedia: parseFloat(avaliacoesRow.nota_media) || 0,
    avaliacoesNegativas: parseFloat(avaliacoesRow.avaliacoes_negativas) || 0,
    avaliacoesCincoEstrelas: parseFloat(avaliacoesRow.avaliacoes_cinco_estrelas) || 0
  };
}

// Métricas mensais (para sparklines e para o gráfico de evolução no modo mensal) sob uma condição SQL livre.
// Faturamento (receita/pedidos/produtos) só conta Entregue/Faturado; o resto olha todos os pedidos do período.
function getMetricasMensais_(condicaoPedidos, condicaoAvaliacoes) {
  const faturamentoRaw = queryDatabricks(
    'SELECT date_format(data_hora_pedido, \'yyyy-MM\') AS mes, ' +
    '  SUM(valor_total_pedido) AS receita, ' +
    '  SUM(valor_total_produtos) AS receita_produtos, ' +
    '  SUM(valor_total_frete) AS custo_frete, ' +
    '  COUNT(*) AS pedidos, ' +
    '  SUM(qtd_itens) AS produtos ' +
    'FROM ' + TABLES.PEDIDOS + ' ' +
    'WHERE ' + condicaoPedidos + ' AND ' + CONDICAO_FATURAMENTO + ' ' +
    'GROUP BY 1 ORDER BY 1'
  );
  const faturamentoPorMes = {};
  faturamentoRaw.forEach(function (r) {
    faturamentoPorMes[r.mes] = {
      receita: parseFloat(r.receita) || 0,
      receitaProdutos: parseFloat(r.receita_produtos) || 0,
      custoFrete: parseFloat(r.custo_frete) || 0,
      pedidos: parseInt(r.pedidos, 10) || 0,
      produtos: parseInt(r.produtos, 10) || 0
    };
  });

  // Clientes distintos e recompra por mês (recompra = mais de 1 pedido faturado dentro do
  // próprio mês — ver nota em PREOCUPACOES_CONFIG sobre a definição adotada).
  const clientesRaw = queryDatabricks(
    'SELECT mes, COUNT(*) AS clientes_distintos, SUM(CASE WHEN qtd_pedidos > 1 THEN 1 ELSE 0 END) AS clientes_recompra ' +
    'FROM (' +
    '  SELECT date_format(data_hora_pedido, \'yyyy-MM\') AS mes, id_cliente_unico, COUNT(*) AS qtd_pedidos ' +
    '  FROM ' + TABLES.PEDIDOS + ' ' +
    '  WHERE ' + condicaoPedidos + ' AND ' + CONDICAO_FATURAMENTO + ' ' +
    '  GROUP BY 1, 2' +
    ') GROUP BY 1 ORDER BY 1'
  );
  const clientesPorMes = {};
  clientesRaw.forEach(function (r) {
    clientesPorMes[r.mes] = {
      distintos: parseInt(r.clientes_distintos, 10) || 0,
      recompra: parseInt(r.clientes_recompra, 10) || 0
    };
  });

  const operacionalRaw = queryDatabricks(
    'SELECT date_format(data_hora_pedido, \'yyyy-MM\') AS mes, ' +
    '  AVG(dias_entrega) AS tempo_medio_entrega, ' +
    '  AVG(datediff(data_entrega_transportadora, data_hora_pedido)) AS tempo_ate_envio, ' +
    '  SUM(CASE WHEN pedido_entrega_atrasado THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN pedido_entregue THEN 1 ELSE 0 END), 0) AS taxa_atraso, ' +
    '  SUM(CASE WHEN pedido_cancelado THEN 1 ELSE 0 END) / COUNT(*) AS taxa_cancelamento ' +
    'FROM ' + TABLES.PEDIDOS + ' ' +
    'WHERE ' + condicaoPedidos + ' ' +
    'GROUP BY 1 ORDER BY 1'
  );

  const notaMediaRaw = queryDatabricks(
    'SELECT date_format(p.data_hora_pedido, \'yyyy-MM\') AS mes, ' +
    '  AVG(a.nota_avaliacao) AS nota_media, ' +
    '  SUM(CASE WHEN a.nota_avaliacao <= 2 THEN 1 ELSE 0 END) / COUNT(*) AS avaliacoes_negativas, ' +
    '  SUM(CASE WHEN a.nota_avaliacao = 5 THEN 1 ELSE 0 END) / COUNT(*) AS avaliacoes_cinco_estrelas ' +
    'FROM ' + TABLES.AVALIACOES + ' a ' +
    'JOIN ' + TABLES.PEDIDOS + ' p ON p.id_pedido = a.id_pedido ' +
    'WHERE ' + condicaoAvaliacoes + ' ' +
    'GROUP BY 1 ORDER BY 1'
  );
  const notaMediaPorMes = {};
  const avaliacoesNegativasPorMes = {};
  const avaliacoesCincoEstrelasPorMes = {};
  notaMediaRaw.forEach(function (r) {
    notaMediaPorMes[r.mes] = parseFloat(r.nota_media) || 0;
    avaliacoesNegativasPorMes[r.mes] = parseFloat(r.avaliacoes_negativas) || 0;
    avaliacoesCincoEstrelasPorMes[r.mes] = parseFloat(r.avaliacoes_cinco_estrelas) || 0;
  });

  return operacionalRaw.map(function (r) {
    const fat = faturamentoPorMes[r.mes] || { receita: 0, receitaProdutos: 0, custoFrete: 0, pedidos: 0, produtos: 0 };
    const cli = clientesPorMes[r.mes] || { distintos: 0, recompra: 0 };
    return {
      mes: r.mes,
      receita: fat.receita,
      receitaProdutos: fat.receitaProdutos,
      custoFrete: fat.custoFrete,
      margemLiquida: fat.receita > 0 ? fat.receitaProdutos / fat.receita : 0,
      freteReceita: fat.receitaProdutos > 0 ? fat.custoFrete / fat.receitaProdutos : 0,
      pedidos: fat.pedidos,
      produtos: fat.produtos,
      ticketMedio: fat.pedidos > 0 ? fat.receita / fat.pedidos : 0,
      receitaPorCliente: cli.distintos > 0 ? fat.receita / cli.distintos : 0,
      taxaRecompra: cli.distintos > 0 ? cli.recompra / cli.distintos : 0,
      tempoMedioEntrega: parseFloat(r.tempo_medio_entrega) || 0,
      tempoAteEnvio: parseFloat(r.tempo_ate_envio) || 0,
      taxaAtraso: parseFloat(r.taxa_atraso) || 0,
      taxaCancelamento: parseFloat(r.taxa_cancelamento) || 0,
      notaMedia: notaMediaPorMes[r.mes] || 0,
      avaliacoesNegativas: avaliacoesNegativasPorMes[r.mes] || 0,
      avaliacoesCincoEstrelas: avaliacoesCincoEstrelasPorMes[r.mes] || 0
    };
  });
}

// Métricas por ano (para as sparklines dos KPIs no modo Anual) — todos os anos disponíveis.
// Faturamento (receita/pedidos/produtos) só conta Entregue/Faturado; o resto olha todos os pedidos.
function getMetricasAnuais_() {
  const faturamentoRaw = queryDatabricks(
    'SELECT date_format(data_hora_pedido, \'yyyy\') AS ano, ' +
    '  SUM(valor_total_pedido) AS receita, ' +
    '  SUM(valor_total_produtos) AS receita_produtos, ' +
    '  SUM(valor_total_frete) AS custo_frete, ' +
    '  COUNT(*) AS pedidos, ' +
    '  SUM(qtd_itens) AS produtos ' +
    'FROM ' + TABLES.PEDIDOS + ' ' +
    'WHERE ' + CONDICAO_FATURAMENTO + ' ' +
    'GROUP BY 1 ORDER BY 1'
  );
  const faturamentoPorAno = {};
  faturamentoRaw.forEach(function (r) {
    faturamentoPorAno[r.ano] = {
      receita: parseFloat(r.receita) || 0,
      receitaProdutos: parseFloat(r.receita_produtos) || 0,
      custoFrete: parseFloat(r.custo_frete) || 0,
      pedidos: parseInt(r.pedidos, 10) || 0,
      produtos: parseInt(r.produtos, 10) || 0
    };
  });

  const clientesRaw = queryDatabricks(
    'SELECT ano, COUNT(*) AS clientes_distintos, SUM(CASE WHEN qtd_pedidos > 1 THEN 1 ELSE 0 END) AS clientes_recompra ' +
    'FROM (' +
    '  SELECT date_format(data_hora_pedido, \'yyyy\') AS ano, id_cliente_unico, COUNT(*) AS qtd_pedidos ' +
    '  FROM ' + TABLES.PEDIDOS + ' ' +
    '  WHERE ' + CONDICAO_FATURAMENTO + ' ' +
    '  GROUP BY 1, 2' +
    ') GROUP BY 1 ORDER BY 1'
  );
  const clientesPorAno = {};
  clientesRaw.forEach(function (r) {
    clientesPorAno[r.ano] = {
      distintos: parseInt(r.clientes_distintos, 10) || 0,
      recompra: parseInt(r.clientes_recompra, 10) || 0
    };
  });

  const operacionalRaw = queryDatabricks(
    'SELECT date_format(data_hora_pedido, \'yyyy\') AS ano, ' +
    '  AVG(dias_entrega) AS tempo_medio_entrega, ' +
    '  AVG(datediff(data_entrega_transportadora, data_hora_pedido)) AS tempo_ate_envio, ' +
    '  SUM(CASE WHEN pedido_entrega_atrasado THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN pedido_entregue THEN 1 ELSE 0 END), 0) AS taxa_atraso, ' +
    '  SUM(CASE WHEN pedido_cancelado THEN 1 ELSE 0 END) / COUNT(*) AS taxa_cancelamento ' +
    'FROM ' + TABLES.PEDIDOS + ' ' +
    'GROUP BY 1 ORDER BY 1'
  );

  const notaMediaRaw = queryDatabricks(
    'SELECT date_format(p.data_hora_pedido, \'yyyy\') AS ano, ' +
    '  AVG(a.nota_avaliacao) AS nota_media, ' +
    '  SUM(CASE WHEN a.nota_avaliacao <= 2 THEN 1 ELSE 0 END) / COUNT(*) AS avaliacoes_negativas, ' +
    '  SUM(CASE WHEN a.nota_avaliacao = 5 THEN 1 ELSE 0 END) / COUNT(*) AS avaliacoes_cinco_estrelas ' +
    'FROM ' + TABLES.AVALIACOES + ' a ' +
    'JOIN ' + TABLES.PEDIDOS + ' p ON p.id_pedido = a.id_pedido ' +
    'GROUP BY 1 ORDER BY 1'
  );
  const notaMediaPorAno = {};
  const avaliacoesNegativasPorAno = {};
  const avaliacoesCincoEstrelasPorAno = {};
  notaMediaRaw.forEach(function (r) {
    notaMediaPorAno[r.ano] = parseFloat(r.nota_media) || 0;
    avaliacoesNegativasPorAno[r.ano] = parseFloat(r.avaliacoes_negativas) || 0;
    avaliacoesCincoEstrelasPorAno[r.ano] = parseFloat(r.avaliacoes_cinco_estrelas) || 0;
  });

  return operacionalRaw.map(function (r) {
    const fat = faturamentoPorAno[r.ano] || { receita: 0, receitaProdutos: 0, custoFrete: 0, pedidos: 0, produtos: 0 };
    const cli = clientesPorAno[r.ano] || { distintos: 0, recompra: 0 };
    return {
      ano: r.ano,
      receita: fat.receita,
      receitaProdutos: fat.receitaProdutos,
      custoFrete: fat.custoFrete,
      margemLiquida: fat.receita > 0 ? fat.receitaProdutos / fat.receita : 0,
      freteReceita: fat.receitaProdutos > 0 ? fat.custoFrete / fat.receitaProdutos : 0,
      pedidos: fat.pedidos,
      produtos: fat.produtos,
      ticketMedio: fat.pedidos > 0 ? fat.receita / fat.pedidos : 0,
      receitaPorCliente: cli.distintos > 0 ? fat.receita / cli.distintos : 0,
      taxaRecompra: cli.distintos > 0 ? cli.recompra / cli.distintos : 0,
      tempoMedioEntrega: parseFloat(r.tempo_medio_entrega) || 0,
      tempoAteEnvio: parseFloat(r.tempo_ate_envio) || 0,
      taxaAtraso: parseFloat(r.taxa_atraso) || 0,
      taxaCancelamento: parseFloat(r.taxa_cancelamento) || 0,
      notaMedia: notaMediaPorAno[r.ano] || 0,
      avaliacoesNegativas: avaliacoesNegativasPorAno[r.ano] || 0,
      avaliacoesCincoEstrelas: avaliacoesCincoEstrelasPorAno[r.ano] || 0
    };
  });
}

// Comparativo mês a mês (Jan-Dez) entre um ano e o ano anterior, para o gráfico da Visão Geral anual.
function getComparativoAnual_(ano) {
  const anoAnterior = periodoAnterior_('ano', ano);

  const rows = queryDatabricks(
    'SELECT date_format(data_hora_pedido, \'yyyy\') AS ano, ' +
    '  date_format(data_hora_pedido, \'MM\') AS mes_num, ' +
    '  SUM(valor_total_pedido) AS receita, ' +
    '  COUNT(*) AS pedidos, ' +
    '  SUM(qtd_itens) AS produtos ' +
    'FROM ' + TABLES.PEDIDOS + ' ' +
    'WHERE date_format(data_hora_pedido, \'yyyy\') IN (\'' + ano + '\', \'' + anoAnterior + '\') ' +
    '  AND ' + CONDICAO_FATURAMENTO + ' ' +
    'GROUP BY 1, 2 ORDER BY 1, 2'
  );

  const porAnoMes = {};
  rows.forEach(function (r) {
    if (!porAnoMes[r.ano]) porAnoMes[r.ano] = {};
    porAnoMes[r.ano][r.mes_num] = {
      receita: parseFloat(r.receita) || 0,
      pedidos: parseInt(r.pedidos, 10) || 0,
      produtos: parseInt(r.produtos, 10) || 0
    };
  });

  const vazio = { receita: 0, pedidos: 0, produtos: 0 };
  const numerosMes = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'];
  const pontos = numerosMes.map(function (mesNum) {
    return {
      mesNum: mesNum,
      atual: (porAnoMes[ano] && porAnoMes[ano][mesNum]) || vazio,
      anterior: (porAnoMes[anoAnterior] && porAnoMes[anoAnterior][mesNum]) || vazio
    };
  });

  return { anoAtual: ano, anoAnterior: anoAnterior, pontos: pontos };
}

const RANKING_ESTADOS_LIMITE = 5;
const DETALHE_PEDIDOS_LIMITE = 15;
const JANELA_MESES = 12;

function getOverviewData(mes, forceRefresh) {
  return montarOverview_('mes', mes || getMesesDisponiveis()[0], forceRefresh);
}

// Abertura do dash: devolve a lista de meses e os dados do mais recente numa chamada só (poupa uma ida e volta ao servidor).
function getOverviewInicial() {
  const meses = getMesesDisponiveis();
  return { meses: meses, data: montarOverview_('mes', meses[0], false) };
}

function getOverviewDataAnual(ano, forceRefresh) {
  return montarOverview_('ano', ano || getAnosDisponiveis()[0], forceRefresh);
}

// Métricas candidatas ao ranking de "principais pontos de atenção" — todas já vêm de `kpis`
// (getKpisDoPeriodo_) e da série histórica (janelaMensal/janelaAnual), sem query extra.
// positiveIsGood define o que conta como piora: se true, delta negativo é piora (e, junto com
// PREOCUPACOES_DESACELERACAO_LIMIAR abaixo, também "cresceu bem menos que o padrão"); se false,
// delta positivo é piora. serieKey é o nome do campo na série histórica, quando diferente de
// `campo` (kpis usa nomes mais verbosos, ex. receitaTotal vs. receita).
//
// peso: importância relativa do KPI pro negócio — pondera o ranking pra uma queda de receita
// pesar mais que uma queda de ticket médio de magnitude % parecida. Escala livre (0–1), quanto
// maior mais importante; só a ordem relativa entre os pesos importa. Ajustar aqui quando a
// prioridade de negócio mudar — é o único lugar do código que define isso.
//
// Notas sobre duas métricas com definição menos óbvia (decidido em conversa com o usuário):
// - margemLiquida: receita líquida (receitaProdutos = valor_total_produtos) ÷ faturamento total (receitaTotal =
//   valor_total_pedido = produtos + frete). As tabelas gold não têm custo de produto (COGS), então não é margem de lucro.
// - freteReceita: custo de frete ÷ receita líquida.
// - taxaRecompra: % de clientes com mais de 1 pedido faturado dentro do próprio período (mês/ano)
//   selecionado. Não olha se o cliente já tinha comprado em período anterior (isso exigiria
//   varrer o histórico completo do cliente a cada cálculo).
const PREOCUPACOES_CONFIG = [
  { campo: 'receitaTotal', serieKey: 'receita', label: 'Receita total', positiveIsGood: true, formato: 'brl', peso: 1.0, pesoTexto: 'Principal indicador financeiro do negócio' },
  { campo: 'receitaProdutos', serieKey: 'receitaProdutos', label: 'Faturamento (produtos)', positiveIsGood: true, formato: 'brl', peso: 0.9, pesoTexto: 'Receita de produtos, sem frete — mede a venda em si' },
  { campo: 'margemLiquida', label: 'Margem', positiveIsGood: true, formato: 'pct', peso: 0.95, pesoTexto: 'Fatia do faturamento total que é receita líquida (quanto menor, mais o frete pesa) — rentabilidade, quase tão crítico quanto a receita bruta' },
  { campo: 'custoFrete', serieKey: 'custoFrete', label: 'Custo de frete', positiveIsGood: false, formato: 'brl', peso: 0.6, pesoTexto: 'Custo operacional — já parcialmente refletido na margem' },
  { campo: 'freteReceita', label: 'Frete / receita', positiveIsGood: false, formato: 'pct', peso: 0.55, pesoTexto: 'Eficiência logística — quanto do faturamento é consumido pelo frete' },
  { campo: 'totalPedidos', serieKey: 'pedidos', label: 'Total de pedidos', positiveIsGood: true, formato: 'num', peso: 0.85, pesoTexto: 'Indicador operacional core — afeta toda a operação a jusante' },
  { campo: 'ticketMedio', label: 'Ticket médio', positiveIsGood: true, formato: 'brl', peso: 0.5, pesoTexto: 'Indicador secundário — em geral é resultado de outros fatores, não causa' },
  { campo: 'receitaPorCliente', label: 'Receita por cliente', positiveIsGood: true, formato: 'brl', peso: 0.8, pesoTexto: 'Combina ticket e frequência — saúde da receita por cliente' },
  { campo: 'taxaRecompra', label: 'Taxa de recompra', positiveIsGood: true, formato: 'pct', peso: 0.85, pesoTexto: 'Retenção — alto valor estratégico de longo prazo' },
  { campo: 'notaMedia', label: 'Nota média', positiveIsGood: true, formato: 'nota', peso: 0.7, pesoTexto: 'Satisfação geral acumulada dos clientes' },
  { campo: 'avaliacoesNegativas', label: 'Avaliações negativas', positiveIsGood: false, formato: 'pct', peso: 0.75, pesoTexto: 'Isola os detratores — mais acionável que a nota média sozinha' },
  { campo: 'taxaAtraso', label: 'Taxa de atraso', positiveIsGood: false, formato: 'pct', peso: 0.8, pesoTexto: 'Impacta diretamente a satisfação e o custo de suporte' },
  { campo: 'taxaCancelamento', label: 'Taxa de cancelamento', positiveIsGood: false, formato: 'pct', peso: 0.85, pesoTexto: 'Perda direta de receita já convertida' },
  { campo: 'tempoMedioEntrega', label: 'Prazo médio de entrega', positiveIsGood: false, formato: 'dias', peso: 0.55, pesoTexto: 'Importante, mas parcialmente coberto pela taxa de atraso' },
  { campo: 'tempoAteEnvio', label: 'Tempo até envio', positiveIsGood: false, formato: 'dias', peso: 0.45, pesoTexto: 'Processo interno — mais controlável, mas com impacto indireto no cliente' }
];
const PREOCUPACOES_LIMITE = 5;
const PREOCUPACOES_CRITICO_TOPN = 2; // as N piores da lista viram "crítico"; o resto, "atenção"

// Quantos períodos anteriores olhar pra calcular o "padrão histórico" de crescimento de um KPI
// (usado só pra detectar desaceleração — ver PREOCUPACOES_DESACELERACAO_LIMIAR).
const PREOCUPACOES_BASELINE_JANELA = 6;

// Quão abaixo da média histórica de crescimento um KPI (positiveIsGood=true) precisa ficar pra
// entrar no ranking mesmo sem ter caído de verdade — ex.: Receita sempre cresce ~9%/mês e num mês
// cresce só 1%: não caiu, mas desacelerou 8 p.p. abaixo do padrão, o que já é sinal de atenção.
// Quanto mais negativo, mais exigente (só desacelerações fortes entram).
const PREOCUPACOES_DESACELERACAO_LIMIAR = -0.05;

// Quedas (ou desacelerações) que se repetem em vários períodos seguidos pesam mais que um caso
// isolado, mesmo com magnitude % menor. Capado em 3+ períodos pra um problema crônico não
// dominar o ranking pra sempre e ofuscar pioras novas.
function persistenciaMultiplicador_(periodos) {
  if (periodos <= 1) return 1.0;
  if (periodos === 2) return 1.3;
  return 1.6;
}

// Conta quantos períodos consecutivos, terminando no período atual (o último da série), essa
// métrica vem piorando de fato (queda literal, não desaceleração) — usado pro multiplicador de
// persistência acima. `serie` deve vir em ordem crescente (mais antigo primeiro).
function contarPersistencia_(serie, serieKey, positiveIsGood, mesAberto) {
  // Mês aberto: o último ponto da série é parcial e não pode ser comparado com o mês anterior inteiro; a queda do mês atual
  // já foi confirmada pelo delta comparável (mesmo período), então conta +1 sobre a sequência dos meses fechados.
  if (mesAberto) return 1 + contarPersistenciaBruta_(serie.slice(0, -1), serieKey, positiveIsGood);
  return Math.max(contarPersistenciaBruta_(serie, serieKey, positiveIsGood), 1);
}

function contarPersistenciaBruta_(serie, serieKey, positiveIsGood) {
  let streak = 0;
  for (let i = serie.length - 1; i > 0; i--) {
    const atual = serie[i][serieKey];
    const anterior = serie[i - 1][serieKey];
    if (!anterior) break;
    const delta = (atual - anterior) / anterior;
    const piorou = positiveIsGood ? delta < 0 : delta > 0;
    if (!piorou) break;
    streak++;
  }
  return streak;
}

// Média das variações período a período de um campo nos PREOCUPACOES_BASELINE_JANELA períodos
// anteriores ao atual (exclui a transição mais recente, que é o que estamos avaliando) — o
// "padrão normal" de crescimento daquele KPI. Retorna null se não houver histórico suficiente.
function mediaMoMHistorica_(serie, serieKey) {
  const fimHistorico = serie.length - 1; // exclui a transição do período atual
  const inicioHistorico = Math.max(1, fimHistorico - PREOCUPACOES_BASELINE_JANELA);
  const deltas = [];
  for (let i = inicioHistorico; i < fimHistorico; i++) {
    const atual = serie[i][serieKey];
    const anterior = serie[i - 1][serieKey];
    if (!anterior) continue;
    deltas.push((atual - anterior) / anterior);
  }
  if (!deltas.length) return null;
  return deltas.reduce(function (soma, d) { return soma + d; }, 0) / deltas.length;
}

// Avalia os 15 KPIs candidatos (score, motivo, se piorou) vs. o período anterior. Métrica sem
// período anterior pra comparar (delta null) ainda aparece na lista, só marcada como tal —
// quem decide o que vira "ponto de atenção" é montarPreocupacoes_, que consome esta função.
function avaliarPreocupacoesCandidatas_(kpis, serie) {
  return PREOCUPACOES_CONFIG.map(function (cfg) {
    const k = kpis[cfg.campo];
    const base = {
      metrica: cfg.label,
      formato: cfg.formato,
      valorAtual: k.valor,
      valorAnterior: k.valorAnterior,
      pesoTexto: cfg.pesoTexto
    };
    if (k.delta === null) {
      return Object.assign(base, { delta: null, magnitude: 0, mediaHistorica: null, motivo: null, persistencia: 0, piorou: false, score: 0 });
    }

    const serieKey = cfg.serieKey || cfg.campo;
    const mediaHistorica = serie ? mediaMoMHistorica_(serie, serieKey) : null;
    const desvio = mediaHistorica !== null ? k.delta - mediaHistorica : null;

    let piorou, magnitude, motivo;
    if (cfg.positiveIsGood) {
      const caiu = k.delta < 0;
      const desacelerou = !caiu && desvio !== null && desvio < PREOCUPACOES_DESACELERACAO_LIMIAR;
      piorou = caiu || desacelerou;
      magnitude = caiu ? Math.abs(k.delta) : (desacelerou ? Math.abs(desvio) : 0);
      motivo = caiu ? 'queda' : (desacelerou ? 'desaceleracao' : null);
    } else {
      piorou = k.delta > 0;
      magnitude = Math.abs(k.delta);
      motivo = piorou ? 'piora' : null;
    }

    const persistencia = piorou && serie ? contarPersistencia_(serie, serieKey, cfg.positiveIsGood, !!CORTE_MES_ABERTO_) : 0;
    return Object.assign(base, {
      delta: k.delta,
      // `magnitude` (não `delta`) é o que deve dimensionar a barra no front-end: pra itens
      // "desaceleracao" o delta pode ser um número pequeno e positivo (ainda cresceu), quem
      // representa a severidade real é o desvio vs. padrão histórico.
      magnitude: magnitude,
      mediaHistorica: mediaHistorica,
      motivo: motivo,
      persistencia: persistencia,
      piorou: piorou,
      score: magnitude * cfg.peso * persistenciaMultiplicador_(persistencia)
    });
  });
}

// Ranking das métricas que pioraram OU cresceram bem menos que o padrão histórico (só pra KPIs
// "maior é melhor" — ver PREOCUPACOES_DESACELERACAO_LIMIAR) vs. o período anterior. A posição é
// dada por um score = magnitude × peso de importância × multiplicador de persistência — não só
// pela variação % isolada do período, pra uma queda de receita "leve mas contínua" poder
// aparecer à frente de uma queda "forte mas pontual" de um KPI secundário.
//
// Retorna `ranking` (só os PREOCUPACOES_LIMITE piores, pro gráfico de barras) e `todos` (os 15
// KPIs candidatos, pra tabela de acessibilidade — "Ver tabela" mostra todo mundo acompanhado,
// não só quem entrou no ranking).
function montarPreocupacoes_(kpis, serie) {
  const avaliados = avaliarPreocupacoesCandidatas_(kpis, serie);

  const ranking = avaliados
    .filter(function (p) { return p.piorou; })
    .sort(function (a, b) { return b.score - a.score; })
    .slice(0, PREOCUPACOES_LIMITE)
    .map(function (p, i) {
      return {
        metrica: p.metrica,
        formato: p.formato,
        valorAtual: p.valorAtual,
        valorAnterior: p.valorAnterior,
        delta: p.delta,
        magnitude: p.magnitude,
        mediaHistorica: p.mediaHistorica,
        motivo: p.motivo,
        pesoTexto: p.pesoTexto,
        persistencia: p.persistencia,
        severidade: i < PREOCUPACOES_CRITICO_TOPN ? 'critico' : 'atencao'
      };
    });

  const severidadePorMetrica = {};
  ranking.forEach(function (r) { severidadePorMetrica[r.metrica] = r.severidade; });

  const todos = avaliados
    .slice()
    .sort(function (a, b) { return b.score - a.score; })
    .map(function (p) {
      return {
        metrica: p.metrica,
        formato: p.formato,
        valorAtual: p.valorAtual,
        valorAnterior: p.valorAnterior,
        delta: p.delta,
        motivo: p.motivo,
        pesoTexto: p.pesoTexto,
        persistencia: p.persistencia,
        piorou: p.piorou,
        // null = não entrou no top N do ranking (mas continua sendo acompanhado aqui).
        severidade: severidadePorMetrica[p.metrica] || null
      };
    });

  return { ranking: ranking, todos: todos };
}

function montarOverview_(tipo, valor, forceRefresh) {
  return comCorte_(tipo === 'mes' ? valor : null, function () {
    return comConsultasEmParalelo_('overview ' + tipo + ' ' + valor, function () { return montarOverviewInterno_(tipo, valor, forceRefresh); });
  });
}

function montarOverviewInterno_(tipo, valor, forceRefresh) {
  const cache = CacheService.getScriptCache();
  const cacheKey = 'overview_' + tipo + '_v18_' + valor + sufixoCorte_(); // v15: sem categorias/geo/status/detalhe (Visão Geral em etapas)
  if (!forceRefresh) {
    const cached = cache.get(cacheKey);
    if (cached) return JSON.parse(cached);
  }

  const anterior = periodoAnterior_(tipo, valor);
  const kpisAtual = getKpisDoPeriodo_(tipo, valor);
  const kpisAnterior = getKpisDoPeriodo_(tipo, anterior, CORTE_MES_ABERTO_ ? CORTE_MES_ABERTO_.dia : null);

  const kpis = {};
  Object.keys(kpisAtual).forEach(function (campo) {
    kpis[campo] = {
      valor: kpisAtual[campo],
      valorAnterior: kpisAnterior[campo],
      delta: calcularDelta_(kpisAtual[campo], kpisAnterior[campo])
    };
  });

  let janelaMensal = null, janelaAnual = null, comparativoAnual = null;
  if (tipo === 'ano') {
    janelaAnual = getMetricasAnuais_();
    comparativoAnual = getComparativoAnual_(valor);
  } else {
    const mesInicioJanela = mesesAntes_(valor, JANELA_MESES - 1);
    janelaMensal = getMetricasMensais_(
      'date_format(data_hora_pedido, \'yyyy-MM\') BETWEEN \'' + mesInicioJanela + '\' AND \'' + valor + '\'',
      'date_format(p.data_hora_pedido, \'yyyy-MM\') BETWEEN \'' + mesInicioJanela + '\' AND \'' + valor + '\''
    );
  }
  // Usa a série (mensal ou anual, a que existir pro tipo atual) pra medir persistência —
  // quantos períodos seguidos aquele KPI vem piorando.
  const preocupacoesResultado = montarPreocupacoes_(kpis, tipo === 'ano' ? janelaAnual : janelaMensal);

  const data = {
    tipo: tipo,
    periodo: valor,
    periodoAnterior: anterior,
    comparacao: comparacaoInfo_(),
    kpis: kpis,
    preocupacoes: preocupacoesResultado.ranking,
    preocupacoesTodos: preocupacoesResultado.todos,
    janelaMensal: janelaMensal,
    janelaAnual: janelaAnual,
    comparativoAnual: comparativoAnual,
    atualizadoEm: new Date().toISOString()
  };

  if (!CONSULTAS_COLETA_) cache.put(cacheKey, JSON.stringify(data), AGGREGATE_CACHE_SECONDS);
  return data;
}

// ===== Visão Vendas (estrutura da máquina comercial) =====
// Narrativa: força comercial (Q1–Q3) → mercado (Q4–Q6) → estrutura/risco (Q7) → diagnóstico (Q8).
// As perguntas são abertas: o gráfico mostra a distribuição e quem lê encontra o padrão. Uma única chamada
// (getVendasData) devolve os KPIs e os dados de todos os blocos; o painel de sinais (Q8) é montado no front.

// Limites (R$ de faturamento no mês) que separam as faixas de faturamento dos vendedores (Q1).
const DISPERSAO_LIMITES_FAIXA = [100, 500, 1000, 5000, 10000];
// Grupos de vendedores por posição no ranking de faturamento, em % dos vendedores (Q1): [de, até].
const VENDAS_GRUPOS_PERCENTIL = [
  { rotulo: 'Top 5%', de: 0, ate: 0.05 },
  { rotulo: '5% a 10%', de: 0.05, ate: 0.10 },
  { rotulo: '10% a 20%', de: 0.10, ate: 0.20 },
  { rotulo: '20% a 50%', de: 0.20, ate: 0.50 },
  { rotulo: 'Demais 50%', de: 0.50, ate: 1 }
];
// Vendedores enviados ao gráfico de bolhas do Q2 (só os de maior faturamento: com centenas de bolhas o gráfico vira uma nuvem ilegível).
const VENDAS_SCATTER_MAX = 50;
// Limite do CacheService por chave (100KB); acima disso a dispersão é reduzida até caber.
const VENDAS_CACHE_LIMITE_CHARS = 90000;
// Faixas de tamanho de carteira (clientes distintos nos últimos 12 meses) — Q3.
const VENDAS_FAIXAS_CARTEIRA = ['1 cliente', '2 a 5', '6 a 10', '11 a 25', '26 a 50', '51 a 100', 'Mais de 100'];
// Classes de concentração da carteira: quanto o maior cliente representa no faturamento do vendedor.
const VENDAS_CLASSES_CONCENTRACAO = ['Maior cliente até 25%', 'Maior cliente de 25% a 50%', 'Maior cliente acima de 50%', 'Cliente único'];
// Q4: frequência (pedidos nos últimos 12 meses) × intervalo médio entre compras (dias).
const VENDAS_FREQUENCIAS = ['2 pedidos', '3 pedidos', '4 ou mais pedidos'];
const VENDAS_INTERVALOS = ['Até 30 dias', '31 a 90 dias', '91 a 180 dias', 'Mais de 180 dias'];
// Q5: quantas cidades (das de mais pedidos) seguem para o gráfico de mercados.
const VENDAS_CIDADES_MAX = 60;
const VENDAS_FAIXAS_TICKET_PEDIDO = ['Até R$ 50', 'R$ 50 a 100', 'R$ 100 a 200', 'R$ 200 a 500', 'Acima de R$ 500'];
// Q6: estados mostrados individualmente (os de mais pedidos).
const VENDAS_PERFIL_ESTADOS_TOP_N = 10;
// Pontos da curva de concentração (Q7): % acumulada dos elementos (vendedores, clientes…).
const VENDAS_CURVA_PONTOS = [0.01, 0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1];

const REGIAO_POR_UF = {
  AC: 'Norte', AP: 'Norte', AM: 'Norte', PA: 'Norte', RO: 'Norte', RR: 'Norte', TO: 'Norte',
  AL: 'Nordeste', BA: 'Nordeste', CE: 'Nordeste', MA: 'Nordeste', PB: 'Nordeste', PE: 'Nordeste', PI: 'Nordeste', RN: 'Nordeste', SE: 'Nordeste',
  DF: 'Centro-Oeste', GO: 'Centro-Oeste', MT: 'Centro-Oeste', MS: 'Centro-Oeste',
  ES: 'Sudeste', MG: 'Sudeste', RJ: 'Sudeste', SP: 'Sudeste',
  PR: 'Sul', RS: 'Sul', SC: 'Sul'
};

function rotuloFaixa_(limites, i) {
  const fmt = function (v) { return v >= 1000 ? (v / 1000) + ' mil' : String(v); };
  if (i === 0) return 'Até R$ ' + fmt(limites[0]);
  if (i === limites.length) return 'Acima de R$ ' + fmt(limites[limites.length - 1]);
  return 'R$ ' + fmt(limites[i - 1]) + ' a ' + fmt(limites[i]);
}

// Índice de Gini (0 = todos iguais, 1 = um só concentra tudo) a partir de valores em ordem crescente.
function giniLista_(valoresAsc) {
  const n = valoresAsc.length;
  const soma = valoresAsc.reduce(function (acc, v) { return acc + v; }, 0);
  if (n === 0 || soma <= 0) return 0;
  let ponderada = 0;
  valoresAsc.forEach(function (v, i) { ponderada += (i + 1) * v; });
  return (2 * ponderada) / (n * soma) - (n + 1) / n;
}

// Curva de concentração: quanto do total os N% maiores elementos respondem (pontos [% dos elementos, % do total]).
function curvaConcentracao_(valores) {
  const v = valores.filter(function (x) { return x > 0; }).sort(function (a, b) { return b - a; });
  const n = v.length;
  const total = v.reduce(function (s, x) { return s + x; }, 0);
  const acumulado = [];
  v.reduce(function (s, x) { acumulado.push(s + x); return s + x; }, 0);
  const parte = function (p) {
    if (n === 0 || total <= 0) return 0;
    return acumulado[Math.max(1, Math.ceil(n * p)) - 1] / total;
  };
  return {
    elementos: n,
    total: total,
    pontos: VENDAS_CURVA_PONTOS.map(function (p) { return [p, parte(p)]; }),
    top10: parte(0.1),
    top20: parte(0.2),
    top50: parte(0.5),
    gini: giniLista_(v.slice().reverse())
  };
}

function getVendasData(mes, forceRefresh) {
  return montarVendas_(mes || getMesesDisponiveis()[0], forceRefresh);
}

// Soma uma lista { nome (UF), pedidos, clientes, faturamento } por região do Brasil.
function agruparPorRegiao_(estados) {
  const mapa = {};
  estados.forEach(function (e) {
    const regiao = REGIAO_POR_UF[e.nome] || 'Outros';
    const r = mapa[regiao] || (mapa[regiao] = { nome: regiao, pedidos: 0, clientes: 0, faturamento: 0 });
    r.pedidos += e.pedidos || 0;
    r.clientes += e.clientes || 0;
    r.faturamento += e.faturamento || 0;
  });
  return Object.keys(mapa).map(function (k) { return mapa[k]; }).sort(function (a, b) { return b.pedidos - a.pedidos; });
}

function montarVendas_(valor, forceRefresh) {
  return comCorte_(valor, function () {
    return comConsultasEmParalelo_('vendas ' + valor, function () { return montarVendasInterno_(valor, forceRefresh); });
  });
}

function montarVendasInterno_(valor, forceRefresh) {
  const cache = CacheService.getScriptCache();
  const cacheKey = 'vendas_estrutura_v6_' + valor + sufixoCorte_();
  if (!forceRefresh) {
    const cached = cache.get(cacheKey);
    if (cached) return JSON.parse(cached);
  }

  const anterior = mesAnterior_(valor);
  const mesInicioJanela = mesesAntes_(valor, JANELA_MESES - 1);
  const fmtMes = function (coluna) { return 'date_format(' + coluna + ', \'yyyy-MM\')'; };
  const intervalo = function (coluna) { return fmtMes(coluna) + ' BETWEEN \'' + mesInicioJanela + '\' AND \'' + valor + '\''; };
  const doMes = function (coluna) { return fmtMes(coluna) + ' = \'' + valor + '\''; };
  const ITENS_PEDIDOS = 'FROM ' + TABLES.ITENS_PEDIDOS + ' ip JOIN ' + TABLES.PEDIDOS + ' p ON p.id_pedido = ip.id_pedido ';
  // Primeiro mês em que cada cliente comprou (histórico inteiro): base de "novo" × "recorrente".
  const PRIMEIRA_COMPRA = '(SELECT id_cliente_unico, MIN(' + fmtMes('data_hora_pedido') + ') AS primeiro FROM ' + TABLES.PEDIDOS + ' WHERE ' + CONDICAO_FATURAMENTO + ' GROUP BY 1)';

  // ---------- KPIs: cenário comercial (série de 12 meses) ----------
  const pedidosRaw = queryDatabricks(
    'SELECT ' + fmtMes('data_hora_pedido') + ' AS mes, COUNT(*) AS pedidos, COUNT(DISTINCT id_cliente_unico) AS clientes ' +
    'FROM ' + TABLES.PEDIDOS + ' WHERE ' + intervalo('data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO + ' ' +
    'GROUP BY 1 ORDER BY 1'
  );
  const vendedoresRaw = queryDatabricks(
    'SELECT ' + fmtMes('p.data_hora_pedido') + ' AS mes, COUNT(DISTINCT ip.id_vendedor) AS vendedores ' +
    ITENS_PEDIDOS + 'WHERE ' + intervalo('p.data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO + ' ' +
    'GROUP BY 1 ORDER BY 1'
  );
  const novosRaw = queryDatabricks(
    'SELECT primeiro AS mes, COUNT(*) AS novos FROM ' + PRIMEIRA_COMPRA + ' ' +
    'WHERE primeiro BETWEEN \'' + mesInicioJanela + '\' AND \'' + valor + '\' GROUP BY 1 ORDER BY 1'
  );
  // Mês aberto: o mês anterior só até o mesmo dia (pedidos, clientes, vendedores e novos clientes)
  let antParcial = null;
  if (CORTE_MES_ABERTO_) {
    const pedP = queryDatabricks(
      'SELECT COUNT(*) AS pedidos, COUNT(DISTINCT id_cliente_unico) AS clientes FROM ' + TABLES.PEDIDOS +
      ' WHERE ' + condAnteriorParcial_('data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO
    )[0];
    const vendP = queryDatabricks(
      'SELECT COUNT(DISTINCT ip.id_vendedor) AS vendedores ' + ITENS_PEDIDOS +
      'WHERE ' + condAnteriorParcial_('p.data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO
    )[0];
    const novP = queryDatabricks(
      'SELECT COUNT(*) AS novos FROM (SELECT id_cliente_unico, MIN(data_hora_pedido) AS primeiro FROM ' + TABLES.PEDIDOS +
      ' WHERE ' + CONDICAO_FATURAMENTO + ' GROUP BY 1) WHERE ' + condAnteriorParcial_('primeiro')
    )[0];
    const pedidosP = parseInt(pedP.pedidos, 10) || 0, clientesP = parseInt(pedP.clientes, 10) || 0, vendedoresP = parseInt(vendP.vendedores, 10) || 0;
    const novosP = Math.min(parseInt(novP.novos, 10) || 0, clientesP);
    antParcial = { mes: CORTE_MES_ABERTO_.anterior, pedidos: pedidosP, vendedores: vendedoresP, clientes: clientesP, novos: novosP, recorrentes: clientesP - novosP,
      pedidosPorVendedor: vendedoresP > 0 ? pedidosP / vendedoresP : 0 };
  }
  const vendedoresPorMes = {}, novosPorMes = {};
  vendedoresRaw.forEach(function (r) { vendedoresPorMes[r.mes] = parseInt(r.vendedores, 10) || 0; });
  novosRaw.forEach(function (r) { novosPorMes[r.mes] = parseInt(r.novos, 10) || 0; });

  const janela = pedidosRaw.map(function (r) {
    const pedidos = parseInt(r.pedidos, 10) || 0;
    const clientes = parseInt(r.clientes, 10) || 0;
    const vendedores = vendedoresPorMes[r.mes] || 0;
    const novos = Math.min(novosPorMes[r.mes] || 0, clientes);
    return {
      mes: r.mes,
      pedidos: pedidos,
      vendedores: vendedores,
      clientes: clientes,
      novos: novos,
      recorrentes: clientes - novos,
      pedidosPorVendedor: vendedores > 0 ? pedidos / vendedores : 0
    };
  });
  const porMes = {};
  janela.forEach(function (r) { porMes[r.mes] = r; });
  const atual = porMes[valor] || {};
  const ant = antParcial || porMes[anterior] || {};
  const kpis = {};
  ['vendedores', 'clientes', 'novos', 'recorrentes', 'pedidos', 'pedidosPorVendedor'].forEach(function (campo) {
    kpis[campo] = {
      valor: atual[campo] || 0,
      valorAnterior: ant[campo] === undefined ? null : ant[campo],
      delta: calcularDelta_(atual[campo] || 0, ant[campo])
    };
  });

  // ---------- Vendedores no mês (base de Q1, Q2 e da curva de concentração) ----------
  const vendedoresMesRaw = queryDatabricks(
    'SELECT ip.id_vendedor AS vendedor, MAX(ip.estado_vendedor) AS estado, SUM(ip.preco_produto) AS faturamento, ' +
    '  COUNT(DISTINCT p.id_pedido) AS pedidos, COUNT(DISTINCT p.id_cliente_unico) AS clientes ' +
    ITENS_PEDIDOS + 'WHERE ' + doMes('p.data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO + ' GROUP BY 1'
  );
  const vendedoresMes = vendedoresMesRaw.map(function (r) {
    return {
      vendedor: r.vendedor,
      estado: r.estado,
      faturamento: parseFloat(r.faturamento) || 0,
      pedidos: parseInt(r.pedidos, 10) || 0,
      clientes: parseInt(r.clientes, 10) || 0
    };
  }).sort(function (a, b) { return b.faturamento - a.faturamento; });
  const nVend = vendedoresMes.length;
  const totalFatVend = vendedoresMes.reduce(function (s, v) { return s + v.faturamento; }, 0);

  // Q1 — quantidade de vendedores por faixa de faturamento + participação por grupo de posição
  const limites = DISPERSAO_LIMITES_FAIXA;
  const faixas = [];
  for (let i = 0; i <= limites.length; i++) faixas.push({ rotulo: rotuloFaixa_(limites, i), vendedores: 0, faturamento: 0 });
  vendedoresMes.forEach(function (v) {
    let i = 0;
    while (i < limites.length && v.faturamento >= limites[i]) i++;
    faixas[i].vendedores++;
    faixas[i].faturamento += v.faturamento;
  });
  faixas.forEach(function (f) {
    f.pctVendedores = nVend > 0 ? f.vendedores / nVend : 0;
    f.pctFaturamento = totalFatVend > 0 ? f.faturamento / totalFatVend : 0;
  });
  const distribuicao = {
    vendedores: nVend,
    faturamento: totalFatVend,
    mediana: medianaLista_(vendedoresMes.map(function (v) { return v.faturamento; })),
    media: nVend > 0 ? totalFatVend / nVend : 0,
    faixas: faixas
  };
  // Pedidos por grupo: soma dos pedidos de cada vendedor (um pedido com vendedores diferentes conta em cada um).
  const totalPedidosVend = vendedoresMes.reduce(function (s, v) { return s + v.pedidos; }, 0);
  const percentis = VENDAS_GRUPOS_PERCENTIL.map(function (g) {
    const ini = Math.ceil(nVend * g.de), fim = Math.ceil(nVend * g.ate);
    const grupo = vendedoresMes.slice(ini, fim);
    const fat = grupo.reduce(function (s, v) { return s + v.faturamento; }, 0);
    const ped = grupo.reduce(function (s, v) { return s + v.pedidos; }, 0);
    return {
      rotulo: g.rotulo,
      vendedores: grupo.length,
      pctVendedores: nVend > 0 ? grupo.length / nVend : 0,
      faturamento: fat,
      pctFaturamento: totalFatVend > 0 ? fat / totalFatVend : 0,
      pedidos: ped,
      pctPedidos: totalPedidosVend > 0 ? ped / totalPedidosVend : 0
    };
  });

  // Q2 — cada ponto é um vendedor: [clientes, pedidos, faturamento, id curto, estado]
  const pontosVendedores = vendedoresMes.slice(0, VENDAS_SCATTER_MAX).map(function (v) {
    return [v.clientes, v.pedidos, Math.round(v.faturamento * 100) / 100, String(v.vendedor).slice(0, 6), v.estado];
  });
  // Q2 — perfis de atuação: 4 quadrantes definidos pelas medianas de clientes atendidos e de pedidos no mês
  // ("muitos" = acima da mediana; "poucos" = na mediana ou abaixo).
  const medianaClientes = medianaLista_(vendedoresMes.map(function (v) { return v.clientes; }));
  const medianaPedidos = medianaLista_(vendedoresMes.map(function (v) { return v.pedidos; }));
  const quadrantes = [
    { chave: 'poucos-muitos', clientes: 'Poucos clientes', pedidos: 'Muitos pedidos', vendedores: 0, faturamento: 0, pedidosTotal: 0, clientesTotal: 0 },
    { chave: 'muitos-muitos', clientes: 'Muitos clientes', pedidos: 'Muitos pedidos', vendedores: 0, faturamento: 0, pedidosTotal: 0, clientesTotal: 0 },
    { chave: 'poucos-poucos', clientes: 'Poucos clientes', pedidos: 'Poucos pedidos', vendedores: 0, faturamento: 0, pedidosTotal: 0, clientesTotal: 0 },
    { chave: 'muitos-poucos', clientes: 'Muitos clientes', pedidos: 'Poucos pedidos', vendedores: 0, faturamento: 0, pedidosTotal: 0, clientesTotal: 0 }
  ];
  vendedoresMes.forEach(function (v) {
    const muitosClientes = v.clientes > medianaClientes;
    const muitosPedidos = v.pedidos > medianaPedidos;
    const q = quadrantes[(muitosPedidos ? 0 : 2) + (muitosClientes ? 1 : 0)];
    q.vendedores++;
    q.faturamento += v.faturamento;
    q.pedidosTotal += v.pedidos;
    q.clientesTotal += v.clientes;
  });
  quadrantes.forEach(function (q) {
    q.pctVendedores = nVend > 0 ? q.vendedores / nVend : 0;
    q.pctFaturamento = totalFatVend > 0 ? q.faturamento / totalFatVend : 0;
    q.ticket = q.pedidosTotal > 0 ? q.faturamento / q.pedidosTotal : 0;
  });
  const padroes = {
    total: nVend,
    faturamento: totalFatVend,
    medianaClientes: medianaClientes,
    medianaPedidos: medianaPedidos,
    quadrantes: quadrantes
  };

  // ---------- Q3 — carteiras: vendedores por tamanho de carteira × concentração no maior cliente (12 meses) ----------
  const cteCarteira =
    'WITH sc AS (' +
    '  SELECT ip.id_vendedor, p.id_cliente_unico, SUM(ip.preco_produto) AS fat ' +
    '  ' + ITENS_PEDIDOS + 'WHERE ' + intervalo('p.data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO + ' GROUP BY 1, 2' +
    '), v AS (' +
    '  SELECT id_vendedor, COUNT(*) AS clientes, MAX(fat) / SUM(fat) AS topo FROM sc GROUP BY 1' +
    ') ';
  const carteiraRaw = queryDatabricks(
    cteCarteira +
    'SELECT CASE WHEN clientes = 1 THEN 0 WHEN clientes <= 5 THEN 1 WHEN clientes <= 10 THEN 2 WHEN clientes <= 25 THEN 3 ' +
    '            WHEN clientes <= 50 THEN 4 WHEN clientes <= 100 THEN 5 ELSE 6 END AS faixa, ' +
    '  CASE WHEN clientes = 1 THEN 3 WHEN topo >= 0.5 THEN 2 WHEN topo >= 0.25 THEN 1 ELSE 0 END AS conc, ' +
    '  COUNT(*) AS vendedores FROM v GROUP BY 1, 2'
  );
  const carteiraStatsRaw = queryDatabricks(cteCarteira + 'SELECT percentile(clientes, 0.5) AS mediana, AVG(clientes) AS media FROM v');
  const carteiraMatriz = VENDAS_FAIXAS_CARTEIRA.map(function () { return [0, 0, 0, 0]; });
  carteiraRaw.forEach(function (r) {
    const f = parseInt(r.faixa, 10), c = parseInt(r.conc, 10);
    if (carteiraMatriz[f] && c >= 0 && c < 4) carteiraMatriz[f][c] += parseInt(r.vendedores, 10) || 0;
  });
  const carteira = {
    faixas: VENDAS_FAIXAS_CARTEIRA,
    classes: VENDAS_CLASSES_CONCENTRACAO,
    matriz: carteiraMatriz,
    vendedores: carteiraMatriz.reduce(function (s, l) { return s + l.reduce(function (a, b) { return a + b; }, 0); }, 0),
    mediana: carteiraStatsRaw.length ? parseFloat(carteiraStatsRaw[0].mediana) || 0 : 0,
    media: carteiraStatsRaw.length ? parseFloat(carteiraStatsRaw[0].media) || 0 : 0
  };

  // ---------- Q4 — comportamento da base: frequência × intervalo médio entre compras (12 meses) ----------
  const comportamentoRaw = queryDatabricks(
    'WITH c AS (' +
    '  SELECT id_cliente_unico, COUNT(*) AS pedidos, ' +
    '    DATEDIFF(MAX(data_hora_pedido), MIN(data_hora_pedido)) / NULLIF(COUNT(*) - 1, 0) AS intervalo ' +
    '  FROM ' + TABLES.PEDIDOS + ' WHERE ' + intervalo('data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO + ' GROUP BY 1' +
    ') ' +
    'SELECT CASE WHEN pedidos = 1 THEN 0 WHEN pedidos = 2 THEN 1 WHEN pedidos = 3 THEN 2 ELSE 3 END AS freq, ' +
    '  CASE WHEN intervalo IS NULL THEN 9 WHEN intervalo <= 30 THEN 0 WHEN intervalo <= 90 THEN 1 WHEN intervalo <= 180 THEN 2 ELSE 3 END AS intv, ' +
    '  COUNT(*) AS clientes, SUM(pedidos) AS pedidos FROM c GROUP BY 1, 2'
  );
  const compMatriz = VENDAS_FREQUENCIAS.map(function () { return [0, 0, 0, 0]; });
  let clientesUnicos = 0, pedidosUnicos = 0;
  comportamentoRaw.forEach(function (r) {
    const f = parseInt(r.freq, 10), i = parseInt(r.intv, 10);
    const n = parseInt(r.clientes, 10) || 0;
    if (f === 0) { clientesUnicos += n; pedidosUnicos += parseInt(r.pedidos, 10) || 0; }
    else if (compMatriz[f - 1] && i >= 0 && i < 4) compMatriz[f - 1][i] += n;
  });
  const clientesRecorrentes12m = compMatriz.reduce(function (s, l) { return s + l.reduce(function (a, b) { return a + b; }, 0); }, 0);
  const comportamento = {
    frequencias: VENDAS_FREQUENCIAS,
    intervalos: VENDAS_INTERVALOS,
    matriz: compMatriz,
    unicos: clientesUnicos,
    recorrentes: clientesRecorrentes12m,
    totalClientes: clientesUnicos + clientesRecorrentes12m
  };

  // ---------- Q5 — mercados: pedidos e clientes por estado, cidade, região e categoria (mês) ----------
  const estadoRaw = queryDatabricks(
    'SELECT estado_cliente AS nome, COUNT(*) AS pedidos, COUNT(DISTINCT id_cliente_unico) AS clientes, SUM(valor_total_produtos) AS faturamento ' +
    'FROM ' + TABLES.PEDIDOS + ' WHERE ' + doMes('data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO + ' AND estado_cliente IS NOT NULL GROUP BY 1'
  );
  const estados = estadoRaw.map(function (r) {
    return { nome: r.nome, pedidos: parseInt(r.pedidos, 10) || 0, clientes: parseInt(r.clientes, 10) || 0, faturamento: parseFloat(r.faturamento) || 0 };
  }).sort(function (a, b) { return b.pedidos - a.pedidos; });
  const cidadeRaw = queryDatabricks(
    'SELECT concat(cidade_cliente, \' (\', estado_cliente, \')\') AS nome, COUNT(*) AS pedidos, COUNT(DISTINCT id_cliente_unico) AS clientes ' +
    'FROM ' + TABLES.PEDIDOS + ' WHERE ' + doMes('data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO + ' AND cidade_cliente IS NOT NULL AND estado_cliente IS NOT NULL ' +
    'GROUP BY 1 ORDER BY 2 DESC LIMIT ' + VENDAS_CIDADES_MAX
  );
  const categoriaRaw = queryDatabricks(
    'SELECT ip.categoria_produto AS nome, COUNT(DISTINCT p.id_pedido) AS pedidos, COUNT(DISTINCT p.id_cliente_unico) AS clientes ' +
    ITENS_PEDIDOS + 'WHERE ' + doMes('p.data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO + ' AND ip.categoria_produto IS NOT NULL GROUP BY 1'
  );
  const semFaturamento = function (l) { return { nome: l.nome, pedidos: l.pedidos, clientes: l.clientes }; };
  const mercados = {
    totalPedidos: atual.pedidos || 0,
    totalClientes: atual.clientes || 0,
    estado: estados.map(semFaturamento),
    regiao: agruparPorRegiao_(estados).map(semFaturamento),
    cidade: cidadeRaw.map(function (r) { return { nome: r.nome, pedidos: parseInt(r.pedidos, 10) || 0, clientes: parseInt(r.clientes, 10) || 0 }; }),
    categoria: categoriaRaw.map(function (r) { return { nome: r.nome, pedidos: parseInt(r.pedidos, 10) || 0, clientes: parseInt(r.clientes, 10) || 0 }; })
      .sort(function (a, b) { return b.pedidos - a.pedidos; })
  };

  // ---------- Q6 — perfis: pedidos de clientes novos × recorrentes por região, estado e faixa de valor (mês) ----------
  const perfilBase = function (colunaDim, extraWhere) {
    return queryDatabricks(
      'SELECT ' + colunaDim + ' AS dim, CASE WHEN f.primeiro = \'' + valor + '\' THEN 0 ELSE 1 END AS tipo, ' +
      '  COUNT(*) AS pedidos, COUNT(DISTINCT p.id_cliente_unico) AS clientes ' +
      'FROM ' + TABLES.PEDIDOS + ' p JOIN ' + PRIMEIRA_COMPRA + ' f ON f.id_cliente_unico = p.id_cliente_unico ' +
      'WHERE ' + doMes('p.data_hora_pedido') + ' AND p.' + CONDICAO_FATURAMENTO + extraWhere + ' GROUP BY 1, 2'
    );
  };
  const juntaPerfil = function (linhas, nomeDe) {
    const mapa = {};
    linhas.forEach(function (r) {
      const nome = nomeDe(r.dim);
      const item = mapa[nome] || (mapa[nome] = { nome: nome, novos: { pedidos: 0, clientes: 0 }, recorrentes: { pedidos: 0, clientes: 0 } });
      const alvo = parseInt(r.tipo, 10) === 0 ? item.novos : item.recorrentes;
      alvo.pedidos += parseInt(r.pedidos, 10) || 0;
      alvo.clientes += parseInt(r.clientes, 10) || 0;
    });
    return Object.keys(mapa).map(function (k) { return mapa[k]; });
  };
  const totalPerfil = function (i) { return i.novos.pedidos + i.recorrentes.pedidos; };
  const perfilEstado = juntaPerfil(perfilBase('p.estado_cliente', ' AND p.estado_cliente IS NOT NULL'), function (d) { return d; });
  const perfilRegiao = juntaPerfil(perfilBase('p.estado_cliente', ' AND p.estado_cliente IS NOT NULL'), function (d) { return REGIAO_POR_UF[d] || 'Outros'; });
  const perfilFaixa = juntaPerfil(
    perfilBase('CASE WHEN p.valor_total_produtos < 50 THEN 0 WHEN p.valor_total_produtos < 100 THEN 1 WHEN p.valor_total_produtos < 200 THEN 2 WHEN p.valor_total_produtos < 500 THEN 3 ELSE 4 END', ''),
    function (d) { return VENDAS_FAIXAS_TICKET_PEDIDO[parseInt(d, 10)] || '—'; }
  );
  const perfis = {
    regiao: perfilRegiao.sort(function (a, b) { return totalPerfil(b) - totalPerfil(a); }),
    estado: perfilEstado.sort(function (a, b) { return totalPerfil(b) - totalPerfil(a); }).slice(0, VENDAS_PERFIL_ESTADOS_TOP_N),
    faixaTicket: perfilFaixa.sort(function (a, b) { return VENDAS_FAIXAS_TICKET_PEDIDO.indexOf(a.nome) - VENDAS_FAIXAS_TICKET_PEDIDO.indexOf(b.nome); }),
    totalPedidos: atual.pedidos || 0
  };

  // ---------- Q7 — concentração do resultado (faturamento) por vendedor, cliente, categoria e estado ----------
  const clientesFatRaw = queryDatabricks(
    'SELECT SUM(valor_total_produtos) AS faturamento FROM ' + TABLES.PEDIDOS + ' ' +
    'WHERE ' + doMes('data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO + ' GROUP BY id_cliente_unico'
  );
  const catFatRaw = queryDatabricks(
    'SELECT SUM(ip.preco_produto) AS faturamento ' + ITENS_PEDIDOS +
    'WHERE ' + doMes('p.data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO + ' AND ip.categoria_produto IS NOT NULL GROUP BY ip.categoria_produto'
  );
  const numeros = function (linhas) { return linhas.map(function (r) { return parseFloat(r.faturamento) || 0; }); };
  const concentracao = {
    vendedor: curvaConcentracao_(vendedoresMes.map(function (v) { return v.faturamento; })),
    cliente: curvaConcentracao_(numeros(clientesFatRaw)),
    categoria: curvaConcentracao_(numeros(catFatRaw)),
    estado: curvaConcentracao_(estados.map(function (e) { return e.faturamento; }))
  };

  const data = {
    tipo: 'mes',
    periodo: valor,
    periodoAnterior: anterior,
    comparacao: comparacaoInfo_(),
    kpis: kpis,
    janela: janela,
    distribuicao: distribuicao,
    percentis: percentis,
    padroes: padroes,
    vendedoresPontos: pontosVendedores,
    carteira: carteira,
    comportamento: comportamento,
    mercados: mercados,
    perfis: perfis,
    concentracao: concentracao,
    atualizadoEm: new Date().toISOString()
  };

  // O CacheService aceita até ~100KB por chave: se a dispersão de vendedores estourar, reduz até caber.
  let json = JSON.stringify(data);
  while (json.length > VENDAS_CACHE_LIMITE_CHARS && data.vendedoresPontos.length > 100) {
    data.vendedoresPontos = data.vendedoresPontos.slice(0, Math.floor(data.vendedoresPontos.length * 0.8));
    json = JSON.stringify(data);
  }
  if (!CONSULTAS_COLETA_) cache.put(cacheKey, json, AGGREGATE_CACHE_SECONDS);
  return data;
}

// ===== Visão Receita & Performance =====

// Até 10 maiores altas e 10 maiores quedas nos gráficos de variação (categorias, cidades, composição).
// O front mostra 5+5 por padrão, ou 10 de um lado só nos botões "Só altas" / "Só quedas".
const RECEITA_VARIACAO_POR_LADO = 10;
// Quantas categorias/cidades aparecem na composição do mês (o restante vira "Outros").
const RECEITA_COMPOSICAO_TOP_N = 10;
// Ranking de cidades por % ignora cidades minúsculas (uma cidade que vai de R$ 10 para R$ 50 seria +400%).
const RECEITA_CIDADES_MIN_PARTICIPACAO = 0.002;
// Quantas categorias aparecem individualmente no Pareto (a % acumulada considera TODAS as categorias).
const RECEITA_PARETO_TOP_N = 20;

function getReceitaData(mes, forceRefresh) {
  return montarReceita_(mes || getMesesDisponiveis()[0], forceRefresh);
}

function medianaLista_(valores) {
  if (!valores.length) return 0;
  const ord = valores.slice().sort(function (a, b) { return a - b; });
  const meio = Math.floor(ord.length / 2);
  return ord.length % 2 ? ord[meio] : (ord[meio - 1] + ord[meio]) / 2;
}

// Lê linhas { mes, nome, faturamento } e devolve os mapas nome→faturamento do período e do anterior,
// mais o total de cada um.
function separarPeriodosReceita_(linhas, valor, anterior) {
  const atual = {}, ant = {};
  let totalAtual = 0, totalAnterior = 0;
  linhas.forEach(function (r) {
    const fat = parseFloat(r.faturamento) || 0;
    if (r.mes === valor) { atual[r.nome] = fat; totalAtual += fat; }
    else if (r.mes === anterior) { ant[r.nome] = fat; totalAnterior += fat; }
  });
  return { atual: atual, ant: ant, totalAtual: totalAtual, totalAnterior: totalAnterior };
}

// Todas as entradas (categoria/cidade) com variação em R$, em % e em participação (p.p.) entre os dois períodos.
function itensVariacaoReceita_(p) {
  const nomes = {};
  Object.keys(p.atual).concat(Object.keys(p.ant)).forEach(function (n) { nomes[n] = true; });
  return Object.keys(nomes).map(function (n) {
    const a = p.atual[n] || 0, b = p.ant[n] || 0;
    const pa = p.totalAtual > 0 ? a / p.totalAtual : 0;
    const pb = p.totalAnterior > 0 ? b / p.totalAnterior : 0;
    return {
      nome: n,
      atual: a,
      anterior: b,
      delta: a - b,
      variacao: b > 0 ? (a - b) / b : null,
      participacao: pa,
      participacaoAnterior: pb,
      deltaPP: (pa - pb) * 100
    };
  });
}

// As N maiores altas e as N maiores quedas segundo `valor`, em ordem decrescente: as altas primeiro, as
// quedas por último (da menor para a maior queda). Ranquear em R$ evita que um item que vai de R$ 1 mil
// para R$ 5 mil pareça mais importante que outro que cai de R$ 500 mil para R$ 400 mil.
function topAltasQuedasReceita_(lista, valor, porLado) {
  const ord = lista.slice().sort(function (x, y) { return valor(y) - valor(x); });
  const altas = ord.filter(function (x) { return valor(x) > 0; }).slice(0, porLado);
  const quedas = ord.filter(function (x) { return valor(x) < 0; }).slice(-porLado);
  return altas.concat(quedas);
}

// Composição do mês por categoria ou cidade, em duas medidas: faturamento (R$) e quantidade (pedidos).
// Cada medida tem a própria lista (as N maiores nela + "Outros"), com as duas participações em cada item.
function composicaoDuplaReceita_(fat, ped, topN) {
  const nomes = Object.keys(fat);
  const totalFaturamento = nomes.reduce(function (s, n) { return s + fat[n]; }, 0);
  const totalPedidos = nomes.reduce(function (s, n) { return s + (ped[n] || 0); }, 0);
  const item = function (nome, f, p) {
    return {
      nome: nome,
      faturamento: f,
      pedidos: p,
      participacaoFaturamento: totalFaturamento > 0 ? f / totalFaturamento : 0,
      participacaoPedidos: totalPedidos > 0 ? p / totalPedidos : 0
    };
  };
  const lista = function (medida) {
    const valor = function (n) { return medida === 'pedidos' ? (ped[n] || 0) : fat[n]; };
    const ord = nomes.slice().sort(function (a, b) { return valor(b) - valor(a); });
    const itens = ord.slice(0, topN).map(function (n) { return item(n, fat[n], ped[n] || 0); });
    const resto = ord.slice(topN);
    if (resto.length) {
      const outros = item('Outros (' + resto.length + ')',
        resto.reduce(function (s, n) { return s + fat[n]; }, 0),
        resto.reduce(function (s, n) { return s + (ped[n] || 0); }, 0));
      outros.outros = true;
      itens.push(outros);
    }
    return itens;
  };
  return {
    faturamento: lista('faturamento'),
    pedidos: lista('pedidos'),
    totalFaturamento: totalFaturamento,
    totalPedidos: totalPedidos,
    quantidade: nomes.length
  };
}

function montarReceita_(valor, forceRefresh) {
  return comCorte_(valor, function () {
    return comConsultasEmParalelo_('receita ' + valor, function () { return montarReceitaInterno_(valor, forceRefresh); });
  });
}

function montarReceitaInterno_(valor, forceRefresh) {
  const cache = CacheService.getScriptCache();
  const cacheKey = 'receita_mes_v11_' + valor + sufixoCorte_();
  if (!forceRefresh) {
    const cached = cache.get(cacheKey);
    if (cached) return JSON.parse(cached);
  }

  const anterior = mesAnterior_(valor);
  const mesInicioJanela = mesesAntes_(valor, JANELA_MESES - 1);
  // Comparações "mês × mês anterior": com mês aberto, o anterior entra só até o mesmo dia.
  const doisMesesCond = function (coluna) {
    return CORTE_MES_ABERTO_
      ? '(date_format(' + coluna + ', \'yyyy-MM\') = \'' + valor + '\' OR (' + condAnteriorParcial_(coluna) + '))'
      : 'date_format(' + coluna + ', \'yyyy-MM\') IN (\'' + valor + '\', \'' + anterior + '\')';
  };

  // Faturamento (aqui: receita líquida) = valor dos produtos; custo = frete; faturamento total = produtos + frete; margem = receita líquida ÷ faturamento total (antes: margem líquida de
  // frete, os dados não têm o custo do produto). Ticket = faturamento ÷ pedidos.
  const pedidosRaw = queryDatabricks(
    'SELECT date_format(data_hora_pedido, \'yyyy-MM\') AS mes, ' +
    '  SUM(valor_total_produtos) AS faturamento, ' +
    '  SUM(valor_total_frete) AS custo_frete, ' +
    '  COUNT(*) AS pedidos ' +
    'FROM ' + TABLES.PEDIDOS + ' ' +
    'WHERE date_format(data_hora_pedido, \'yyyy-MM\') BETWEEN \'' + mesInicioJanela + '\' AND \'' + valor + '\' ' +
    '  AND ' + CONDICAO_FATURAMENTO + ' ' +
    'GROUP BY 1 ORDER BY 1'
  );

  const janela = pedidosRaw.map(function (r) {
    const faturamento = parseFloat(r.faturamento) || 0;
    const custoFrete = parseFloat(r.custo_frete) || 0;
    const pedidos = parseInt(r.pedidos, 10) || 0;
    return {
      mes: r.mes,
      faturamento: faturamento,
      custoFrete: custoFrete,
      faturamentoTotal: faturamento + custoFrete,
      margemPct: faturamento + custoFrete > 0 ? faturamento / (faturamento + custoFrete) : 0,
      pedidos: pedidos,
      ticketMedio: pedidos > 0 ? (faturamento + custoFrete) / pedidos : 0
    };
  });

  const porMes = {};
  janela.forEach(function (r) { porMes[r.mes] = r; });
  const atualMes = porMes[valor] || {};
  let antMes = porMes[anterior] || {};
  if (CORTE_MES_ABERTO_) {
    const rP = queryDatabricks(
      'SELECT SUM(valor_total_produtos) AS faturamento, SUM(valor_total_frete) AS custo_frete, COUNT(*) AS pedidos FROM ' + TABLES.PEDIDOS +
      ' WHERE ' + condAnteriorParcial_('data_hora_pedido') + ' AND ' + CONDICAO_FATURAMENTO
    )[0];
    const fatP = parseFloat(rP.faturamento) || 0, freteP = parseFloat(rP.custo_frete) || 0, pedP = parseInt(rP.pedidos, 10) || 0;
    antMes = { mes: anterior, faturamento: fatP, custoFrete: freteP, faturamentoTotal: fatP + freteP, margemPct: fatP + freteP > 0 ? fatP / (fatP + freteP) : 0, pedidos: pedP, ticketMedio: pedP > 0 ? (fatP + freteP) / pedP : 0 };
  }
  const kpis = {};
  ['faturamento', 'custoFrete', 'faturamentoTotal', 'margemPct', 'ticketMedio', 'pedidos'].forEach(function (campo) {
    kpis[campo] = {
      valor: atualMes[campo] || 0,
      valorAnterior: antMes[campo] === undefined ? null : antMes[campo],
      delta: calcularDelta_(atualMes[campo] || 0, antMes[campo])
    };
  });

  // ----- Cidades (do cliente): maiores variações em R$ -----
  const cidadesRaw = queryDatabricks(
    'SELECT date_format(data_hora_pedido, \'yyyy-MM\') AS mes, ' +
    '  concat(cidade_cliente, \' (\', estado_cliente, \')\') AS nome, ' +
    '  SUM(valor_total_produtos) AS faturamento, COUNT(*) AS pedidos ' +
    'FROM ' + TABLES.PEDIDOS + ' ' +
    'WHERE ' + doisMesesCond('data_hora_pedido') + ' ' +
    '  AND ' + CONDICAO_FATURAMENTO + ' AND cidade_cliente IS NOT NULL AND estado_cliente IS NOT NULL ' +
    'GROUP BY 1, 2'
  );
  const cid = separarPeriodosReceita_(cidadesRaw, valor, anterior);
  const cidItens = itensVariacaoReceita_(cid);
  const pedidosDaCidade = {};
  cidadesRaw.forEach(function (r) {
    if (r.mes === valor) pedidosDaCidade[r.nome] = parseInt(r.pedidos, 10) || 0;
  });
  const cidRelevantes = cidItens.filter(function (x) {
    return x.variacao != null && Math.max(x.participacao, x.participacaoAnterior) >= RECEITA_CIDADES_MIN_PARTICIPACAO;
  });
  const cidades = {
    // Um ranking pré-calculado por métrica (são milhares de cidades: não dá para mandar todas ao navegador).
    variacoes: {
      valor: topAltasQuedasReceita_(cidItens, function (x) { return x.delta; }, RECEITA_VARIACAO_POR_LADO),
      pct: topAltasQuedasReceita_(cidRelevantes, function (x) { return x.variacao; }, RECEITA_VARIACAO_POR_LADO),
      pp: topAltasQuedasReceita_(cidItens, function (x) { return x.deltaPP; }, RECEITA_VARIACAO_POR_LADO)
    },
    minParticipacao: RECEITA_CIDADES_MIN_PARTICIPACAO,
    composicao: composicaoDuplaReceita_(cid.atual, pedidosDaCidade, RECEITA_COMPOSICAO_TOP_N),
    total: cid.totalAtual,
    quantidade: Object.keys(cid.atual).length
  };

  // ----- Categorias: composição (participação) e pedidos × ticket -----
  const categoriasRaw = queryDatabricks(
    'SELECT date_format(p.data_hora_pedido, \'yyyy-MM\') AS mes, ip.categoria_produto AS nome, ' +
    '  SUM(ip.preco_produto) AS faturamento, COUNT(DISTINCT p.id_pedido) AS pedidos ' +
    'FROM ' + TABLES.ITENS_PEDIDOS + ' ip ' +
    'JOIN ' + TABLES.PEDIDOS + ' p ON p.id_pedido = ip.id_pedido ' +
    'WHERE ' + doisMesesCond('p.data_hora_pedido') + ' ' +
    '  AND ' + CONDICAO_FATURAMENTO + ' AND ip.categoria_produto IS NOT NULL ' +
    'GROUP BY 1, 2'
  );
  const cat = separarPeriodosReceita_(categoriasRaw, valor, anterior);

  // Pedidos distintos que contêm a categoria, no mês (base para ticket por categoria e para o "por quê" do Pareto).
  const pedidosDaCategoria = {};
  let pedidosTotalCategorias = 0;
  categoriasRaw.forEach(function (r) {
    if (r.mes !== valor) return;
    pedidosDaCategoria[r.nome] = parseInt(r.pedidos, 10) || 0;
    pedidosTotalCategorias += pedidosDaCategoria[r.nome];
  });
  // Ticket de referência: faturamento de todas as categorias ÷ pedidos por categoria (um pedido com 2
  // categorias conta uma vez em cada), para comparar a categoria com a média na mesma base.
  const ticketBaseCategorias = pedidosTotalCategorias > 0 ? cat.totalAtual / pedidosTotalCategorias : 0;

  // Quem mudou (variação em R$) e quem sustenta (Pareto) — por categoria, porque os dados não têm o nome do produto.
  const categoriasOrdenadas = Object.keys(cat.atual).sort(function (a, b) { return cat.atual[b] - cat.atual[a]; });
  let acumulado = 0, n50 = 0, n80 = 0;
  const paretoTodas = categoriasOrdenadas.map(function (n, i) {
    acumulado += cat.atual[n];
    const pct = cat.totalAtual > 0 ? acumulado / cat.totalAtual : 0;
    if (!n50 && pct >= 0.5) n50 = i + 1;
    if (!n80 && pct >= 0.8) n80 = i + 1;
    const pedidos = pedidosDaCategoria[n] || 0;
    const ticket = pedidos > 0 ? cat.atual[n] / pedidos : 0;
    return {
      nome: n,
      faturamento: cat.atual[n],
      participacao: cat.totalAtual > 0 ? cat.atual[n] / cat.totalAtual : 0,
      acumulado: pct,
      pedidos: pedidos,
      participacaoPedidos: pedidosTotalCategorias > 0 ? pedidos / pedidosTotalCategorias : 0,
      ticket: ticket,
      // > 1: ticket acima da média (a categoria pesa mais no faturamento do que nos pedidos)
      razaoTicket: ticketBaseCategorias > 0 ? ticket / ticketBaseCategorias : 0
    };
  });
  const pareto = {
    itens: paretoTodas.slice(0, RECEITA_PARETO_TOP_N),
    total: cat.totalAtual,
    totalCategorias: categoriasOrdenadas.length,
    ticketBase: ticketBaseCategorias,
    n50: n50,
    n80: n80
  };

  // Todas as categorias com variação em R$, em % e em participação (p.p.): o front escolhe a métrica e o
  // filtro (altas/quedas) sem nova consulta.
  const nomesCat = {};
  Object.keys(cat.atual).concat(Object.keys(cat.ant)).forEach(function (n) { nomesCat[n] = true; });
  const categoriasItens = Object.keys(nomesCat).map(function (n) {
    const a = cat.atual[n] || 0, b = cat.ant[n] || 0;
    const pa = cat.totalAtual > 0 ? a / cat.totalAtual : 0;
    const pb = cat.totalAnterior > 0 ? b / cat.totalAnterior : 0;
    return {
      nome: n,
      atual: a,
      anterior: b,
      delta: a - b,
      variacao: b > 0 ? (a - b) / b : null,
      participacao: pa,
      participacaoAnterior: pb,
      deltaPP: (pa - pb) * 100
    };
  });

  const pontos = Object.keys(cat.atual).map(function (n) {
    const pedidos = pedidosDaCategoria[n] || 0;
    return {
      nome: n,
      faturamento: cat.atual[n],
      pedidos: pedidos,
      ticket: pedidos > 0 ? cat.atual[n] / pedidos : 0,
      participacao: cat.totalAtual > 0 ? cat.atual[n] / cat.totalAtual : 0
    };
  }).filter(function (x) { return x.pedidos > 0; });

  const data = {
    tipo: 'mes',
    periodo: valor,
    periodoAnterior: anterior,
    comparacao: comparacaoInfo_(),
    kpis: kpis,
    janela: janela,
    cidades: cidades,
    categorias: {
      itens: categoriasItens,
      composicao: composicaoDuplaReceita_(cat.atual, pedidosDaCategoria, RECEITA_COMPOSICAO_TOP_N),
      pareto: pareto,
      pontos: pontos,
      medianaPedidos: medianaLista_(pontos.map(function (x) { return x.pedidos; })),
      medianaTicket: medianaLista_(pontos.map(function (x) { return x.ticket; }))
    },
    atualizadoEm: new Date().toISOString()
  };

  if (!CONSULTAS_COLETA_) cache.put(cacheKey, JSON.stringify(data), AGGREGATE_CACHE_SECONDS);
  return data;
}

// ===== Visão Logística =====
// Pergunta central: como está o resultado da operação de entrega, onde está o gargalo e o que ele custa?
// Narrativa: resultado → evolução → promessa → gargalo → onde/rotas → custo e impacto → pedidos parados → sinais.
// Base dos tempos e do cumprimento: pedidos ENTREGUES (pedido_entregue). "No prazo" = entregue na data estimada ou antes.
// Tempo total = pedido → entrega ao cliente; até envio = pedido → transportadora; transporte = transportadora → cliente.
// Frete = o frete cobrado do cliente (não é o custo real da operação). Uma única chamada (getLogisticaData) devolve tudo.

// Mínimo de pedidos entregues para um grupo (estado, cidade, rota…) aparecer nos gráficos.
const LOGISTICA_MIN_PEDIDOS = 30;
const LOGISTICA_CIDADES_MAX = 80;
const LOGISTICA_CATEGORIAS_MAX = 25;
const LOGISTICA_ROTAS_ORIGENS = 6;
const LOGISTICA_ROTAS_DESTINOS = 8;
const LOGISTICA_FAIXAS_DESVIO = ['Mais de 15 dias antes', '8 a 15 dias antes', '1 a 7 dias antes', 'No dia previsto', '1 a 3 dias depois', '4 a 7 dias depois', 'Mais de 7 dias depois'];
const LOGISTICA_FAIXAS_TEMPO = ['Até 7 dias', '8 a 14 dias', '15 a 21 dias', '22 a 30 dias', 'Mais de 30 dias'];
const LOGISTICA_FAIXAS_ITENS = ['1 item', '2 itens', '3 itens', '4 ou mais itens'];
const LOGISTICA_NIVEIS_CUMPRIMENTO = ['Entregue no prazo', 'Atraso de 1 a 7 dias', 'Atraso de 8 a 15 dias', 'Atraso de 16 dias ou mais'];
const LOGISTICA_FAIXAS_IDADE = ['Até 7 dias', '8 a 15 dias', '16 a 30 dias', 'Mais de 30 dias'];
const LOGISTICA_STATUS_ANDAMENTO = ['Enviado', 'Processando', 'Faturado', 'Aprovado', 'Criado'];

function getLogisticaData(mes, forceRefresh) {
  return montarLogistica_(mes || getMesesDisponiveis()[0], forceRefresh);
}

// Linha de resultado → grupo (médias em dias; frete em R$). `nome` sobrescreve r.nome.
function grupoLogistica_(r, nome) {
  return {
    nome: nome !== undefined ? nome : r.nome,
    pedidos: parseInt(r.pedidos, 10) || 0,
    atrasados: parseInt(r.atrasados, 10) || 0,
    total: parseFloat(r.total) || 0,
    transporte: parseFloat(r.transporte) || 0,
    prometido: parseFloat(r.prometido) || 0,
    frete: parseFloat(r.frete) || 0
  };
}

// Junta estados em regiões do Brasil: soma pedidos/atrasados e pondera as médias pelos pedidos.
function agruparLogisticaPorRegiao_(estados) {
  const mapa = {};
  estados.forEach(function (e) {
    const nome = REGIAO_POR_UF[e.nome] || 'Outros';
    const r = mapa[nome] || (mapa[nome] = { nome: nome, pedidos: 0, atrasados: 0, s: { total: 0, transporte: 0, prometido: 0, frete: 0 } });
    r.pedidos += e.pedidos;
    r.atrasados += e.atrasados;
    ['total', 'transporte', 'prometido', 'frete'].forEach(function (c) { r.s[c] += e[c] * e.pedidos; });
  });
  return Object.keys(mapa).map(function (k) {
    const r = mapa[k], p = r.pedidos;
    return { nome: r.nome, pedidos: p, atrasados: r.atrasados, total: p > 0 ? r.s.total / p : 0, transporte: p > 0 ? r.s.transporte / p : 0, prometido: p > 0 ? r.s.prometido / p : 0, frete: p > 0 ? r.s.frete / p : 0 };
  }).sort(function (a, b) { return b.pedidos - a.pedidos; });
}

function montarLogistica_(valor, forceRefresh) {
  return comCorte_(valor, function () {
    return comConsultasEmParalelo_('logistica ' + valor, function () { return montarLogisticaInterno_(valor, forceRefresh); });
  });
}

function montarLogisticaInterno_(valor, forceRefresh) {
  const cache = CacheService.getScriptCache();
  const cacheKey = 'logistica_v3_' + valor + sufixoCorte_();
  if (!forceRefresh) {
    const cached = cache.get(cacheKey);
    if (cached) return JSON.parse(cached);
  }

  const anterior = mesAnterior_(valor);
  const mesInicioJanela = mesesAntes_(valor, JANELA_MESES - 1);
  const fmtMes = function (coluna) { return 'date_format(' + coluna + ', \'yyyy-MM\')'; };
  const intervalo = function (coluna) { return fmtMes(coluna) + ' BETWEEN \'' + mesInicioJanela + '\' AND \'' + valor + '\''; };
  const doMes = function (coluna) { return fmtMes(coluna) + ' = \'' + valor + '\''; };
  const ATRASADO = 'CASE WHEN pedido_entrega_atrasado THEN 1 ELSE 0 END';
  const TRANSPORTE = 'datediff(data_entrega_cliente, data_entrega_transportadora)';
  const ANDAMENTO = 'status_pedido IN (' + LOGISTICA_STATUS_ANDAMENTO.map(function (s) { return '\'' + s + '\''; }).join(', ') + ')';
  const colunasGrupo = 'COUNT(*) AS pedidos, SUM(' + ATRASADO + ') AS atrasados, AVG(dias_entrega) AS total, AVG(' + TRANSPORTE + ') AS transporte, ' +
    'AVG(dias_previsao_entrega) AS prometido, AVG(valor_total_frete) AS frete ';

  // ---------- Série de 12 meses: KPIs, evolução, cancelados e indisponíveis ----------
  const serieSql = function (filtro) { return (
    'SELECT ' + fmtMes('data_hora_pedido') + ' AS mes, COUNT(*) AS pedidos, ' +
    '  SUM(CASE WHEN pedido_entregue THEN 1 ELSE 0 END) AS entregues, ' +
    '  SUM(CASE WHEN pedido_entregue AND NOT pedido_entrega_atrasado THEN 1 ELSE 0 END) AS no_prazo, ' +
    '  SUM(CASE WHEN status_pedido = \'Cancelado\' THEN 1 ELSE 0 END) AS cancelados, ' +
    '  SUM(CASE WHEN status_pedido LIKE \'Indispon%\' THEN 1 ELSE 0 END) AS indisponiveis, ' +
    '  SUM(CASE WHEN ' + ANDAMENTO + ' THEN 1 ELSE 0 END) AS em_andamento, ' +
    '  percentile(CASE WHEN pedido_entregue THEN dias_entrega END, 0.5) AS mediana, ' +
    '  percentile(CASE WHEN pedido_entregue THEN dias_entrega END, 0.9) AS p90, ' +
    '  AVG(valor_total_frete) AS frete ' +
    'FROM ' + TABLES.PEDIDOS + ' WHERE ' + filtro + ' GROUP BY 1 ORDER BY 1'
  ); };
  const serieRaw = queryDatabricks(serieSql(intervalo('data_hora_pedido')));
  const serieAntRaw = CORTE_MES_ABERTO_ ? queryDatabricks(serieSql(condAnteriorParcial_('data_hora_pedido'))) : [];
  const paraJanela = function (r) {
    const entregues = parseInt(r.entregues, 10) || 0;
    const noPrazo = parseInt(r.no_prazo, 10) || 0;
    return {
      mes: r.mes,
      pedidos: parseInt(r.pedidos, 10) || 0,
      entregues: entregues,
      noPrazo: noPrazo,
      pctNoPrazo: entregues > 0 ? noPrazo / entregues : 0,
      tempoMediano: parseFloat(r.mediana) || 0,
      tempoP90: parseFloat(r.p90) || 0,
      freteMedio: parseFloat(r.frete) || 0,
      emAndamento: parseInt(r.em_andamento, 10) || 0,
      cancelados: parseInt(r.cancelados, 10) || 0,
      indisponiveis: parseInt(r.indisponiveis, 10) || 0
    };
  };
  const janela = serieRaw.map(paraJanela);
  const porMes = {};
  janela.forEach(function (r) { porMes[r.mes] = r; });
  const atual = porMes[valor] || {};
  const ant = CORTE_MES_ABERTO_ ? (serieAntRaw.map(paraJanela)[0] || {}) : (porMes[anterior] || {});
  const kpis = {};
  ['entregues', 'pctNoPrazo', 'tempoMediano', 'tempoP90', 'freteMedio', 'emAndamento'].forEach(function (campo) {
    kpis[campo] = { valor: atual[campo] || 0, valorAnterior: ant[campo] === undefined ? null : ant[campo], delta: calcularDelta_(atual[campo] || 0, ant[campo]) };
  });

  // ---------- Bloco 2: desvio (real − estimada) e folga por região ----------
  const desvioRaw = queryDatabricks(
    'SELECT CASE WHEN d < -15 THEN 0 WHEN d < -7 THEN 1 WHEN d < 0 THEN 2 WHEN d = 0 THEN 3 WHEN d <= 3 THEN 4 WHEN d <= 7 THEN 5 ELSE 6 END AS faixa, COUNT(*) AS pedidos FROM (' +
    '  SELECT datediff(data_entrega_cliente, data_estimada_entrega) AS d FROM ' + TABLES.PEDIDOS +
    '  WHERE ' + doMes('data_hora_pedido') + ' AND pedido_entregue AND data_entrega_cliente IS NOT NULL AND data_estimada_entrega IS NOT NULL) GROUP BY 1'
  );
  const desvioCont = LOGISTICA_FAIXAS_DESVIO.map(function () { return 0; });
  desvioRaw.forEach(function (r) { const i = parseInt(r.faixa, 10); if (desvioCont[i] !== undefined) desvioCont[i] = parseInt(r.pedidos, 10) || 0; });
  const totalDesvio = desvioCont.reduce(function (s, n) { return s + n; }, 0);
  const estadoRaw = queryDatabricks(
    'SELECT estado_cliente AS nome, ' + colunasGrupo + 'FROM ' + TABLES.PEDIDOS +
    ' WHERE ' + doMes('data_hora_pedido') + ' AND pedido_entregue AND estado_cliente IS NOT NULL GROUP BY 1'
  );
  const estados = estadoRaw.map(function (r) { return grupoLogistica_(r); }).sort(function (a, b) { return b.pedidos - a.pedidos; });
  const regioes = agruparLogisticaPorRegiao_(estados);
  const prazo = {
    faixas: LOGISTICA_FAIXAS_DESVIO.map(function (nome, i) { return { nome: nome, pedidos: desvioCont[i], pct: totalDesvio > 0 ? desvioCont[i] / totalDesvio : 0 }; }),
    pedidos: totalDesvio,
    folgaRegiao: regioes.map(function (r) { return { nome: r.nome, pedidos: r.pedidos, prometido: r.prometido, realizado: r.total }; })
  };

  // ---------- Bloco 3: gargalo — tempo por etapa (no prazo × atrasados) e distribuição do tempo total ----------
  const etapasRaw = queryDatabricks(
    'SELECT pedido_entrega_atrasado AS atrasado, COUNT(*) AS pedidos, ' +
    '  AVG(datediff(data_aprovacao_pedido, data_pedido)) AS aprovacao, ' +
    '  AVG(datediff(data_entrega_transportadora, data_aprovacao_pedido)) AS processamento, ' +
    '  AVG(' + TRANSPORTE + ') AS transporte, AVG(dias_entrega) AS total, AVG(dias_previsao_entrega) AS prometido ' +
    'FROM ' + TABLES.PEDIDOS + ' WHERE ' + doMes('data_hora_pedido') + ' AND pedido_entregue GROUP BY 1'
  );
  const etapaDe = function (r) {
    return { pedidos: parseInt(r.pedidos, 10) || 0, aprovacao: parseFloat(r.aprovacao) || 0, processamento: parseFloat(r.processamento) || 0,
      transporte: parseFloat(r.transporte) || 0, total: parseFloat(r.total) || 0, prometido: parseFloat(r.prometido) || 0 };
  };
  const vazio = { pedidos: 0, aprovacao: 0, processamento: 0, transporte: 0, total: 0, prometido: 0 };
  let etapasNoPrazo = vazio, etapasAtrasados = vazio;
  etapasRaw.forEach(function (r) {
    // a coluna booleana volta como texto ("true"/"false")
    if (String(r.atrasado) === 'true') etapasAtrasados = etapaDe(r); else etapasNoPrazo = etapaDe(r);
  });
  const tempoRaw = queryDatabricks(
    'SELECT CASE WHEN dias_entrega <= 7 THEN 0 WHEN dias_entrega <= 14 THEN 1 WHEN dias_entrega <= 21 THEN 2 WHEN dias_entrega <= 30 THEN 3 ELSE 4 END AS faixa, COUNT(*) AS pedidos ' +
    'FROM ' + TABLES.PEDIDOS + ' WHERE ' + doMes('data_hora_pedido') + ' AND pedido_entregue AND dias_entrega IS NOT NULL GROUP BY 1'
  );
  const tempoCont = LOGISTICA_FAIXAS_TEMPO.map(function () { return 0; });
  tempoRaw.forEach(function (r) { const i = parseInt(r.faixa, 10); if (tempoCont[i] !== undefined) tempoCont[i] = parseInt(r.pedidos, 10) || 0; });
  const totalTempo = tempoCont.reduce(function (s, n) { return s + n; }, 0);
  const gargalo = {
    etapas: { noPrazo: etapasNoPrazo, atrasados: etapasAtrasados },
    distribuicao: LOGISTICA_FAIXAS_TEMPO.map(function (nome, i) { return { nome: nome, pedidos: tempoCont[i], pct: totalTempo > 0 ? tempoCont[i] / totalTempo : 0 }; }),
    mediana: atual.tempoMediano || 0,
    p90: atual.tempoP90 || 0
  };

  // ---------- Bloco 4: mercados (estado, cidade, região, categoria, valor, itens) e rotas ----------
  const cidadeRaw = queryDatabricks(
    'SELECT concat(cidade_cliente, \' (\', estado_cliente, \')\') AS nome, ' + colunasGrupo + 'FROM ' + TABLES.PEDIDOS +
    ' WHERE ' + doMes('data_hora_pedido') + ' AND pedido_entregue AND cidade_cliente IS NOT NULL AND estado_cliente IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT ' + LOGISTICA_CIDADES_MAX
  );
  const categoriaRaw = queryDatabricks(
    'SELECT ip.categoria_produto AS nome, COUNT(DISTINCT p.id_pedido) AS pedidos, ' +
    '  COUNT(DISTINCT CASE WHEN p.pedido_entrega_atrasado THEN p.id_pedido END) AS atrasados, ' +
    '  AVG(p.dias_entrega) AS total, AVG(datediff(p.data_entrega_cliente, p.data_entrega_transportadora)) AS transporte, ' +
    '  AVG(p.dias_previsao_entrega) AS prometido, AVG(p.valor_total_frete) AS frete ' +
    'FROM ' + TABLES.ITENS_PEDIDOS + ' ip JOIN ' + TABLES.PEDIDOS + ' p ON p.id_pedido = ip.id_pedido ' +
    'WHERE ' + doMes('p.data_hora_pedido') + ' AND p.pedido_entregue AND ip.categoria_produto IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT ' + LOGISTICA_CATEGORIAS_MAX
  );
  const perfilPor = function (expr, nomeDe) {
    return queryDatabricks(
      'SELECT ' + expr + ' AS dim, ' + colunasGrupo + 'FROM ' + TABLES.PEDIDOS +
      ' WHERE ' + doMes('data_hora_pedido') + ' AND pedido_entregue GROUP BY 1 ORDER BY 1'
    ).map(function (r) { return grupoLogistica_(r, nomeDe(r.dim)); });
  };
  const mercados = {
    estado: estados,
    cidade: cidadeRaw.map(function (r) { return grupoLogistica_(r); }),
    regiao: regioes,
    categoria: categoriaRaw.map(function (r) { return grupoLogistica_(r); }),
    valor: perfilPor(
      'CASE WHEN valor_total_produtos < 50 THEN 0 WHEN valor_total_produtos < 100 THEN 1 WHEN valor_total_produtos < 200 THEN 2 WHEN valor_total_produtos < 500 THEN 3 ELSE 4 END',
      function (d) { return VENDAS_FAIXAS_TICKET_PEDIDO[parseInt(d, 10)] || '—'; }
    ),
    itens: perfilPor(
      'CASE WHEN qtd_itens <= 1 THEN 0 WHEN qtd_itens = 2 THEN 1 WHEN qtd_itens = 3 THEN 2 ELSE 3 END',
      function (d) { return LOGISTICA_FAIXAS_ITENS[parseInt(d, 10)] || '—'; }
    ),
    minPedidos: LOGISTICA_MIN_PEDIDOS
  };
  // Rotas: estado do vendedor (origem) → estado do cliente (destino). Um pedido com itens de vendedores de estados diferentes conta em cada rota.
  const rotasRaw = queryDatabricks(
    'SELECT ip.estado_vendedor AS origem, p.estado_cliente AS destino, COUNT(DISTINCT p.id_pedido) AS pedidos, ' +
    '  COUNT(DISTINCT CASE WHEN p.pedido_entrega_atrasado THEN p.id_pedido END) AS atrasados, AVG(p.dias_entrega) AS total ' +
    'FROM ' + TABLES.ITENS_PEDIDOS + ' ip JOIN ' + TABLES.PEDIDOS + ' p ON p.id_pedido = ip.id_pedido ' +
    'WHERE ' + doMes('p.data_hora_pedido') + ' AND p.pedido_entregue AND ip.estado_vendedor IS NOT NULL AND p.estado_cliente IS NOT NULL GROUP BY 1, 2'
  );
  const rotasTodas = rotasRaw.map(function (r) {
    return { origem: r.origem, destino: r.destino, pedidos: parseInt(r.pedidos, 10) || 0, atrasados: parseInt(r.atrasados, 10) || 0, total: parseFloat(r.total) || 0 };
  });
  const somaPor = function (campo) {
    const m = {};
    rotasTodas.forEach(function (r) { m[r[campo]] = (m[r[campo]] || 0) + r.pedidos; });
    return Object.keys(m).sort(function (a, b) { return m[b] - m[a]; });
  };
  const origens = somaPor('origem').slice(0, LOGISTICA_ROTAS_ORIGENS);
  const destinos = somaPor('destino').slice(0, LOGISTICA_ROTAS_DESTINOS);
  const agregado = { mesmo: { pedidos: 0, atrasados: 0, soma: 0 }, entre: { pedidos: 0, atrasados: 0, soma: 0 } };
  rotasTodas.forEach(function (r) {
    const a = r.origem === r.destino ? agregado.mesmo : agregado.entre;
    a.pedidos += r.pedidos; a.atrasados += r.atrasados; a.soma += r.total * r.pedidos;
  });
  const fechar = function (a) { return { pedidos: a.pedidos, atrasados: a.atrasados, total: a.pedidos > 0 ? a.soma / a.pedidos : 0 }; };
  const rotas = {
    origens: origens,
    destinos: destinos,
    celulas: rotasTodas.filter(function (r) { return origens.indexOf(r.origem) >= 0 && destinos.indexOf(r.destino) >= 0; }),
    mesmoEstado: fechar(agregado.mesmo),
    entreEstados: fechar(agregado.entre),
    minPedidos: LOGISTICA_MIN_PEDIDOS
  };

  // ---------- Bloco 5: custo (frete × prazo por região) e impacto na nota por nível de cumprimento (12 meses) ----------
  const custo = regioes.map(function (r) { return { nome: r.nome, pedidos: r.pedidos, frete: r.frete, total: r.total, transporte: r.transporte }; });
  const notasRaw = queryDatabricks(
    'SELECT CASE WHEN NOT p.pedido_entrega_atrasado THEN 0 WHEN p.dias_atraso <= 7 THEN 1 WHEN p.dias_atraso <= 15 THEN 2 ELSE 3 END AS nivel, ' +
    '  COUNT(*) AS avaliacoes, AVG(a.nota_avaliacao) AS nota, SUM(CASE WHEN a.nota_avaliacao <= 2 THEN 1 ELSE 0 END) / COUNT(*) AS baixas ' +
    'FROM ' + TABLES.AVALIACOES + ' a JOIN ' + TABLES.PEDIDOS + ' p ON p.id_pedido = a.id_pedido ' +
    'WHERE ' + intervalo('p.data_hora_pedido') + ' AND p.pedido_entregue GROUP BY 1 ORDER BY 1'
  );
  const impacto = LOGISTICA_NIVEIS_CUMPRIMENTO.map(function (nome, i) {
    const r = notasRaw.filter(function (x) { return parseInt(x.nivel, 10) === i; })[0];
    return { nome: nome, avaliacoes: r ? parseInt(r.avaliacoes, 10) || 0 : 0, nota: r ? parseFloat(r.nota) || 0 : 0, baixas: r ? parseFloat(r.baixas) || 0 : 0 };
  });

  // ---------- Bloco 6: pedidos parados agora (por idade e status) — foto da base, não do mês ----------
  const andamentoRaw = queryDatabricks(
    'SELECT status_pedido AS status, CASE WHEN idade <= 7 THEN 0 WHEN idade <= 15 THEN 1 WHEN idade <= 30 THEN 2 ELSE 3 END AS faixa, COUNT(*) AS pedidos FROM (' +
    '  SELECT status_pedido, datediff((SELECT MAX(data_pedido) FROM ' + TABLES.PEDIDOS + '), data_pedido) AS idade FROM ' + TABLES.PEDIDOS + ' WHERE ' + ANDAMENTO +
    ') GROUP BY 1, 2'
  );
  const andamento = {
    faixas: LOGISTICA_FAIXAS_IDADE,
    status: LOGISTICA_STATUS_ANDAMENTO,
    celulas: andamentoRaw.map(function (r) { return { status: r.status, faixa: parseInt(r.faixa, 10), pedidos: parseInt(r.pedidos, 10) || 0 }; })
  };

  const data = {
    tipo: 'mes',
    periodo: valor,
    periodoAnterior: anterior,
    comparacao: comparacaoInfo_(),
    kpis: kpis,
    janela: janela,
    prazo: prazo,
    gargalo: gargalo,
    mercados: mercados,
    rotas: rotas,
    custo: custo,
    impacto: impacto,
    andamento: andamento,
    atualizadoEm: new Date().toISOString()
  };
  let json = JSON.stringify(data);
  if (!CONSULTAS_COLETA_) cache.put(cacheKey, json, AGGREGATE_CACHE_SECONDS);
  return data;
}

// ===== Visão Satisfação =====
// Pergunta central: o cliente percebe o nosso negócio como nós achamos que ele percebe?
// Definições: positiva = nota 4 ou 5; insatisfeita = nota 1, 2 ou 3 (neutra + negativa); mês de referência = mês do PEDIDO.
// Meta: mais de 95% de avaliações positivas (SATISFACAO_META_POSITIVAS, também usada no front-end).
// Cliente insatisfeito = cliente com ao menos uma avaliação insatisfeita no mês. Uma única chamada (getSatisfacaoData) devolve tudo.

const SATISFACAO_META_POSITIVAS = 0.95;
// Mínimo de avaliações para um segmento (estado, cidade, categoria…) aparecer nos gráficos.
const SATISFACAO_MIN_AVALIACOES = 30;
const SATISFACAO_CIDADES_MAX = 80;
const SATISFACAO_CATEGORIAS_MAX = 25;
const SATISFACAO_FAIXAS_ITENS = ['1 item', '2 itens', '3 itens', '4 ou mais itens'];
const SATISFACAO_NIVEIS_PRAZO = ['Entregue no prazo', 'Atraso de 1 a 7 dias', 'Atraso de 8 a 15 dias', 'Atraso de 16 dias ou mais'];
// Temas dos comentários das avaliações insatisfeitas: classificação simples por PALAVRA-CHAVE (sem acento, minúsculas).
// Um comentário pode ter mais de um tema. Só cobre quem escreveu comentário.
const SATISFACAO_TEMAS = [
  { nome: 'Atraso ou demora na entrega', regex: 'atras|demor|prazo|demora' },
  { nome: 'Não recebeu o produto', regex: 'nao recebi|nao chegou|nunca chegou|nao foi entregue|nao entregaram|ainda nao recebi' },
  { nome: 'Produto diferente do anunciado', regex: 'diferente|errado|errada|outro produto|nao corresponde|nao e o ' },
  { nome: 'Defeito ou produto danificado', regex: 'defeit|quebrad|danific|nao funciona|estragad|avariad|rachad|amassad' },
  { nome: 'Qualidade abaixo do esperado', regex: 'qualidade|pessim|horrivel|decepcion|fragil|fraco|ruim' },
  { nome: 'Pedido incompleto', regex: 'incompleto|faltou|falta |so veio|apenas um|veio so' },
  { nome: 'Vendedor ou atendimento', regex: 'vendedor|loja|atendimento|resposta|responder|cancel|reembolso|devolu|estorno' },
  { nome: 'Embalagem', regex: 'embalag|embalado' },
  { nome: 'Preço ou frete', regex: 'frete|preco|caro' }
];

function getSatisfacaoData(mes, forceRefresh) {
  return montarSatisfacao_(mes || getMesesDisponiveis()[0], forceRefresh);
}

function grupoSatisfacao_(r, nome) {
  return {
    nome: nome !== undefined ? nome : r.nome,
    avaliacoes: parseInt(r.avaliacoes, 10) || 0,
    insatisfeitas: parseInt(r.insatisfeitas, 10) || 0,
    positivas: parseInt(r.positivas, 10) || 0,
    nota: parseFloat(r.nota) || 0
  };
}

function agruparSatisfacaoPorRegiao_(estados) {
  const mapa = {};
  estados.forEach(function (e) {
    const nome = REGIAO_POR_UF[e.nome] || 'Outros';
    const r = mapa[nome] || (mapa[nome] = { nome: nome, avaliacoes: 0, insatisfeitas: 0, positivas: 0, somaNota: 0 });
    r.avaliacoes += e.avaliacoes;
    r.insatisfeitas += e.insatisfeitas;
    r.positivas += e.positivas;
    r.somaNota += e.nota * e.avaliacoes;
  });
  return Object.keys(mapa).map(function (k) {
    const r = mapa[k];
    return { nome: r.nome, avaliacoes: r.avaliacoes, insatisfeitas: r.insatisfeitas, positivas: r.positivas, nota: r.avaliacoes > 0 ? r.somaNota / r.avaliacoes : 0 };
  }).sort(function (a, b) { return b.avaliacoes - a.avaliacoes; });
}

function montarSatisfacao_(valor, forceRefresh) {
  return comCorte_(valor, function () {
    return comConsultasEmParalelo_('satisfacao ' + valor, function () { return montarSatisfacaoInterno_(valor, forceRefresh); });
  });
}

function montarSatisfacaoInterno_(valor, forceRefresh) {
  const cache = CacheService.getScriptCache();
  const cacheKey = 'satisfacao_v3_' + valor + sufixoCorte_();
  if (!forceRefresh) {
    const cached = cache.get(cacheKey);
    if (cached) return JSON.parse(cached);
  }

  const anterior = mesAnterior_(valor);
  const mesInicioJanela = mesesAntes_(valor, JANELA_MESES - 1);
  const fmtMes = function (coluna) { return 'date_format(' + coluna + ', \'yyyy-MM\')'; };
  const intervalo = function (coluna) { return fmtMes(coluna) + ' BETWEEN \'' + mesInicioJanela + '\' AND \'' + valor + '\''; };
  const doMes = function (coluna) { return fmtMes(coluna) + ' = \'' + valor + '\''; };
  // mês aberto: o mês anterior entra só até o mesmo dia
  const DOIS_MESES = CORTE_MES_ABERTO_
    ? '(' + doMes('p.data_hora_pedido') + ' OR (' + condAnteriorParcial_('p.data_hora_pedido') + '))'
    : fmtMes('p.data_hora_pedido') + ' IN (\'' + valor + '\', \'' + anterior + '\')';
  const AVALIACOES_PEDIDOS = 'FROM ' + TABLES.AVALIACOES + ' a JOIN ' + TABLES.PEDIDOS + ' p ON p.id_pedido = a.id_pedido ';
  const INSATISFEITA = 'CASE WHEN a.nota_avaliacao <= 3 THEN 1 ELSE 0 END';
  const colunasGrupo = 'COUNT(*) AS avaliacoes, SUM(' + INSATISFEITA + ') AS insatisfeitas, SUM(CASE WHEN a.nota_avaliacao >= 4 THEN 1 ELSE 0 END) AS positivas, AVG(a.nota_avaliacao) AS nota ';

  // ---------- KPIs e evolução: série de 12 meses (mês do pedido) ----------
  const serieSql = function (filtro) { return (
    'SELECT ' + fmtMes('p.data_hora_pedido') + ' AS mes, COUNT(*) AS avaliacoes, AVG(a.nota_avaliacao) AS nota, ' +
    '  SUM(CASE WHEN a.nota_avaliacao >= 4 THEN 1 ELSE 0 END) AS positivas, ' +
    '  SUM(CASE WHEN a.nota_avaliacao = 3 THEN 1 ELSE 0 END) AS neutras, ' +
    '  SUM(CASE WHEN a.nota_avaliacao <= 2 THEN 1 ELSE 0 END) AS negativas, ' +
    '  COUNT(DISTINCT p.id_cliente_unico) AS avaliadores, ' +
    '  COUNT(DISTINCT CASE WHEN a.nota_avaliacao <= 3 THEN p.id_cliente_unico END) AS clientes_insatisfeitos, ' +
    '  COUNT(DISTINCT CASE WHEN a.nota_avaliacao <= 3 THEN p.id_pedido END) AS pedidos_insatisfeitos, ' +
    '  SUM(CASE WHEN a.nota_avaliacao <= 3 THEN p.valor_total_produtos ELSE 0 END) AS faturamento_insatisfeitos, ' +
    '  SUM(CASE WHEN a.comentario_avaliacao IS NOT NULL AND length(trim(a.comentario_avaliacao)) > 0 THEN 1 ELSE 0 END) AS com_comentario ' +
    AVALIACOES_PEDIDOS + 'WHERE ' + filtro + ' GROUP BY 1 ORDER BY 1'
  ); };
  const serieRaw = queryDatabricks(serieSql(intervalo('p.data_hora_pedido')));
  const serieAntRaw = CORTE_MES_ABERTO_ ? queryDatabricks(serieSql(condAnteriorParcial_('p.data_hora_pedido'))) : [];
  const paraJanela = function (r) {
    const avaliacoes = parseInt(r.avaliacoes, 10) || 0;
    const positivas = parseInt(r.positivas, 10) || 0;
    const neutras = parseInt(r.neutras, 10) || 0;
    const negativas = parseInt(r.negativas, 10) || 0;
    return {
      mes: r.mes,
      avaliacoes: avaliacoes,
      notaMedia: parseFloat(r.nota) || 0,
      pctPositivas: avaliacoes > 0 ? positivas / avaliacoes : 0,
      pctNeutras: avaliacoes > 0 ? neutras / avaliacoes : 0,
      pctNegativas: avaliacoes > 0 ? negativas / avaliacoes : 0,
      pctInsatisfeitas: avaliacoes > 0 ? (neutras + negativas) / avaliacoes : 0,
      avaliadores: parseInt(r.avaliadores, 10) || 0,
      clientesInsatisfeitos: parseInt(r.clientes_insatisfeitos, 10) || 0,
      pedidosInsatisfeitos: parseInt(r.pedidos_insatisfeitos, 10) || 0,
      faturamentoInsatisfeitos: parseFloat(r.faturamento_insatisfeitos) || 0,
      pctComentario: avaliacoes > 0 ? (parseInt(r.com_comentario, 10) || 0) / avaliacoes : 0
    };
  };
  const janela = serieRaw.map(paraJanela);
  const porMes = {};
  janela.forEach(function (r) { porMes[r.mes] = r; });
  const atual = porMes[valor] || {};
  const ant = CORTE_MES_ABERTO_ ? (serieAntRaw.map(paraJanela)[0] || {}) : (porMes[anterior] || {});
  const kpis = {};
  ['notaMedia', 'pctPositivas', 'pctInsatisfeitas', 'clientesInsatisfeitos', 'avaliadores', 'pctComentario'].forEach(function (campo) {
    kpis[campo] = { valor: atual[campo] || 0, valorAnterior: ant[campo] === undefined ? null : ant[campo], delta: calcularDelta_(atual[campo] || 0, ant[campo]) };
  });

  // ---------- Distribuição das notas (mês e mês anterior) ----------
  const distRaw = queryDatabricks(
    'SELECT ' + fmtMes('p.data_hora_pedido') + ' AS mes, a.nota_avaliacao AS nota, COUNT(*) AS avaliacoes ' +
    AVALIACOES_PEDIDOS + 'WHERE ' + DOIS_MESES + ' GROUP BY 1, 2'
  );
  const contar = function (mes) {
    const v = [0, 0, 0, 0, 0];
    distRaw.forEach(function (r) { const n = parseInt(r.nota, 10); if (r.mes === mes && n >= 1 && n <= 5) v[n - 1] += parseInt(r.avaliacoes, 10) || 0; });
    return v;
  };
  const distribuicao = { mes: contar(valor), anterior: contar(anterior) };

  // ---------- Onde está a insatisfação (mês): estado, cidade, região, categoria, faixa de valor, itens ----------
  const estadoRaw = queryDatabricks(
    'SELECT p.estado_cliente AS nome, ' + colunasGrupo + AVALIACOES_PEDIDOS + 'WHERE ' + doMes('p.data_hora_pedido') + ' AND p.estado_cliente IS NOT NULL GROUP BY 1'
  );
  const estados = estadoRaw.map(function (r) { return grupoSatisfacao_(r); }).sort(function (a, b) { return b.avaliacoes - a.avaliacoes; });
  const cidadeRaw = queryDatabricks(
    'SELECT concat(p.cidade_cliente, \' (\', p.estado_cliente, \')\') AS nome, ' + colunasGrupo + AVALIACOES_PEDIDOS +
    'WHERE ' + doMes('p.data_hora_pedido') + ' AND p.cidade_cliente IS NOT NULL AND p.estado_cliente IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT ' + SATISFACAO_CIDADES_MAX
  );
  const categoriaRaw = queryDatabricks(
    'SELECT ip.categoria_produto AS nome, COUNT(DISTINCT a.id_avaliacao) AS avaliacoes, ' +
    '  COUNT(DISTINCT CASE WHEN a.nota_avaliacao <= 3 THEN a.id_avaliacao END) AS insatisfeitas, ' +
    '  COUNT(DISTINCT CASE WHEN a.nota_avaliacao >= 4 THEN a.id_avaliacao END) AS positivas, AVG(a.nota_avaliacao) AS nota ' +
    'FROM ' + TABLES.AVALIACOES + ' a JOIN ' + TABLES.PEDIDOS + ' p ON p.id_pedido = a.id_pedido JOIN ' + TABLES.ITENS_PEDIDOS + ' ip ON ip.id_pedido = p.id_pedido ' +
    'WHERE ' + doMes('p.data_hora_pedido') + ' AND ip.categoria_produto IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT ' + SATISFACAO_CATEGORIAS_MAX
  );
  const perfilPor = function (expr, nomeDe) {
    return queryDatabricks(
      'SELECT ' + expr + ' AS dim, ' + colunasGrupo + AVALIACOES_PEDIDOS + 'WHERE ' + doMes('p.data_hora_pedido') + ' GROUP BY 1 ORDER BY 1'
    ).map(function (r) { return grupoSatisfacao_(r, nomeDe(r.dim)); });
  };
  const onde = {
    estado: estados,
    cidade: cidadeRaw.map(function (r) { return grupoSatisfacao_(r); }),
    regiao: agruparSatisfacaoPorRegiao_(estados),
    categoria: categoriaRaw.map(function (r) { return grupoSatisfacao_(r); }),
    valor: perfilPor(
      'CASE WHEN p.valor_total_produtos < 50 THEN 0 WHEN p.valor_total_produtos < 100 THEN 1 WHEN p.valor_total_produtos < 200 THEN 2 WHEN p.valor_total_produtos < 500 THEN 3 ELSE 4 END',
      function (d) { return VENDAS_FAIXAS_TICKET_PEDIDO[parseInt(d, 10)] || '—'; }
    ),
    itens: perfilPor(
      'CASE WHEN p.qtd_itens <= 1 THEN 0 WHEN p.qtd_itens = 2 THEN 1 WHEN p.qtd_itens = 3 THEN 2 ELSE 3 END',
      function (d) { return SATISFACAO_FAIXAS_ITENS[parseInt(d, 10)] || '—'; }
    ),
    minAvaliacoes: SATISFACAO_MIN_AVALIACOES
  };

  // ---------- Experiência: prazo × avaliações insatisfeitas (mês, pedidos entregues) ----------
  const prazoRaw = queryDatabricks(
    'SELECT CASE WHEN NOT p.pedido_entrega_atrasado THEN 0 WHEN p.dias_atraso <= 7 THEN 1 WHEN p.dias_atraso <= 15 THEN 2 ELSE 3 END AS nivel, ' + colunasGrupo +
    AVALIACOES_PEDIDOS + 'WHERE ' + doMes('p.data_hora_pedido') + ' AND p.pedido_entregue GROUP BY 1 ORDER BY 1'
  );
  const prazo = SATISFACAO_NIVEIS_PRAZO.map(function (nome, i) {
    const r = prazoRaw.filter(function (x) { return parseInt(x.nivel, 10) === i; })[0];
    return r ? grupoSatisfacao_(r, nome) : { nome: nome, avaliacoes: 0, insatisfeitas: 0, positivas: 0, nota: 0 };
  });

  // ---------- Experiência: temas dos comentários das avaliações insatisfeitas (mês) ----------
  const NORMALIZADO = 'translate(lower(a.comentario_avaliacao), \'áàâãäéèêëíìîïóòôõöúùûüç\', \'aaaaaeeeeiiiiooooouuuuc\')';
  const testes = SATISFACAO_TEMAS.map(function (t) { return NORMALIZADO + ' RLIKE \'' + t.regex + '\''; });
  const temasRaw = queryDatabricks(
    'SELECT COUNT(*) AS comentarios, ' +
    testes.map(function (t, i) { return 'SUM(CASE WHEN ' + t + ' THEN 1 ELSE 0 END) AS t' + i; }).join(', ') + ', ' +
    'SUM(CASE WHEN NOT (' + testes.join(' OR ') + ') THEN 1 ELSE 0 END) AS sem_tema ' +
    AVALIACOES_PEDIDOS + 'WHERE ' + doMes('p.data_hora_pedido') + ' AND a.nota_avaliacao <= 3 AND a.comentario_avaliacao IS NOT NULL AND length(trim(a.comentario_avaliacao)) > 0'
  );
  const t0 = temasRaw[0] || {};
  const comentarios = parseInt(t0.comentarios, 10) || 0;
  const temas = {
    comentarios: comentarios,
    itens: SATISFACAO_TEMAS.map(function (t, i) { return { nome: t.nome, comentarios: parseInt(t0['t' + i], 10) || 0 }; })
      .concat([{ nome: 'Sem tema identificado', comentarios: parseInt(t0.sem_tema, 10) || 0 }])
  };

  // ---------- Impacto: clientes insatisfeitos voltam a comprar? (12 meses; comparação entre os dois grupos) ----------
  const retencaoRaw = queryDatabricks(
    'SELECT CASE WHEN a.nota_avaliacao <= 3 THEN 1 ELSE 0 END AS insatisfeito, COUNT(DISTINCT p.id_cliente_unico) AS clientes, ' +
    '  COUNT(DISTINCT CASE WHEN u.ultimo > p.data_hora_pedido THEN p.id_cliente_unico END) AS voltaram ' +
    AVALIACOES_PEDIDOS +
    'JOIN (SELECT id_cliente_unico, MAX(data_hora_pedido) AS ultimo FROM ' + TABLES.PEDIDOS + ' WHERE ' + CONDICAO_FATURAMENTO + ' GROUP BY 1) u ON u.id_cliente_unico = p.id_cliente_unico ' +
    'WHERE ' + intervalo('p.data_hora_pedido') + ' AND p.' + CONDICAO_FATURAMENTO + ' GROUP BY 1'
  );
  const grupoRet = function (flag) {
    const r = retencaoRaw.filter(function (x) { return parseInt(x.insatisfeito, 10) === flag; })[0];
    const clientes = r ? parseInt(r.clientes, 10) || 0 : 0;
    const voltaram = r ? parseInt(r.voltaram, 10) || 0 : 0;
    return { clientes: clientes, voltaram: voltaram, pct: clientes > 0 ? voltaram / clientes : 0 };
  };
  const retencao = { insatisfeitos: grupoRet(1), satisfeitos: grupoRet(0) };

  const data = {
    tipo: 'mes',
    periodo: valor,
    periodoAnterior: anterior,
    comparacao: comparacaoInfo_(),
    meta: SATISFACAO_META_POSITIVAS,
    kpis: kpis,
    janela: janela,
    distribuicao: distribuicao,
    onde: onde,
    prazo: prazo,
    temas: temas,
    retencao: retencao,
    atualizadoEm: new Date().toISOString()
  };
  if (!CONSULTAS_COLETA_) cache.put(cacheKey, JSON.stringify(data), AGGREGATE_CACHE_SECONDS);
  return data;
}

// ===== Diagnóstico das variações (rodar no editor) =====
// Mostra, com todas as casas decimais, o valor do mês, o valor do período de comparação e a variação de cada KPI de cada
// página — para comparar com uma consulta SQL feita na mão. Rode com o mês desejado (ex.: diagnosticoVariacoes('2018-08'));
// sem argumento usa o mês mais recente do menu. Ignora o cache (recalcula tudo do Databricks).
function diagnosticoVariacoes(mes) {
  const alvo = mes || getMesesDisponiveis()[0];
  const paginas = [
    ['Visão Geral', function () { return montarOverview_('mes', alvo, true); }],
    ['Receita', function () { return montarReceita_(alvo, true); }],
    ['Vendas', function () { return montarVendas_(alvo, true); }],
    ['Logística', function () { return montarLogistica_(alvo, true); }],
    ['Satisfação', function () { return montarSatisfacao_(alvo, true); }]
  ];
  paginas.forEach(function (pg) {
    const d = pg[1]();
    const comp = d.comparacao && d.comparacao.parcial ? 'dias 1–' + d.comparacao.dia + ' de ' + d.periodoAnterior : d.periodoAnterior + ' (mês inteiro)';
    Logger.log('=== ' + pg[0] + ' — ' + alvo + ' vs. ' + comp + ' ===');
    Object.keys(d.kpis).forEach(function (campo) {
      const k = d.kpis[campo];
      Logger.log(campo + ': atual=' + k.valor + ' | anterior=' + k.valorAnterior + ' | variação=' + (k.delta === null || k.delta === undefined ? 'n/d' : k.delta) +
        (k.valorAnterior != null && Math.abs(k.valor) <= 1.5 && Math.abs(k.valorAnterior) <= 1.5 ? ' | dif. em p.p.=' + ((k.valor - k.valorAnterior) * 100) : ''));
    });
  });
}

// ===== Utilitários de inspeção (rodar manualmente no editor) =====

function testConnection() {
  Logger.log(queryDatabricks('SELECT 1 AS ok'));
}

function describeTodasAsTabelas() {
  Object.keys(TABLES).forEach(function (key) {
    Logger.log('--- ' + TABLES[key] + ' ---');
    Logger.log(queryDatabricks('DESCRIBE TABLE ' + TABLES[key]));
  });
}

// Tamanho em bytes de cada tabela (via DESCRIBE DETAIL, comando do Delta Lake) —
// útil pra confirmar que uma carga/reprocessamento de dados realmente gravou algo.
function getTamanhoTabelas() {
  let totalBytes = 0;
  Object.keys(TABLES).forEach(function (key) {
    const tabela = TABLES[key];
    const detalhe = queryDatabricks('DESCRIBE DETAIL ' + tabela)[0];
    const bytes = parseInt(detalhe.sizeInBytes, 10) || 0;
    const numFiles = parseInt(detalhe.numFiles, 10) || 0;
    totalBytes += bytes;
    Logger.log(
      tabela + ': ' + bytes.toLocaleString('pt-BR') + ' bytes (' +
      (bytes / (1024 * 1024)).toFixed(2) + ' MB), ' + numFiles + ' arquivo(s)'
    );
  });
  Logger.log('TOTAL: ' + totalBytes.toLocaleString('pt-BR') + ' bytes (' + (totalBytes / (1024 * 1024)).toFixed(2) + ' MB)');
}

const LIMITE_CACHE_BYTES = 100 * 1024; // 100KB por chave — limite do CacheService
const LIMITE_URLFETCH_BYTES = 50 * 1024 * 1024; // ~50MB por resposta — limite do UrlFetchApp

function formatarBytes_(bytes) {
  if (bytes >= 1024 * 1024 * 1024) return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
  if (bytes >= 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
  if (bytes >= 1024) return (bytes / 1024).toFixed(2) + ' KB';
  return bytes + ' bytes';
}

// Invalida todo o cache do dashboard de uma vez: troca a "geração" que compõe as chaves (as antigas expiram sozinhas).
function limparCacheDashboard_() {
  const props = PropertiesService.getScriptProperties();
  const nova = (parseInt(props.getProperty('cache_geracao') || '0', 10) || 0) + 1;
  props.setProperty('cache_geracao', String(nova));
  return 1;
}

// ===== Atualização dos dados (rodar no editor do Apps Script) =====
// Invalida o cache e já recalcula todas as páginas para o mês mais recente e o anterior (os dois do menu de período),
// deixando o cache pronto por 6h. É o que o gatilho automático roda; para forçar na hora, rode esta função no editor.
function atualizarDadosDashboard() {
  const inicio = Date.now();
  limparCacheDashboard_();
  const meses = getMesesDisponiveis();
  meses.forEach(function (mes) {
    [
      ['visão geral', function () { return montarOverview_('mes', mes, true); }],
      ['receita', function () { return montarReceita_(mes, true); }],
      ['vendas', function () { return montarVendas_(mes, true); }],
      ['logística', function () { return montarLogistica_(mes, true); }],
      ['satisfação', function () { return montarSatisfacao_(mes, true); }]
    ].forEach(function (pagina) {
      const t0 = Date.now();
      try {
        pagina[1]();
        Logger.log('[atualizar] ' + pagina[0] + ' ' + mes + ': ok em ' + (Date.now() - t0) + ' ms');
      } catch (e) {
        Logger.log('[atualizar] ' + pagina[0] + ' ' + mes + ': FALHOU — ' + e);
      }
    });
  });
  getAnosDisponiveis();
  Logger.log('[atualizar] concluído em ' + Math.round((Date.now() - inicio) / 1000) + ' s. Meses: ' + meses.join(', '));
}

// Cria (ou recria) os gatilhos que rodam atualizarDadosDashboard todo dia às 6h e às 12h. Rodar UMA vez no editor;
// o Google pede autorização para gerenciar gatilhos. O cache dura 6h: cobre das 6h às 18h; à noite, se alguém abrir,
// os dados são calculados na hora (e ficam guardados por 6h).
const HORAS_ATUALIZACAO_AUTOMATICA = [6, 12];
function configurarAtualizacaoAutomatica() {
  removerAtualizacaoAutomatica();
  HORAS_ATUALIZACAO_AUTOMATICA.forEach(function (hora) {
    ScriptApp.newTrigger('atualizarDadosDashboard').timeBased().everyDays(1).atHour(hora).create();
  });
  Logger.log('Gatilhos criados: atualizarDadosDashboard às ' + HORAS_ATUALIZACAO_AUTOMATICA.join('h e ') + 'h (fuso ' + Session.getScriptTimeZone() + ').');
}

function removerAtualizacaoAutomatica() {
  ScriptApp.getProjectTriggers().forEach(function (t) {
    if (t.getHandlerFunction() === 'atualizarDadosDashboard') ScriptApp.deleteTrigger(t);
  });
}

// Diagnóstico (não mexe no cache; para atualizar os dados use atualizarDadosDashboard): mostra o
// tamanho de cada tabela comparado aos limites do Apps Script.
//
// Importante: as queries que o dashboard usa são agregadas (SUM/COUNT/GROUP BY direto no
// Databricks), então o tamanho bruto da tabela NÃO trava o dashboard hoje — essa comparação
// é um diagnóstico pra caso a gente algum dia precise puxar linhas brutas (como getTableData faz).
function diagnosticoTamanhoTabelas() {
  Logger.log('Tamanho das tabelas vs. limites do Apps Script:');

  let totalBytes = 0;
  Object.keys(TABLES).forEach(function (key) {
    const tabela = TABLES[key];
    const detalhe = queryDatabricks('DESCRIBE DETAIL ' + tabela)[0];
    const bytes = parseInt(detalhe.sizeInBytes, 10) || 0;
    totalBytes += bytes;

    const vezesCache = bytes / LIMITE_CACHE_BYTES;
    const pctUrlFetch = (bytes / LIMITE_URLFETCH_BYTES) * 100;
    Logger.log(
      tabela + ': ' + formatarBytes_(bytes) +
      ' — ' + vezesCache.toFixed(0) + 'x o limite de 100KB/chave do CacheService, ' +
      pctUrlFetch.toFixed(1) + '% do limite de 50MB/resposta do UrlFetchApp'
    );
  });

  Logger.log('');
  const vezesCacheTotal = totalBytes / LIMITE_CACHE_BYTES;
  const pctUrlFetchTotal = (totalBytes / LIMITE_URLFETCH_BYTES) * 100;
  Logger.log(
    'TOTAL: ' + formatarBytes_(totalBytes) +
    ' — ' + vezesCacheTotal.toFixed(0) + 'x o limite de 100KB/chave, ' +
    pctUrlFetchTotal.toFixed(1) + '% do limite de 50MB/resposta'
  );
}
