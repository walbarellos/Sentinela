"""
haEmet — CLI
Uso:
  python -m src.cli ingest --ano 2024
  python -m src.cli ingest --todos
  python -m src.cli buscar --cpf 52998224725
  python -m src.cli top-insights
"""
import logging
import os
import sys

import click
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("haEmet")


def get_graph():
    from src.core.graph import GraphDB
    return GraphDB(
        uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "haEmet2025"),
    )


@click.group()
def cli():
    """haEmet — Sistema de análise anticorrupção para o Acre."""
    pass


@cli.command()
@click.option("--ano", type=int, default=None, help="Ano específico (ex: 2024)")
@click.option("--todos", is_flag=True, help="Processa todos os anos disponíveis")
@click.option("--force", is_flag=True, help="Força re-download mesmo com cache")
@click.option("--sem-neo4j", is_flag=True, help="Só normaliza, não persiste no grafo")
def ingest(ano, todos, force, sem_neo4j):
    """Baixa dados do TSE e persiste no Neo4j."""
    from src.ingest.tse_connector import TseConnector, ANOS
    from src.detection.patrimonio import detectar_variacao_patrimonial

    anos = ANOS if todos else ([ano] if ano else [2024])
    log.info("Iniciando ingestão TSE — anos: %s", anos)

    connector = TseConnector(
        data_dir=os.getenv("DATA_DIR", "./data/tse"),
        force=force,
    )
    pessoas = connector.run(anos=anos)

    # Detecta padrões
    insights = detectar_variacao_patrimonial(pessoas)
    log.info("%d insights de variação patrimonial detectados", len(insights))

    if sem_neo4j:
        # Imprime resumo no terminal
        click.echo(f"\n{'='*60}")
        click.echo(f"PESSOAS ÚNICAS NO ACRE: {len(pessoas)}")
        click.echo(f"INSIGHTS GERADOS:       {len(insights)}")
        click.echo(f"\nTOP 10 POR SCORE:")
        for i in insights[:10]:
            p = pessoas.get(i.cpf_sujeito)
            nome = p.nome_canonico if p else i.cpf_sujeito
            click.echo(f"  [{i.severidade.value:8}] {i.score:.2f} — {nome}")
            click.echo(f"           {i.descricao[:80]}...")
        return

    # Persiste no Neo4j
    db = get_graph()
    db.create_schema()

    with click.progressbar(pessoas.values(), label="Persistindo pessoas") as bar:
        for pessoa in bar:
            try:
                db.upsert_pessoa(pessoa)
            except Exception as e:
                log.error("Erro ao persistir %s: %s", pessoa.cpf, e)

    with click.progressbar(insights, label="Persistindo insights") as bar:
        for insight in bar:
            try:
                db.upsert_insight(insight)
            except Exception as e:
                log.error("Erro ao persistir insight %s: %s", insight.id, e)

    db.close()
    click.echo(f"\n✅ {len(pessoas)} pessoas e {len(insights)} insights no Neo4j.")


@cli.command()
@click.option("--cpf", required=True, help="CPF do agente público (com ou sem máscara)")
def buscar(cpf):
    """Busca subgrafo completo de uma pessoa por CPF."""
    import json
    from src.core.normalizer import normalize_cpf

    cpf_norm = normalize_cpf(cpf)
    if not cpf_norm:
        click.echo("❌ CPF inválido.", err=True)
        sys.exit(1)

    db = get_graph()
    resultado = db.buscar_por_cpf(cpf_norm)
    db.close()

    if not resultado:
        click.echo(f"Nenhum dado encontrado para CPF {cpf_norm}.")
        return

    click.echo(json.dumps(resultado, indent=2, ensure_ascii=False, default=str))


@cli.command("top-insights")
@click.option("--tipo", default=None, help="Filtro por tipo (ex: VARIACAO_PATRIMONIAL)")
@click.option("--limite", default=20, show_default=True)
def top_insights(tipo, limite):
    """Lista os insights mais críticos do Acre."""
    db = get_graph()
    results = db.top_insights_ac(tipo=tipo, limit=limite)
    db.close()

    if not results:
        click.echo("Nenhum insight encontrado.")
        return

    click.echo(f"\n{'NOME':<40} {'TIPO':<25} {'SCORE':>5}  {'SEV'}")
    click.echo("-" * 85)
    for r in results:
        click.echo(
            f"{r['nome']:<40} {r['tipo']:<25} {r['score']:>5.2f}  {r['severidade']}"
        )


@cli.command()
@click.option("--ano", type=int, default=2024, help="Ano (padrão: 2024)")
@click.option("--force", is_flag=True, help="Re-download forçado")
@click.option("--sem-neo4j", is_flag=True)
def pipeline(ano, force, sem_neo4j):
    """
    Pipeline completo:
    TSE → Transparência AC → QSA Receita → Detecção → Neo4j
    """
    from src.ingest.tse_connector import TseConnector, ANOS
    from src.ingest.transparencia_ac_connector import TransparenciaAcConnector
    from src.ingest.receita_qsa_connector import ReceitaQSAConnector
    from src.detection.patrimonio import detectar_variacao_patrimonial
    from src.detection.emenda_familia import detectar_emenda_familia, detectar_contrato_suspeito

    # SE PEDIR 2024, CARREGA 2022 JUNTO (Para ter CPFs e histórico)
    anos_tse = [ano, 2022] if ano == 2024 else [ano]

    click.echo(f"\n{'='*60}")
    click.echo("haEmet — Pipeline Anticorrupção Acre (Multi-Year)")
    click.echo(f"Processando: {anos_tse}")
    click.echo(f"{'='*60}\n")

    # ── 1. TSE ─────────────────────────────────────────────────────────────────
    click.echo("📋 [1/4] Baixando dados do TSE...")
    tse = TseConnector(data_dir=os.getenv("DATA_DIR", "./data/tse"), force=force)
    # Carregamos TODOS os anos para o mesmo dicionário de pessoas na memória
    pessoas = tse.run(anos=anos_tse)
    click.echo(f"    → {len(pessoas)} pessoas únicas (ER - Entity Resolution)\n")

    # ── 2. Transparência AC ────────────────────────────────────────────────────
    click.echo("💰 [2/4] Baixando pagamentos/contratos do Portal de Transparência AC...")
    transp = TransparenciaAcConnector(force=force)
    # Filtra anos >= 2020 para o portal (limite da API do Acre)
    dados_ac = transp.run(anos=[a for a in anos_tse if a >= 2020])
    click.echo(f"    → {len(dados_ac['pagamentos'])} pagamentos")
    click.echo(f"    → {len(dados_ac['contratos'])} contratos")
    click.echo(f"    → {len(dados_ac['licitacoes'])} licitações")
    click.echo(f"    → {len(dados_ac['cnpjs_unicos'])} CNPJs únicos\n")

    # ── 3. QSA Receita Federal ─────────────────────────────────────────────────
    click.echo("🏢 [3/4] Resolvendo QSA (sócios) via Receita Federal...")
    cpfs_politicos = set(pessoas.keys())
    qsa = ReceitaQSAConnector(force=force)
    empresas = qsa.lookup_many(dados_ac["cnpjs_unicos"], cpfs_politicos)
    empresas_politicas = [e for e in empresas if e.tem_socio_politico]
    click.echo(f"    → {len(empresas)} empresas resolvidas")
    click.echo(f"    → {len(empresas_politicas)} com sócios políticos\n")

    # ── 4. Detecção ────────────────────────────────────────────────────────────
    click.echo("🔍 [4/4] Detectando padrões...")
    insights_patrimonio  = detectar_variacao_patrimonial(pessoas)
    insights_familia     = detectar_emenda_familia(pessoas, empresas, dados_ac["pagamentos"])
    insights_contratos   = detectar_contrato_suspeito(pessoas, empresas, dados_ac["licitacoes"])

    todos_insights = insights_patrimonio + insights_familia + insights_contratos
    todos_insights.sort(key=lambda i: i.score, reverse=True)

    click.echo(f"\n{'='*60}")
    click.echo(f"RESULTADO FINAL — {len(todos_insights)} insights")
    click.echo(f"  Variação Patrimonial: {len(insights_patrimonio)}")
    click.echo(f"  Emenda/Família:       {len(insights_familia)}")
    click.echo(f"  Contrato Suspeito:    {len(insights_contratos)}")
    click.echo(f"\nTOP 15 POR SCORE:")
    click.echo(f"{'NOME':<38} {'TIPO':<22} {'SCORE':>5}  SEV")
    click.echo("-" * 80)
    for i in todos_insights[:15]:
        p = pessoas.get(i.cpf_sujeito)
        nome = (p.nome_canonico if p else i.cpf_sujeito)[:37]
        click.echo(f"{nome:<38} {i.tipo.value:<22} {i.score:>5.2f}  {i.severidade.value}")

    if sem_neo4j:
        return

    # ── Persiste tudo no Neo4j ─────────────────────────────────────────────────
    click.echo(f"\n💾 Persistindo no Neo4j...")
    db = get_graph()
    db.create_schema()

    for pessoa in pessoas.values():
        db.upsert_pessoa(pessoa)

    for empresa in empresas:
        db.upsert_empresa_qsa(empresa)

    for pag in dados_ac["pagamentos"]:
        if pag.cnpjcpf and len(pag.cnpjcpf) == 14:
            db.upsert_pagamento_edge(
                pag.cnpjcpf, pag.credor, pag.valor,
                pag.data_movimento, pag.numero_empenho, pag.natureza_despesa
            )

    for insight in todos_insights:
        db.upsert_insight(insight)

    db.close()
    click.echo(f"✅ Concluído. Abra http://localhost:7474 para explorar o grafo.")


@cli.command()
def report():
    """Gera um relatório HTML visual com evidências claras para denúncia."""
    db = get_graph()
    results = db.top_insights_ac(limit=200)
    db.close()

    if not results:
        click.echo("Nenhum insight para reportar. Rode o pipeline com mais anos.")
        return

    html = f"""
    <html>
    <head>
        <title>Sentinela Acre — Relatório de Evidências</title>
        <style>
            body {{ font-family: 'Helvetica', sans-serif; background: #eef2f3; padding: 30px; }}
            .header {{ text-align: center; margin-bottom: 40px; border-bottom: 2px solid #333; padding-bottom: 10px; }}
            .card {{ background: #fff; padding: 25px; margin-bottom: 25px; border-radius: 12px; box-shadow: 0 4px 8px rgba(0,0,0,0.15); }}
            .CRITICO {{ border-top: 15px solid #ff4444; }}
            .ALTO {{ border-top: 15px solid #ff8800; }}
            .MEDIO {{ border-top: 15px solid #33b5e5; }}
            .score-badge {{ float: right; background: #333; color: #fff; padding: 5px 15px; border-radius: 20px; font-weight: bold; }}
            .nome {{ font-size: 1.6em; font-weight: bold; color: #d32f2f; margin-top: 10px; }}
            .tipo {{ font-size: 0.85em; background: #f0f0f0; padding: 3px 8px; border-radius: 5px; color: #555; }}
            .desc {{ font-size: 1.2em; margin: 15px 0; line-height: 1.6; color: #111; border-left: 4px solid #eee; padding-left: 15px; }}
            .evidencia {{ background: #fff9c4; padding: 10px; border-radius: 5px; font-family: monospace; font-size: 0.95em; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🔍 SENTINELA ACRE: Alertas Anticorrupção</h1>
            <p>Relatório de evidências gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
        </div>
    """
    for r in results:
        # Tenta formatar números se estiverem na descrição
        desc = r['descricao']
        # Highlight de valores R$
        import re
        desc = re.sub(r'(R\$ [\d\.,]+)', r'<strong style="color:#d32f2f">\1</strong>', desc)
        
        html += f"""
        <div class="card {r['severidade']}">
            <div class="score-badge">Score: {r['score']:.2f}</div>
            <span class="tipo">{r['tipo']}</span>
            <div class="nome">{r['nome']}</div>
            <div class="desc">{desc}</div>
            <div class="evidencia">EVIDÊNCIA: CPF {r['cpf']} - Processado via haEmet Intelligence</div>
        </div>
        """
    html += "</body></html>"
    
    with open("relatorio_corrupcao_acre.html", "w", encoding="utf-8") as f:
        f.write(html)
    
    click.echo(f"\n✅ Relatório de denúncia gerado: relatorio_corrupcao_acre.html")
    click.echo("Abra no seu navegador, escolha os casos críticos e tire seus prints.")

if __name__ == "__main__":
    from datetime import datetime
    cli()
