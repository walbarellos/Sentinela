"""
haEmet — Persistência no Neo4j
Usa MERGE para garantir idempotência — pode rodar várias vezes sem duplicar.
"""
import logging
from contextlib import contextmanager
from neo4j import GraphDatabase, Driver
from src.core.entities import Pessoa, Empresa, Insight

log = logging.getLogger(__name__)


class GraphDB:
    def __init__(self, uri: str, user: str, password: str):
        self._driver: Driver = GraphDatabase.driver(uri, auth=(user, password))
        log.info("Neo4j conectado em %s", uri)

    def close(self):
        self._driver.close()

    @contextmanager
    def session(self):
        with self._driver.session() as s:
            yield s

    # ── Schema ────────────────────────────────────────────────────────────────

    def create_schema(self):
        """Cria constraints e índices. Idempotente."""
        constraints = [
            "CREATE CONSTRAINT pessoa_cpf IF NOT EXISTS FOR (p:Pessoa) REQUIRE p.cpf IS UNIQUE",
            "CREATE CONSTRAINT empresa_cnpj IF NOT EXISTS FOR (e:Empresa) REQUIRE e.cnpj IS UNIQUE",
            "CREATE CONSTRAINT insight_id IF NOT EXISTS FOR (i:Insight) REQUIRE i.id IS UNIQUE",
        ]
        indexes = [
            "CREATE INDEX pessoa_nome IF NOT EXISTS FOR (p:Pessoa) ON (p.nome_canonico)",
            "CREATE INDEX insight_tipo IF NOT EXISTS FOR (i:Insight) ON (i.tipo)",
            "CREATE INDEX insight_score IF NOT EXISTS FOR (i:Insight) ON (i.score)",
        ]
        with self.session() as s:
            for q in constraints + indexes:
                s.run(q)
        log.info("Schema Neo4j OK")

    # ── Pessoa ────────────────────────────────────────────────────────────────

    def upsert_pessoa(self, p: Pessoa):
        query = """
        MERGE (pessoa:Pessoa {cpf: $cpf})
        SET pessoa.nome_canonico    = $nome_canonico,
            pessoa.nome_urna        = $nome_urna,
            pessoa.fonte            = $fonte,
            pessoa.proveniencia_sha = $sha,
            pessoa.updated_at       = datetime()

        WITH pessoa

        UNWIND $candidaturas AS c
        MERGE (cand:Candidatura {cpf: $cpf, ano: c.ano, cargo: c.cargo})
        SET cand.partido     = c.partido,
            cand.situacao    = c.situacao,
            cand.uf          = c.uf,
            cand.total_bens  = c.total_bens
        MERGE (pessoa)-[:CONCORREU]->(cand)

        WITH pessoa

        UNWIND $patrimonio AS snap
        MERGE (pat:PatrimonioSnapshot {cpf: $cpf, ano: snap.ano})
        SET pat.total_declarado = snap.total_declarado,
            pat.fonte_sha256    = snap.fonte_sha256
        MERGE (pessoa)-[:DECLAROU_PATRIMONIO]->(pat)
        """
        with self.session() as s:
            s.run(query,
                cpf=p.cpf,
                nome_canonico=p.nome_canonico,
                nome_urna=p.nome_urna,
                fonte=p.fonte,
                sha=p.proveniencia_sha256,
                candidaturas=[
                    {
                        "ano": c.ano, "cargo": c.cargo, "partido": c.partido,
                        "situacao": c.situacao, "uf": c.uf, "total_bens": c.total_bens
                    }
                    for c in p.candidaturas
                ],
                patrimonio=[
                    {"ano": s.ano, "total_declarado": s.total_declarado,
                     "fonte_sha256": s.fonte_sha256}
                    for s in p.historico_patrimonio
                ],
            )

    # ── Empresa ───────────────────────────────────────────────────────────────

    def upsert_empresa(self, e: Empresa):
        query = """
        MERGE (emp:Empresa {cnpj: $cnpj})
        SET emp.razao_social       = $razao_social,
            emp.nome_fantasia      = $nome_fantasia,
            emp.cnae_principal     = $cnae_principal,
            emp.situacao_cadastral = $situacao_cadastral,
            emp.municipio          = $municipio,
            emp.uf                 = $uf,
            emp.updated_at         = datetime()

        WITH emp

        UNWIND $socios AS s
        MERGE (socio:Pessoa {cpf: s.cpf_cnpj})
        ON CREATE SET socio.nome_canonico = s.nome
        MERGE (socio)-[:SOCIO_DE {qualificacao: s.qualificacao}]->(emp)
        """
        with self.session() as s:
            s.run(query,
                cnpj=e.cnpj,
                razao_social=e.razao_social,
                nome_fantasia=e.nome_fantasia,
                cnae_principal=e.cnae_principal,
                situacao_cadastral=e.situacao_cadastral,
                municipio=e.municipio,
                uf=e.uf,
                socios=e.socios,
            )

    # ── Insight ───────────────────────────────────────────────────────────────

    def upsert_insight(self, insight: Insight):
        query = """
        MERGE (i:Insight {id: $id})
        SET i.tipo        = $tipo,
            i.descricao   = $descricao,
            i.score       = $score,
            i.severidade  = $severidade,
            i.evidencias  = $evidencias,
            i.versao_regra= $versao_regra,
            i.detectado_em= datetime()

        WITH i
        MATCH (p:Pessoa {cpf: $cpf_sujeito})
        MERGE (p)-[:TEM_INSIGHT]->(i)
        """
        import json
        with self.session() as s:
            s.run(query,
                id=insight.id,
                tipo=insight.tipo.value,
                descricao=insight.descricao,
                score=insight.score,
                severidade=insight.severidade.value,
                evidencias=json.dumps(insight.evidencias, ensure_ascii=False),
                versao_regra=insight.versao_regra,
                cpf_sujeito=insight.cpf_sujeito,
            )

    # ── Queries analíticas ────────────────────────────────────────────────────

    def buscar_por_cpf(self, cpf: str) -> dict:
        """Retorna subgrafo completo de uma pessoa."""
        query = """
        MATCH (p:Pessoa {cpf: $cpf})
        OPTIONAL MATCH (p)-[:TEM_INSIGHT]->(i:Insight)
        OPTIONAL MATCH (p)-[:SOCIO_DE]->(e:Empresa)
        OPTIONAL MATCH (p)-[:CONCORREU]->(c:Candidatura)
        OPTIONAL MATCH (p)-[:DECLAROU_PATRIMONIO]->(pat:PatrimonioSnapshot)
        RETURN p, collect(DISTINCT i) AS insights,
               collect(DISTINCT e) AS empresas,
               collect(DISTINCT c) AS candidaturas,
               collect(DISTINCT pat) AS patrimonio
        """
        with self.session() as s:
            result = s.run(query, cpf=cpf).single()
            return dict(result) if result else {}

    def upsert_empresa_qsa(self, empresa):
        """Upsert de EmpresaQSA com seus sócios."""
        query = """
        MERGE (emp:Empresa {cnpj: $cnpj})
        SET emp.razao_social       = $razao_social,
            emp.nome_fantasia      = $nome_fantasia,
            emp.cnae_principal     = $cnae_principal,
            emp.cnae_descricao     = $cnae_descricao,
            emp.situacao_cadastral = $situacao_cadastral,
            emp.municipio          = $municipio,
            emp.uf                 = $uf,
            emp.data_abertura      = $data_abertura,
            emp.capital_social     = $capital_social,
            emp.updated_at         = datetime()

        WITH emp
        UNWIND $socios AS s
        MERGE (socio:Pessoa {cpf: s.cpf_cnpj})
        ON CREATE SET socio.nome_canonico = s.nome
        MERGE (socio)-[:SOCIO_DE {qualificacao: s.qualificacao, entrada: s.entrada}]->(emp)
        """
        with self.session() as s:
            s.run(query,
                cnpj=empresa.cnpj,
                razao_social=empresa.razao_social,
                nome_fantasia=empresa.nome_fantasia,
                cnae_principal=empresa.cnae_principal,
                cnae_descricao=empresa.cnae_descricao,
                situacao_cadastral=empresa.situacao_cadastral,
                municipio=empresa.municipio,
                uf=empresa.uf,
                data_abertura=empresa.data_abertura,
                capital_social=empresa.capital_social,
                socios=[{
                    "cpf_cnpj": s.cpf_cnpj,
                    "nome": s.nome,
                    "qualificacao": s.qualificacao,
                    "entrada": s.entrada,
                } for s in empresa.socios],
            )

    def upsert_pagamento_edge(self, cnpj: str, credor: str, valor: float,
                              data: str, num_empenho: str, natureza: str):
        """Cria borda (:Empresa)-[:RECEBEU_PAGAMENTO]->(:OrgaoEstadual)."""
        query = """
        MERGE (e:Empresa {cnpj: $cnpj})
        ON CREATE SET e.razao_social = $credor
        MERGE (o:OrgaoEstadual {nome: 'GOVERNO_ESTADO_ACRE'})
        MERGE (e)-[rel:RECEBEU_PAGAMENTO {num_empenho: $num_empenho}]->(o)
        SET rel.valor    = $valor,
            rel.data     = $data,
            rel.natureza = $natureza
        """
        with self.session() as s:
            s.run(query, cnpj=cnpj, credor=credor, valor=valor,
                  data=data, num_empenho=num_empenho, natureza=natureza)

    def top_insights_ac(self, tipo: str = None, limit: int = 50) -> list[dict]:
        """Top insights do Acre por score."""
        where = "WHERE c.uf = 'AC'"
        if tipo:
            where += f" AND i.tipo = '{tipo}'"
        query = f"""
        MATCH (p:Pessoa)-[:TEM_INSIGHT]->(i:Insight)
        MATCH (p)-[:CONCORREU]->(c:Candidatura)
        {where}
        RETURN p.cpf AS cpf, p.nome_canonico AS nome,
               i.tipo AS tipo, i.score AS score,
               i.severidade AS severidade, i.descricao AS descricao
        ORDER BY i.score DESC
        LIMIT $limit
        """
        with self.session() as s:
            return [dict(r) for r in s.run(query, limit=limit)]
