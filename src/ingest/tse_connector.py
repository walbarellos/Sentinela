"""
haEmet — Conector TSE (Tribunal Superior Eleitoral)

Baixa para o Acre:
  - consulta_cand_AAAA.zip  → candidaturas (filtra UF=AC)
  - bem_candidato_AAAA.zip  → bens declarados (filtra UF=AC)

Fonte: https://dadosabertos.tse.jus.br/
"""
import io
import logging
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

from src.core.entities import Candidatura, PatrimonioSnapshot, Pessoa
from src.core.normalizer import normalize_cpf, normalize_currency, normalize_name, sha256_file

log = logging.getLogger(__name__)

BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele"
ANOS = [2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024]


@dataclass
class IngestResult:
    source: str = ""
    records_baixados: int = 0
    records_normalizados: int = 0
    records_pulados: int = 0
    insights_gerados: int = 0
    success: bool = False
    error: str = ""


class TseConnector:
    def __init__(self, data_dir: str = "./data/tse", force: bool = False):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.force = force
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "haEmet/1.0 (anticorrupcao-acre)"

    # ── Ponto de entrada ──────────────────────────────────────────────────────

    def run(self, anos: list[int] = None) -> dict[str, Pessoa]:
        """
        Processa todos os anos e retorna dicionário CPF → Pessoa
        com histórico patrimonial completo — pronto para ir ao Neo4j.
        """
        anos = anos or ANOS
        pessoas: dict[str, Pessoa] = {}

        for ano in anos:
            log.info("TSE — processando %d", ano)
            try:
                df_cand = self._get_candidaturas(ano)
                df_bens = self._get_bens(ano)
                self._apply_candidaturas(pessoas, df_cand, ano)
                self._apply_bens(pessoas, df_bens, ano)
                log.info("  %d: %d candidatos AC acumulados", ano, len(pessoas))
            except Exception as e:
                log.error("  %d falhou: %s", ano, e)

        log.info("TSE concluído — %d pessoas únicas no Acre", len(pessoas))
        return pessoas

    # ── Download + parse ──────────────────────────────────────────────────────

    def _get_candidaturas(self, ano: int) -> pd.DataFrame:
        url = f"{BASE_URL}/consulta_cand/consulta_cand_{ano}.zip"
        csv_name = f"consulta_cand_{ano}_AC.csv"
        path = self._download_zip(url, csv_name, ano, "cand")
        return self._read_tse_csv(path)

    def _get_bens(self, ano: int) -> pd.DataFrame:
        url = f"{BASE_URL}/bem_candidato/bem_candidato_{ano}.zip"
        csv_name = f"bem_candidato_{ano}_AC.csv"
        try:
            path = self._download_zip(url, csv_name, ano, "bens")
            return self._read_tse_csv(path)
        except Exception as e:
            log.warning("Bens %d não disponível: %s", ano, e)
            return pd.DataFrame()

    def _download_zip(self, url: str, csv_name: str, ano: int, tipo: str) -> Path:
        dest = self.data_dir / csv_name

        if dest.exists() and not self.force:
            log.debug("Cache hit: %s", dest)
            return dest

        log.info("Baixando %s ...", url)
        resp = self.session.get(url, stream=True, timeout=300)
        resp.raise_for_status()

        total = int(resp.headers.get("content-length", 0))
        buf = io.BytesIO()

        with tqdm(total=total, unit="B", unit_scale=True, desc=f"{tipo}/{ano}") as bar:
            for chunk in resp.iter_content(chunk_size=65536):
                buf.write(chunk)
                bar.update(len(chunk))

        buf.seek(0)
        with zipfile.ZipFile(buf) as zf:
            # Tenta o arquivo específico do AC primeiro
            names = zf.namelist()
            target = next((n for n in names if csv_name.lower() in n.lower()), None)

            if not target:
                # Fallback: arquivo nacional (filtramos por UF depois)
                target = next((n for n in names if n.lower().endswith(".csv")), None)

            if not target:
                raise FileNotFoundError(f"Nenhum CSV encontrado em {url}")

            data = zf.read(target)
            dest.write_bytes(data)
            log.info("  Extraído: %s (%d KB)", dest.name, len(data) // 1024)

        return dest

    def _read_tse_csv(self, path: Path) -> pd.DataFrame:
        """
        Lê CSV do TSE: Latin-1, separador ";".
        Tenta detectar se há linha de metadados ou se começa direto no header.
        """
        try:
            # Tenta ler as primeiras linhas para ver se a primeira é metadado
            with open(path, "r", encoding="latin-1") as f:
                first_line = f.readline()
                # Se a primeira linha não tem as colunas principais, pula ela
                skip = 0
                if "ANO_ELEICAO" not in first_line.upper() and "DT_GERACAO" not in first_line.upper():
                    skip = 1
            
            df = pd.read_csv(
                path,
                encoding="latin-1",
                sep=";",
                skiprows=skip,
                dtype=str,
                on_bad_lines="skip",
                low_memory=False,
                quotechar='"',
            )
        except Exception as e:
            log.error("Erro ao ler CSV %s: %s", path.name, e)
            return pd.DataFrame()

        df.columns = [c.strip().upper() for c in df.columns]
        df = df.fillna("")

        # Filtra Acre se a coluna existir
        if "SG_UF" in df.columns:
            df = df[df["SG_UF"].str.strip() == "AC"]

        log.info("  CSV %s: %d linhas AC carregadas", path.name, len(df))
        return df

    # ── Normalização ──────────────────────────────────────────────────────────

    def _apply_candidaturas(
        self,
        pessoas: dict[str, Pessoa],
        df: pd.DataFrame,
        ano: int,
    ):
        if df.empty:
            return

        lgpd_count = 0

        for _, row in df.iterrows():
            cpf_raw = row.get("NR_CPF_CANDIDATO", "")
            cpf = normalize_cpf(cpf_raw)
            nome = normalize_name(row.get("NM_CANDIDATO", ""))
            nasc = row.get("DT_NASCIMENTO", "").strip()
            
            # CHAVE MESTRA: Nome + Nascimento (Robusta contra LGPD)
            id_unico = f"{nome}|{nasc}"

            # Se não temos CPF, usamos a chave mestre para tentar achar a pessoa no grafo
            # ou usamos o sequencial do TSE se for uma pessoa nova
            if not cpf:
                sq = row.get("SQ_CANDIDATO", "").strip()
                cpf = f"seq:{sq}"
                lgpd_count += 1

            # Tenta encontrar no dicionário pela chave mestre (ER - Entity Resolution)
            p = next((p for p in pessoas.values() if p.nome_canonico == nome and p.data_nascimento == nasc), None)
            
            if p:
                # Se achamos, usamos o CPF que já temos (que pode ser o real de 2022!)
                cpf = p.cpf
            else:
                pessoas[cpf] = Pessoa(
                    cpf=cpf,
                    nome_canonico=nome,
                    nome_urna=normalize_name(row.get("NM_URNA_CANDIDATO", "")),
                    data_nascimento=nasc,
                    fonte=f"TSE_CAND_{ano}",
                )
                p = pessoas[cpf]

            cand = Candidatura(
                ano=ano,
                cargo=normalize_name(row.get("DS_CARGO", "")),
                partido=row.get("SG_PARTIDO", "").strip(),
                numero_urna=row.get("NR_CANDIDATO", "").strip(),
                situacao=normalize_name(row.get("DS_SIT_TOT_TURNO", "")),
                uf="AC",
            )
            p.candidaturas.append(cand)

        if lgpd_count:
            log.warning(
                "  %d candidatos com CPF mascarado (LGPD) — "
                "usando SQ_CANDIDATO como chave. "
                "Cruzamento com QSA só funciona para anos ≤ 2022.",
                lgpd_count
            )

    def _apply_bens(
        self,
        pessoas: dict[str, Pessoa],
        df: pd.DataFrame,
        ano: int,
    ):
        if df.empty:
            return

        # CHAVE MESTRA: Primeiro vamos criar um mapa (Nome|Nasc) -> Pessoa
        # para que o CPF mascarado (LGPD) em 2024 encontre a pessoa de 2022
        mapa_chaves = {f"{p.nome_canonico}|{p.data_nascimento}": p for p in pessoas.values()}

        # Agrupa bens por CPF/SEQ e soma
        totais_brutos: dict[str, float] = {}
        for _, row in df.iterrows():
            nome = normalize_name(row.get("NM_CANDIDATO", ""))
            # No arquivo de BENS, às vezes não vem DT_NASCIMENTO em alguns anos antigos.
            # Em 2024 vem. Se não vier, usamos o CPF mascarado como chave de agrupamento temporária.
            nasc = row.get("DT_NASCIMENTO", "").strip()
            
            # Tenta encontrar a pessoa pela chave mestre
            chave_pessoa = f"{nome}|{nasc}"
            p = mapa_chaves.get(chave_pessoa)
            
            if p:
                id_pessoa = p.cpf
            else:
                # Se não temos a pessoa (estranho, deveria ter vindo do arquivo de cand)
                # usamos o CPF que estiver no registro
                id_pessoa = normalize_cpf(row.get("NR_CPF_CANDIDATO", ""))
                if not id_pessoa:
                    sq = row.get("SQ_CANDIDATO", "").strip()
                    id_pessoa = f"seq:{sq}"
            
            totais_brutos[id_pessoa] = totais_brutos.get(id_pessoa, 0.0) + normalize_currency(row.get("VR_BEM", ""))

        for id_pessoa, total in totais_brutos.items():
            if id_pessoa not in pessoas:
                # Cria pessoa "fantasma" se ela existir no arquivo de bens mas não no de cand
                pessoas[id_pessoa] = Pessoa(cpf=id_pessoa, nome_canonico="", fonte=f"TSE_BENS_{ano}")

            p = pessoas[id_pessoa]
            # Remove snapshot do mesmo ano (evita duplicata no re-run)
            p.historico_patrimonio = [
                s for s in p.historico_patrimonio if s.ano != ano
            ]
            p.historico_patrimonio.append(
                PatrimonioSnapshot(ano=ano, total_declarado=total)
            )

        # Ordena histórico por ano
        for p in pessoas.values():
            p.historico_patrimonio.sort(key=lambda x: x.ano)
