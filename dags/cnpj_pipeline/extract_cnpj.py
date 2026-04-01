import logging
import os
import re
import time
from pathlib import Path
from xml.etree import ElementTree

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

TOKEN = "YggdBLfdninEJX9"
BASE_URL = "https://arquivos.receitafederal.gov.br/public.php/dav/files"
DATA_DIR = Path("/opt/airflow/cnpj_data")

# só os arquivos usados pela pipeline atual
ARQUIVOS = [
    "Cnaes.zip",
    *[f"Empresas{i}.zip" for i in range(10)],
    *[f"Estabelecimentos{i}.zip" for i in range(10)],
    "Motivos.zip",
    "Municipios.zip",
    "Naturezas.zip",
    "Paises.zip",
    "Qualificacoes.zip",
    "Simples.zip",
]


def criar_sessao() -> requests.Session:
    session = requests.Session()
    session.auth = (TOKEN, "")
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept-Encoding": "identity",
        }
    )

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=2,
        allowed_methods=["GET", "PROPFIND"],
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def consulta_base_webdav(session: requests.Session) -> dict:
    """
    Descobre a última pasta YYYY-MM disponível e lista os zips do mês.
    """
    dav_ns = {"d": "DAV:"}
    url = "https://arquivos.receitafederal.gov.br/public.php/webdav/"

    resp = session.request(
        "PROPFIND",
        url,
        headers={"Depth": "1"},
        timeout=(30, 120),
    )
    resp.raise_for_status()

    root = ElementTree.fromstring(resp.content)
    diretorios = []

    for item in root.findall("d:response", dav_ns):
        href = item.find("d:href", dav_ns)
        if href is None or href.text is None:
            continue

        match = re.search(r"(\d{4}-\d{2})/?$", href.text)
        if match:
            diretorios.append(match.group(1))

    if not diretorios:
        raise RuntimeError("Não foi possível localizar diretórios no padrão YYYY-MM.")

    diretorios = sorted(set(diretorios))
    ano_mes = diretorios[-1]

    resp = session.request(
        "PROPFIND",
        f"{url}{ano_mes}/",
        headers={"Depth": "1"},
        timeout=(30, 120),
    )
    resp.raise_for_status()

    root = ElementTree.fromstring(resp.content)
    arquivos_disponiveis = []

    for item in root.findall("d:response", dav_ns):
        href = item.find("d:href", dav_ns)
        if href is None or href.text is None:
            continue

        match = re.search(r"/([^/]+\.zip)$", href.text, re.IGNORECASE)
        if match:
            arquivos_disponiveis.append(match.group(1))

    if not arquivos_disponiveis:
        raise RuntimeError(f"Nenhum arquivo .zip encontrado para a base {ano_mes}.")

    return {
        "ano_mes": ano_mes,
        "arquivos_disponiveis": sorted(set(arquivos_disponiveis)),
    }


def preparar_pasta(limpar_arquivos: bool = False) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if limpar_arquivos:
        for path in DATA_DIR.glob("*.zip"):
            path.unlink(missing_ok=True)
        for path in DATA_DIR.glob("*.part"):
            path.unlink(missing_ok=True)


def baixar_arquivo(
    session: requests.Session,
    ano_mes: str,
    nome_arquivo: str,
    tentativas: int = 6,
) -> None:
    url = f"{BASE_URL}/{TOKEN}/{ano_mes}/{nome_arquivo}"
    caminho_final = DATA_DIR / nome_arquivo
    caminho_tmp = DATA_DIR / f"{nome_arquivo}.part"

    for tentativa in range(1, tentativas + 1):
        try:
            baixado_parcial = caminho_tmp.stat().st_size if caminho_tmp.exists() else 0
            headers = {}

            if baixado_parcial > 0:
                headers["Range"] = f"bytes={baixado_parcial}-"
                log.info(
                    "Retomando %s a partir de %.1f MB (tentativa %s/%s)",
                    nome_arquivo,
                    baixado_parcial / 1_048_576,
                    tentativa,
                    tentativas,
                )
            else:
                log.info(
                    "Baixando %s (tentativa %s/%s)",
                    nome_arquivo,
                    tentativa,
                    tentativas,
                )

            with session.get(
                url,
                stream=True,
                timeout=(30, 1800),
                headers=headers,
            ) as resp:
                resp.raise_for_status()

                # Se pedimos Range mas o servidor respondeu 200,
                # então ele ignorou o Range e precisamos recomeçar do zero.
                if baixado_parcial > 0 and resp.status_code == 200:
                    log.warning(
                        "Servidor não retomou %s; reiniciando do zero.",
                        nome_arquivo,
                    )
                    caminho_tmp.unlink(missing_ok=True)
                    baixado_parcial = 0

                modo = "ab" if baixado_parcial > 0 and resp.status_code == 206 else "wb"

                content_length = int(resp.headers.get("content-length", 0))
                total_esperado = baixado_parcial + content_length if content_length else 0
                escritos = baixado_parcial
                ultimo_pct = int((baixado_parcial / total_esperado) * 100) if total_esperado else 0

                with open(caminho_tmp, modo) as f:
                    for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                        if not chunk:
                            continue

                        f.write(chunk)
                        escritos += len(chunk)

                        if total_esperado:
                            pct = int((escritos / total_esperado) * 100)
                            if pct >= ultimo_pct + 10:
                                log.info("%s - %s%%", nome_arquivo, pct)
                                ultimo_pct = pct

            tamanho_final = caminho_tmp.stat().st_size

            if total_esperado and tamanho_final != total_esperado:
                raise RuntimeError(
                    f"Download incompleto de {nome_arquivo}: "
                    f"esperado {total_esperado} bytes, baixado {tamanho_final}"
                )

            if tamanho_final == 0:
                raise RuntimeError(f"Arquivo baixado com 0 bytes: {nome_arquivo}")

            caminho_tmp.replace(caminho_final)
            log.info("OK: %s", nome_arquivo)
            return

        except Exception as e:
            log.warning(
                "Falha em %s na tentativa %s: %s",
                nome_arquivo,
                tentativa,
                e,
            )

            if tentativa == tentativas:
                caminho_tmp.unlink(missing_ok=True)
                raise

            time.sleep(min(10 * tentativa, 60))


def extract_cnpj(limpar_arquivos: bool = False):
    """
    Baixa os arquivos necessários do CNPJ para DATA_DIR.
    Retorna um resumo da extração.
    """
    preparar_pasta(limpar_arquivos=limpar_arquivos)

    session = criar_sessao()
    base = consulta_base_webdav(session)
    ano_mes = base["ano_mes"]
    disponiveis = set(base["arquivos_disponiveis"])

    faltando_no_site = [nome for nome in ARQUIVOS if nome not in disponiveis]
    if faltando_no_site:
        raise RuntimeError(
            f"Arquivos esperados não encontrados na base {ano_mes}: {faltando_no_site}"
        )

    erros = []

    for nome in ARQUIVOS:
        try:
            baixar_arquivo(session, ano_mes, nome)
        except Exception as e:
            log.error("ERRO: %s - %s", nome, e)
            erros.append(nome)
            break

    if erros:
        raise RuntimeError(f"Falha no download dos arquivos: {erros}")

    arquivos_baixados = sorted(p.name for p in DATA_DIR.glob("*.zip"))

    log.info("Extração concluída com sucesso")
    return {
        "ano_mes": ano_mes,
        "pasta_zip": str(DATA_DIR),
        "quantidade_arquivos": len(arquivos_baixados),
        "arquivos": arquivos_baixados,
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s - %(message)s",
    )
    print(extract_cnpj(limpar_arquivos=False))