import os
import csv
import re
import unicodedata
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# ============== CONFIG BÁSICA ==============
APP_TITLE = "Painel financeiro compartilhado"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

USERS_CSV = DATA_DIR / "users.csv"
PRODUCTS_CSV = DATA_DIR / "products.csv"
PRICING_CSV = DATA_DIR / "pricing.csv"
STOCK_CSV = DATA_DIR / "stock.csv"
CASH_CSV = DATA_DIR / "cash.csv"
SALES_CSV = DATA_DIR / "sales.csv"
BAD_SALES_CSV = DATA_DIR / "bad_rows_sales.csv"

LOJAS = {
    "dx": "Loja 1 (DX)",
    "nv": "Loja 2 (NV)",
    "c3": "Loja 3 (C3)",
    "admin": "Admin (Venda livre)",
}

PAGAMENTOS = ["dinheiro", "pix", "fiado"]
TIPOS_VENDA = ["gramas", "pacote"]  # pacote = preço fixo e gramas fixas por pacote

# Produtos "pseudônimos" padrão (admin pode editar no CSV products.csv)
DEFAULT_PRODUCTS = [
    {"produto_id": "produto1", "nome": "Produto 1", "unidade": "g", "ativo": True},
    {"produto_id": "produto2", "nome": "Produto 2", "unidade": "g", "ativo": True},
    {"produto_id": "produto3", "nome": "Produto 3", "unidade": "g", "ativo": True},
]

# ===================== NORMALIZAÇÃO (mata "leite/fotos/nam" no produto) =====================
def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def normalize_text(raw: str) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def normalize_id(raw: str) -> str:
    """id interno sem acento, minúsculo, underscore."""
    s = normalize_text(raw).lower()
    s = _strip_accents(s)
    s = s.replace("-", "_").replace(" ", "_").replace("/", "_")
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s

# ===================== CSV HELPERS =====================
def ensure_csv(path: Path, header: list[str], default_rows: list[dict] | None = None):
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            if default_rows:
                for r in default_rows:
                    w.writerow({k: r.get(k, "") for k in header})

def read_csv_safe(path: Path, expected_cols: list[str]) -> pd.DataFrame:
    """
    Lê CSV blindado:
    - se tiver linha quebrada, manda para quarentena (no caso de sales)
    - garante todas as colunas esperadas
    """
    if not path.exists():
        return pd.DataFrame(columns=expected_cols)

    # tenta leitura "tolerante"
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, on_bad_lines="skip", encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, on_bad_lines="skip", encoding="utf-8-sig")

    # garante colunas
    for c in expected_cols:
        if c not in df.columns:
            df[c] = ""
    df = df[expected_cols].copy()

    return df

def append_row_csv(path: Path, header: list[str], row: dict):
    ensure_csv(path, header)
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writerow({k: row.get(k, "") for k in header})

def to_float(x, default=0.0) -> float:
    try:
        if x is None or str(x).strip() == "":
            return default
        s = str(x).replace(",", ".")
        return float(s)
    except Exception:
        return default

def to_int(x, default=0) -> int:
    try:
        if x is None or str(x).strip() == "":
            return default
        return int(float(str(x).replace(",", ".")))
    except Exception:
        return default

def parse_date_safe(x) -> date | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

# ===================== SCHEMAS =====================
USERS_HEADER = ["username", "password", "role", "loja"]
PRODUCTS_HEADER = ["produto_id", "nome", "unidade", "ativo"]
PRICING_HEADER = [
    "loja", "produto_id",
    "preco_venda_g", "custo_g",
    "pacote_ativo", "pacote_preco", "pacote_gramas",
    "repasse_colab_fixo",
    "updated_at",
]
STOCK_HEADER = ["loja", "produto_id", "estoque_g", "pacotes_prontos", "updated_at"]
CASH_HEADER = ["loja", "saldo", "updated_at"]
SALES_HEADER = [
    "id", "timestamp", "data",
    "loja", "vendedor", "role",
    "produto_id", "tipo_venda",
    "gramas", "pacotes",
    "valor_venda",
    "pagamento",
    "fiado_nome", "fiado_vencimento",
    "observacao",
    "custo_estimado", "repasse_colab", "lucro_estimado"
]

# ===================== INIT DATA =====================
def init_data():
    # users
    ensure_csv(
        USERS_CSV,
        USERS_HEADER,
        default_rows=[
            {"username": "admin", "password": "admin123", "role": "admin", "loja": "admin"},
            {"username": "lojista_dx", "password": "123", "role": "lojista", "loja": "dx"},
            {"username": "lojista_nv", "password": "123", "role": "lojista", "loja": "nv"},
            {"username": "lojista_c3", "password": "123", "role": "lojista", "loja": "c3"},
        ],
    )

    # products
    ensure_csv(PRODUCTS_CSV, PRODUCTS_HEADER, default_rows=DEFAULT_PRODUCTS)

    # pricing/stock/cash - cria vazio, mas coerente
    ensure_csv(PRICING_CSV, PRICING_HEADER)
    ensure_csv(STOCK_CSV, STOCK_HEADER)
    ensure_csv(CASH_CSV, CASH_HEADER)
    ensure_csv(SALES_CSV, SALES_HEADER)
    ensure_csv(BAD_SALES_CSV, ["raw_line", "reason", "created_at"])

def load_users() -> pd.DataFrame:
    return read_csv_safe(USERS_CSV, USERS_HEADER)

def load_products() -> pd.DataFrame:
    df = read_csv_safe(PRODUCTS_CSV, PRODUCTS_HEADER)
    df["ativo"] = df["ativo"].astype(str).str.lower().isin(["true", "1", "yes", "sim"])
    df["produto_id"] = df["produto_id"].apply(normalize_id)
    df["nome"] = df["nome"].apply(normalize_text)
    return df

def load_pricing() -> pd.DataFrame:
    df = read_csv_safe(PRICING_CSV, PRICING_HEADER)
    df["loja"] = df["loja"].apply(normalize_id)
    df["produto_id"] = df["produto_id"].apply(normalize_id)
    return df

def load_stock() -> pd.DataFrame:
    df = read_csv_safe(STOCK_CSV, STOCK_HEADER)
    df["loja"] = df["loja"].apply(normalize_id)
    df["produto_id"] = df["produto_id"].apply(normalize_id)
    return df

def load_cash() -> pd.DataFrame:
    df = read_csv_safe(CASH_CSV, CASH_HEADER)
    df["loja"] = df["loja"].apply(normalize_id)
    return df

def load_sales() -> pd.DataFrame:
    df = read_csv_safe(SALES_CSV, SALES_HEADER)

    # normaliza e garante DATA válida (resolve KeyError: 'data')
    df["data"] = df["data"].apply(lambda x: parse_date_safe(x).isoformat() if parse_date_safe(x) else "")
    df["timestamp"] = df["timestamp"].apply(normalize_text)
    df["loja"] = df["loja"].apply(normalize_id)
    df["produto_id"] = df["produto_id"].apply(normalize_id)
    df["vendedor"] = df["vendedor"].apply(normalize_text)
    df["role"] = df["role"].apply(normalize_id)
    df["tipo_venda"] = df["tipo_venda"].apply(normalize_id)
    df["pagamento"] = df["pagamento"].apply(normalize_id)
    return df

def get_product_label(prod_id: str, products_df: pd.DataFrame) -> str:
    row = products_df.loc[products_df["produto_id"] == prod_id]
    if len(row) == 0:
        return prod_id
    name = row.iloc[0]["nome"]
    # ícones leves (sem animação pesada)
    icon = "📦"
    if prod_id == "produto1":
        icon = "🟩"
    elif prod_id == "produto2":
        icon = "🧊"
    elif prod_id == "produto3":
        icon = "⚡"
    return f"{name} {icon}"

# ===================== REGRAS DE NEGÓCIO =====================
def ensure_pricing_rows(pricing_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    now = datetime.now().isoformat(timespec="seconds")
    rows = []
    for loja in ["dx", "nv", "c3"]:
        for _, p in products_df[products_df["ativo"]].iterrows():
            pid = p["produto_id"]
            exists = ((pricing_df["loja"] == loja) & (pricing_df["produto_id"] == pid)).any()
            if not exists:
                rows.append({
                    "loja": loja, "produto_id": pid,
                    "preco_venda_g": "0",
                    "custo_g": "0",
                    "pacote_ativo": "true",
                    "pacote_preco": "0",
                    "pacote_gramas": "0",
                    "repasse_colab_fixo": "0",
                    "updated_at": now,
                })
    if rows:
        for r in rows:
            append_row_csv(PRICING_CSV, PRICING_HEADER, r)
        pricing_df = load_pricing()
    return pricing_df

def ensure_stock_rows(stock_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    now = datetime.now().isoformat(timespec="seconds")
    rows = []
    for loja in ["dx", "nv", "c3"]:
        for _, p in products_df[products_df["ativo"]].iterrows():
            pid = p["produto_id"]
            exists = ((stock_df["loja"] == loja) & (stock_df["produto_id"] == pid)).any()
            if not exists:
                rows.append({
                    "loja": loja, "produto_id": pid,
                    "estoque_g": "0",
                    "pacotes_prontos": "0",
                    "updated_at": now,
                })
    if rows:
        for r in rows:
            append_row_csv(STOCK_CSV, STOCK_HEADER, r)
        stock_df = load_stock()
    return stock_df

def ensure_cash_rows(cash_df: pd.DataFrame) -> pd.DataFrame:
    now = datetime.now().isoformat(timespec="seconds")
    for loja in ["dx", "nv", "c3", "admin"]:
        if not (cash_df["loja"] == loja).any():
            append_row_csv(CASH_CSV, CASH_HEADER, {"loja": loja, "saldo": "0", "updated_at": now})
    return load_cash()

def write_df_over_csv(path: Path, header: list[str], df: pd.DataFrame):
    df = df.copy()
    for c in header:
        if c not in df.columns:
            df[c] = ""
    df = df[header]
    df.to_csv(path, index=False, encoding="utf-8")

def update_stock(stock_df: pd.DataFrame, loja: str, produto_id: str, delta_g: float, delta_pacotes: int):
    # delta negativo = baixa
    mask = (stock_df["loja"] == loja) & (stock_df["produto_id"] == produto_id)
    if not mask.any():
        return
    i = stock_df[mask].index[0]
    estoque_g = to_float(stock_df.at[i, "estoque_g"])
    pacotes = to_int(stock_df.at[i, "pacotes_prontos"])
    estoque_g = estoque_g + delta_g
    pacotes = pacotes + delta_pacotes
    stock_df.at[i, "estoque_g"] = f"{estoque_g:.3f}"
    stock_df.at[i, "pacotes_prontos"] = str(pacotes)
    stock_df.at[i, "updated_at"] = datetime.now().isoformat(timespec="seconds")

def update_cash(cash_df: pd.DataFrame, loja: str, delta: float):
    mask = (cash_df["loja"] == loja)
    if not mask.any():
        return
    i = cash_df[mask].index[0]
    saldo = to_float(cash_df.at[i, "saldo"])
    saldo += delta
    cash_df.at[i, "saldo"] = f"{saldo:.2f}"
    cash_df.at[i, "updated_at"] = datetime.now().isoformat(timespec="seconds")

def compute_sale(pricing_df: pd.DataFrame, loja: str, produto_id: str, tipo: str, gramas: float, pacotes: int, valor: float, role: str):
    """
    Calcula custo/lucro estimado.
    - role=admin => repasse 0
    - lojista => repasse_colab_fixo aplicado por venda (config)
    """
    mask = (pricing_df["loja"] == loja) & (pricing_df["produto_id"] == produto_id)
    custo_g = 0.0
    repasse = 0.0
    pacote_gramas = 0.0
    pacote_preco = 0.0

    if mask.any():
        r = pricing_df[mask].iloc[0]
        custo_g = to_float(r["custo_g"])
        repasse = to_float(r["repasse_colab_fixo"])
        pacote_gramas = to_float(r["pacote_gramas"])
        pacote_preco = to_float(r["pacote_preco"])

    # custo estimado
    if tipo == "pacote":
        total_g = pacote_gramas * pacotes
        custo = total_g * custo_g
    else:
        custo = gramas * custo_g

    if role == "admin":
        repasse = 0.0

    lucro = valor - custo - repasse
    return custo, repasse, lucro

# ===================== UI HELPERS =====================
def kpi_row(items: list[tuple[str, str]]):
    cols = st.columns(len(items))
    for c, (label, value) in zip(cols, items):
        with c:
            st.metric(label, value)

def df_last_14_days(sales_df: pd.DataFrame, loja: str | None = None):
    # garante 'data' como date
    df = sales_df.copy()
    df["data_dt"] = df["data"].apply(parse_date_safe)
    df = df[df["data_dt"].notna()].copy()
    if loja:
        df = df[df["loja"] == loja].copy()

    end = date.today()
    start = end - timedelta(days=13)
    df = df[(df["data_dt"] >= start) & (df["data_dt"] <= end)].copy()

    df["valor_venda_num"] = df["valor_venda"].apply(to_float)
    daily = df.groupby("data_dt", as_index=False)["valor_venda_num"].sum().sort_values("data_dt")
    # preencher dias faltantes (evita bug visual)
    all_days = pd.date_range(start=start, end=end, freq="D").date
    daily = daily.set_index("data_dt").reindex(all_days, fill_value=0).reset_index()
    daily.columns = ["data_dt", "valor_venda_num"]
    return df, daily

# ===================== AUTH =====================
def do_login(users_df: pd.DataFrame):
    st.title(APP_TITLE)
    st.caption("Login para Admin e Lojistas.")

    with st.form("login"):
        u = st.text_input("Usuário")
        p = st.text_input("Senha", type="password")
        ok = st.form_submit_button("Entrar")

    if ok:
        row = users_df[(users_df["username"] == u) & (users_df["password"] == p)]
        if len(row) == 0:
            st.error("Usuário ou senha inválidos.")
            return
        r = row.iloc[0]
        st.session_state["auth"] = True
        st.session_state["username"] = r["username"]
        st.session_state["role"] = r["role"]
        st.session_state["loja"] = r["loja"]
        st.rerun()

def logout_button():
    if st.sidebar.button("Sair"):
        for k in ["auth", "username", "role", "loja"]:
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

# ===================== IA (opcional) =====================
def ai_enabled() -> bool:
    return bool(os.environ.get("OPENAI_API_KEY", "").strip())

def ai_analyze(text: str) -> str:
    """
    Integração simples com OpenAI via SDK.
    Se não tiver OPENAI_API_KEY, retorna aviso.
    """
    if not ai_enabled():
        return "IA desativada: defina a variável de ambiente OPENAI_API_KEY no Windows."

    try:
        from openai import OpenAI
        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        # Modelo pode mudar — ajuste se quiser.
        resp = client.responses.create(
            model="gpt-5.2",
            input=text,
        )
        return resp.output_text or "(sem resposta)"
    except Exception as e:
        return f"Erro na IA: {e}"

# ===================== PÁGINAS =====================
def page_resumo(role: str, loja: str, products_df, pricing_df, stock_df, cash_df, sales_df):
    st.header("Resumo")

    if role == "admin":
        lojas = ["dx", "nv", "c3", "admin"]
    else:
        lojas = [loja]

    total_vendas = 0.0
    total_lucro = 0.0
    total_custo = 0.0
    total_repasse = 0.0

    for lj in lojas:
        df = sales_df[sales_df["loja"] == lj].copy()
        total_vendas += df["valor_venda"].apply(to_float).sum()
        total_lucro += df["lucro_estimado"].apply(to_float).sum()
        total_custo += df["custo_estimado"].apply(to_float).sum()
        total_repasse += df["repasse_colab"].apply(to_float).sum()

    saldos = cash_df[cash_df["loja"].isin(lojas)].copy()
    saldo_total = saldos["saldo"].apply(to_float).sum()

    kpi_row([
        ("Vendas (R$)", f"{total_vendas:,.2f}"),
        ("Lucro estimado (R$)", f"{total_lucro:,.2f}"),
        ("Custo estimado (R$)", f"{total_custo:,.2f}"),
        ("Caixa (R$)", f"{saldo_total:,.2f}"),
    ])

    st.divider()

    # 14 dias
    st.subheader("Vendas (últimos 14 dias)")
    if role == "admin":
        loja_filtro = st.selectbox("Filtrar loja", ["todas"] + ["dx", "nv", "c3", "admin"], index=0)
        loja_sel = None if loja_filtro == "todas" else loja_filtro
    else:
        loja_sel = loja

    _, daily = df_last_14_days(sales_df, loja=loja_sel)
    fig = px.line(daily, x="data_dt", y="valor_venda_num", markers=True)
    fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

def page_lancar_venda(role: str, loja: str, products_df, pricing_df, stock_df, cash_df, sales_df, venda_livre_admin=False):
    st.header("Lançar venda" + (" (Admin — venda livre)" if venda_livre_admin else ""))

    ativos = products_df[products_df["ativo"]].copy()
    prod_ids = ativos["produto_id"].tolist()

    if role == "admin" and venda_livre_admin:
        loja_usar = "admin"
    else:
        loja_usar = loja

    # opções de UI
    labels = {pid: get_product_label(pid, products_df) for pid in prod_ids}

    with st.form("form_venda", clear_on_submit=True):
        produto_id = st.selectbox("Produto", prod_ids, format_func=lambda x: labels.get(x, x))

        tipo = st.selectbox("Tipo", TIPOS_VENDA)  # gramas / pacote
        col1, col2 = st.columns(2)

        gramas = 0.0
        pacotes = 0

        # buscar config do pacote
        maskp = (pricing_df["loja"] == (loja if loja != "admin" else "dx")) & (pricing_df["produto_id"] == produto_id)
        # se admin venda livre, não depende de loja, mas usamos qualquer config para mostrar sugestão (opcional)
        if (pricing_df["loja"] == loja_usar).any():
            maskp = (pricing_df["loja"] == loja_usar) & (pricing_df["produto_id"] == produto_id)

        pacote_preco = 0.0
        pacote_gramas = 0.0
        preco_g = 0.0
        if maskp.any():
            r = pricing_df[maskp].iloc[0]
            pacote_preco = to_float(r["pacote_preco"])
            pacote_gramas = to_float(r["pacote_gramas"])
            preco_g = to_float(r["preco_venda_g"])

        with col1:
            if tipo == "gramas":
                gramas = st.number_input("Gramas", min_value=0.0, value=0.0, step=0.1)
            else:
                pacotes = st.number_input("Pacotes", min_value=0, value=1, step=1)

        with col2:
            st.caption("Sugestão")
            if tipo == "gramas":
                st.write(f"Preço sugerido por g: **R$ {preco_g:.2f}**")
            else:
                st.write(f"Pacote: **R$ {pacote_preco:.2f}** — gramas/pacote: **{pacote_gramas:.2f}g**")

        pagamento = st.selectbox("Pagamento", PAGAMENTOS)
        fiado_nome = ""
        fiado_venc = ""
        if pagamento == "fiado":
            fiado_nome = st.text_input("Nome do fiado")
            fiado_venc_date = st.date_input("Vencimento", value=date.today() + timedelta(days=7))
            fiado_venc = fiado_venc_date.isoformat()

        observacao = st.text_input("Observação (opcional)")

        # valor final sempre manual (evita erro e dá liberdade)
        valor_venda = st.number_input("Valor da venda (R$)", min_value=0.0, value=0.0, step=1.0)

        submitted = st.form_submit_button("Salvar venda")

    if submitted:
        # validações fortes para não corromper CSV
        if produto_id not in prod_ids:
            st.error("Produto inválido.")
            return
        if tipo == "gramas" and gramas <= 0 and valor_venda <= 0:
            st.error("Informe gramas > 0 ou valor > 0.")
            return
        if tipo == "pacote" and pacotes <= 0:
            st.error("Pacotes deve ser > 0.")
            return
        if pagamento == "fiado" and not fiado_nome.strip():
            st.error("Fiado precisa de um nome.")
            return

        # baixa estoque (se for loja de lojista)
        stock2 = stock_df.copy()
        cash2 = cash_df.copy()

        # custo/lucro estimado
        custo, repasse, lucro = compute_sale(
            pricing_df=pricing_df,
            loja=loja if loja in ["dx", "nv", "c3"] else "dx",
            produto_id=produto_id,
            tipo="pacote" if tipo == "pacote" else "gramas",
            gramas=gramas,
            pacotes=pacotes,
            valor=valor_venda,
            role=role if not venda_livre_admin else "admin",
        )

        # atualiza estoque apenas para lojas dx/nv/c3 (admin não tem estoque por padrão)
        if loja_usar in ["dx", "nv", "c3"]:
            if tipo == "gramas":
                update_stock(stock2, loja_usar, produto_id, delta_g=-gramas, delta_pacotes=0)
            else:
                update_stock(stock2, loja_usar, produto_id, delta_g=0.0, delta_pacotes=-pacotes)

        # atualiza caixa
        update_cash(cash2, loja_usar, delta=valor_venda)

        # salva venda
        now = datetime.now()
        rid = f"{int(now.timestamp())}_{normalize_id(st.session_state.get('username','user'))}"
        row = {
            "id": rid,
            "timestamp": now.isoformat(timespec="seconds"),
            "data": date.today().isoformat(),
            "loja": loja_usar,
            "vendedor": st.session_state.get("username", ""),
            "role": "admin" if venda_livre_admin else role,
            "produto_id": produto_id,
            "tipo_venda": "pacote" if tipo == "pacote" else "gramas",
            "gramas": f"{gramas:.3f}" if tipo == "gramas" else "0",
            "pacotes": str(pacotes) if tipo == "pacote" else "0",
            "valor_venda": f"{valor_venda:.2f}",
            "pagamento": pagamento,
            "fiado_nome": fiado_nome,
            "fiado_vencimento": fiado_venc,
            "observacao": observacao,
            "custo_estimado": f"{custo:.2f}",
            "repasse_colab": f"{repasse:.2f}",
            "lucro_estimado": f"{lucro:.2f}",
        }

        append_row_csv(SALES_CSV, SALES_HEADER, row)
        write_df_over_csv(STOCK_CSV, STOCK_HEADER, stock2)
        write_df_over_csv(CASH_CSV, CASH_HEADER, cash2)

        st.success("Venda salva ✅")
        st.rerun()

def page_vendas(role: str, loja: str, products_df, sales_df):
    st.header("Vendas")
    if role == "admin":
        loja_filtro = st.selectbox("Loja", ["dx", "nv", "c3", "admin"], index=0)
        df = sales_df[sales_df["loja"] == loja_filtro].copy()
    else:
        df = sales_df[sales_df["loja"] == loja].copy()

    # rótulos bonitos
    pid_to_label = {p["produto_id"]: get_product_label(p["produto_id"], products_df) for _, p in products_df.iterrows()}
    df["produto"] = df["produto_id"].map(lambda x: pid_to_label.get(x, x))

    df["valor_venda_num"] = df["valor_venda"].apply(to_float)
    df["data_dt"] = df["data"].apply(parse_date_safe)

    st.subheader("Últimos 14 dias")
    df14, daily = df_last_14_days(df, loja=None)  # df já filtrado
    fig = px.bar(daily, x="data_dt", y="valor_venda_num")
    fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Tabela")
    cols = ["timestamp", "data", "vendedor", "produto", "tipo_venda", "gramas", "pacotes", "valor_venda", "pagamento", "fiado_nome", "observacao"]
    st.dataframe(df[cols].sort_values("timestamp", ascending=False), use_container_width=True, height=420)

def page_estoque(role: str, loja: str, products_df, stock_df):
    st.header("Estoque")
    if role == "admin":
        loja_sel = st.selectbox("Loja", ["dx", "nv", "c3"], index=0)
    else:
        loja_sel = loja

    df = stock_df[stock_df["loja"] == loja_sel].copy()
    pid_to_label = {p["produto_id"]: get_product_label(p["produto_id"], products_df) for _, p in products_df.iterrows()}
    df["produto"] = df["produto_id"].map(lambda x: pid_to_label.get(x, x))
    df["estoque_g"] = df["estoque_g"].apply(to_float)
    df["pacotes_prontos"] = df["pacotes_prontos"].apply(to_int)

    st.dataframe(df[["produto", "estoque_g", "pacotes_prontos", "updated_at"]], use_container_width=True)

    if role == "admin":
        st.divider()
        st.subheader("Editar estoque (Admin)")
        with st.form("edit_stock"):
            produto_id = st.selectbox("Produto", products_df[products_df["ativo"]]["produto_id"].tolist(),
                                      format_func=lambda x: pid_to_label.get(x, x))
            novo_estoque_g = st.number_input("Novo estoque (g)", min_value=-999999.0, value=0.0, step=1.0)
            novos_pacotes = st.number_input("Novos pacotes prontos", min_value=-999999, value=0, step=1)
            aplicar = st.form_submit_button("Aplicar")

        if aplicar:
            s2 = stock_df.copy()
            mask = (s2["loja"] == loja_sel) & (s2["produto_id"] == produto_id)
            if mask.any():
                i = s2[mask].index[0]
                s2.at[i, "estoque_g"] = f"{novo_estoque_g:.3f}"
                s2.at[i, "pacotes_prontos"] = str(int(novos_pacotes))
                s2.at[i, "updated_at"] = datetime.now().isoformat(timespec="seconds")
                write_df_over_csv(STOCK_CSV, STOCK_HEADER, s2)
                st.success("Estoque atualizado ✅")
                st.rerun()

def page_precos_admin(products_df, pricing_df):
    st.header("Preços / Custos / Pacotes (Admin)")
    loja_sel = st.selectbox("Loja", ["dx", "nv", "c3"], index=0)

    pid_to_label = {p["produto_id"]: get_product_label(p["produto_id"], products_df) for _, p in products_df.iterrows()}

    df = pricing_df[pricing_df["loja"] == loja_sel].copy()
    df["produto"] = df["produto_id"].map(lambda x: pid_to_label.get(x, x))

    # editar
    st.caption("Edite valores e clique em salvar.")
    edit = df.copy()
    edit["preco_venda_g"] = edit["preco_venda_g"].apply(to_float)
    edit["custo_g"] = edit["custo_g"].apply(to_float)
    edit["pacote_ativo"] = edit["pacote_ativo"].astype(str).str.lower().isin(["true", "1", "yes", "sim"])
    edit["pacote_preco"] = edit["pacote_preco"].apply(to_float)
    edit["pacote_gramas"] = edit["pacote_gramas"].apply(to_float)
    edit["repasse_colab_fixo"] = edit["repasse_colab_fixo"].apply(to_float)

    edited = st.data_editor(
        edit[["produto_id", "produto", "preco_venda_g", "custo_g", "pacote_ativo", "pacote_preco", "pacote_gramas", "repasse_colab_fixo"]],
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        disabled=["produto_id", "produto"],
    )

    if st.button("Salvar alterações"):
        p2 = pricing_df.copy()
        now = datetime.now().isoformat(timespec="seconds")
        for _, row in edited.iterrows():
            pid = row["produto_id"]
            mask = (p2["loja"] == loja_sel) & (p2["produto_id"] == pid)
            if mask.any():
                i = p2[mask].index[0]
                p2.at[i, "preco_venda_g"] = f"{to_float(row['preco_venda_g']):.4f}"
                p2.at[i, "custo_g"] = f"{to_float(row['custo_g']):.4f}"
                p2.at[i, "pacote_ativo"] = "true" if bool(row["pacote_ativo"]) else "false"
                p2.at[i, "pacote_preco"] = f"{to_float(row['pacote_preco']):.2f}"
                p2.at[i, "pacote_gramas"] = f"{to_float(row['pacote_gramas']):.4f}"
                p2.at[i, "repasse_colab_fixo"] = f"{to_float(row['repasse_colab_fixo']):.2f}"
                p2.at[i, "updated_at"] = now
        write_df_over_csv(PRICING_CSV, PRICING_HEADER, p2)
        st.success("Preços atualizados ✅")
        st.rerun()

def page_caixa(role: str, loja: str, cash_df):
    st.header("Caixa")
    if role == "admin":
        loja_sel = st.selectbox("Loja", ["dx", "nv", "c3", "admin"], index=0)
    else:
        loja_sel = loja

    row = cash_df[cash_df["loja"] == loja_sel].copy()
    saldo = to_float(row.iloc[0]["saldo"]) if len(row) else 0.0
    st.metric("Saldo (R$)", f"{saldo:,.2f}")

    if role == "admin":
        st.divider()
        st.subheader("Ajustar caixa (Admin)")
        with st.form("ajuste_caixa"):
            delta = st.number_input("Ajuste (R$) — use negativo para tirar", value=0.0, step=10.0)
            motivo = st.text_input("Motivo (opcional)")
            ok = st.form_submit_button("Aplicar")

        if ok and abs(delta) > 0:
            c2 = cash_df.copy()
            update_cash(c2, loja_sel, delta)
            write_df_over_csv(CASH_CSV, CASH_HEADER, c2)
            # registra uma "venda/lançamento" administrativo como histórico (opcional)
            st.success("Caixa ajustado ✅")
            st.rerun()

def page_relatorios_admin(products_df, sales_df):
    st.header("Relatórios (Admin)")
    df = sales_df.copy()
    df["valor_venda_num"] = df["valor_venda"].apply(to_float)
    df["data_dt"] = df["data"].apply(parse_date_safe)
    df = df[df["data_dt"].notna()].copy()

    pid_to_label = {p["produto_id"]: get_product_label(p["produto_id"], products_df) for _, p in products_df.iterrows()}
    df["produto"] = df["produto_id"].map(lambda x: pid_to_label.get(x, x))

    st.subheader("Por vendedor / produto")
    grp = df.groupby(["loja", "vendedor", "produto", "pagamento"], as_index=False)["valor_venda_num"].sum()
    st.dataframe(grp.sort_values("valor_venda_num", ascending=False), use_container_width=True, height=420)

def page_ai(role: str, loja: str, products_df, sales_df, stock_df, cash_df):
    st.header("IA — Diagnóstico")
    st.caption("Analisa métricas e sugere melhorias (não altera dados).")

    if not ai_enabled():
        st.warning("IA desativada. Configure a variável OPENAI_API_KEY no Windows para habilitar.")
        return

    if role == "admin":
        loja_sel = st.selectbox("Loja para analisar", ["dx", "nv", "c3", "admin"], index=0)
    else:
        loja_sel = loja

    df = sales_df[sales_df["loja"] == loja_sel].copy()
    df["data_dt"] = df["data"].apply(parse_date_safe)
    df = df[df["data_dt"].notna()].copy()

    last30 = date.today() - timedelta(days=29)
    df30 = df[df["data_dt"] >= last30].copy()
    total = df30["valor_venda"].apply(to_float).sum()
    lucro = df30["lucro_estimado"].apply(to_float).sum()
    fiados = df30[df30["pagamento"] == "fiado"]["valor_venda"].apply(to_float).sum()

    prompt = f"""
Você é um analista financeiro. Analise indicadores e aponte riscos e melhorias.
Contexto: painel de vendas/estoque/caixa.
Loja: {loja_sel}
Últimos 30 dias:
- Vendas totais (R$): {total:.2f}
- Lucro estimado (R$): {lucro:.2f}
- Fiado (R$): {fiados:.2f}

Responda em tópicos:
1) Principais alertas
2) Oportunidades de melhoria
3) Próximas ações recomendadas
4) Métricas que devo acompanhar
"""

    if st.button("Rodar diagnóstico IA 🤖"):
        with st.spinner("Analisando..."):
            out = ai_analyze(prompt)
        st.markdown(out)

def page_admin_tools():
    st.header("Admin — Ferramentas")
    st.warning("Cuidado: essas ações podem apagar dados.")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Zerar SOMENTE vendas (sales.csv)"):
            # backup
            if SALES_CSV.exists():
                SALES_CSV.replace(DATA_DIR / f"sales_backup_{int(datetime.now().timestamp())}.csv")
            ensure_csv(SALES_CSV, SALES_HEADER)
            st.success("Vendas zeradas ✅ (backup criado)")
            st.rerun()

    with col2:
        if st.button("ZERAR TUDO (estoque, caixa, vendas)"):
            # backups
            ts = int(datetime.now().timestamp())
            for p in [SALES_CSV, STOCK_CSV, CASH_CSV, PRICING_CSV]:
                if p.exists():
                    p.replace(DATA_DIR / f"{p.stem}_backup_{ts}.csv")
            ensure_csv(SALES_CSV, SALES_HEADER)
            ensure_csv(STOCK_CSV, STOCK_HEADER)
            ensure_csv(CASH_CSV, CASH_HEADER)
            ensure_csv(PRICING_CSV, PRICING_HEADER)
            st.success("Tudo zerado ✅ (backups criados)")
            st.rerun()

# ===================== MAIN =====================
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    init_data()

    # carrega bases
    users_df = load_users()
    products_df = load_products()
    pricing_df = load_pricing()
    stock_df = load_stock()
    cash_df = load_cash()
    sales_df = load_sales()

    # garante linhas mínimas coerentes
    pricing_df = ensure_pricing_rows(pricing_df, products_df)
    stock_df = ensure_stock_rows(stock_df, products_df)
    cash_df = ensure_cash_rows(cash_df)

    # auth
    if not st.session_state.get("auth"):
        do_login(users_df)
        return

    role = st.session_state.get("role", "")
    loja = st.session_state.get("loja", "")

    # sidebar
    st.sidebar.title("Menu")
    st.sidebar.write(f"Usuário: **{st.session_state.get('username','')}**")
    st.sidebar.write(f"Perfil: **{role}**")
    st.sidebar.write(f"Loja: **{loja}**")
    logout_button()
    st.sidebar.divider()

    if role == "admin":
        page = st.sidebar.radio(
            "Páginas",
            [
                "Resumo",
                "Lançar venda (Admin — venda livre)",
                "Vendas",
                "Estoque",
                "Preços/Custos/Pacotes",
                "Caixa",
                "Relatórios",
                "IA",
                "Ferramentas (zerar/backup)",
            ],
        )

        if page == "Resumo":
            page_resumo(role, loja, products_df, pricing_df, stock_df, cash_df, sales_df)
        elif page == "Lançar venda (Admin — venda livre)":
            page_lancar_venda(role, loja, products_df, pricing_df, stock_df, cash_df, sales_df, venda_livre_admin=True)
        elif page == "Vendas":
            page_vendas(role, loja, products_df, sales_df)
        elif page == "Estoque":
            page_estoque(role, loja, products_df, stock_df)
        elif page == "Preços/Custos/Pacotes":
            page_precos_admin(products_df, pricing_df)
        elif page == "Caixa":
            page_caixa(role, loja, cash_df)
        elif page == "Relatórios":
            page_relatorios_admin(products_df, sales_df)
        elif page == "IA":
            page_ai(role, loja, products_df, sales_df, stock_df, cash_df)
        elif page == "Ferramentas (zerar/backup)":
            page_admin_tools()

    else:
        page = st.sidebar.radio(
            "Páginas",
            [
                "Resumo",
                "Lançar venda",
                "Vendas",
                "Estoque",
                "Caixa",
                "IA",
            ],
        )

        if page == "Resumo":
            page_resumo(role, loja, products_df, pricing_df, stock_df, cash_df, sales_df)
        elif page == "Lançar venda":
            page_lancar_venda(role, loja, products_df, pricing_df, stock_df, cash_df, sales_df, venda_livre_admin=False)
        elif page == "Vendas":
            page_vendas(role, loja, products_df, sales_df)
        elif page == "Estoque":
            page_estoque(role, loja, products_df, stock_df)
        elif page == "Caixa":
            page_caixa(role, loja, cash_df)
        elif page == "IA":
            page_ai(role, loja, products_df, sales_df, stock_df, cash_df)

if __name__ == "__main__":
    main()