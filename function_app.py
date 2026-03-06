import logging
import os
import io
import json
import datetime as dt

import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from azure.core.exceptions import ResourceNotFoundError

import unicodedata
import re
from difflib import SequenceMatcher

app = func.FunctionApp()

DEFAULT_RULES_BLOB = os.getenv("RULES_BLOB", "rules/Reglas.xlsx")
DEFAULT_INPUT_BLOB = os.getenv("DEFAULT_INPUT_BLOB", "inputs/Gastos_Entrada.xlsx")
OUTPUT_CONTAINER = os.getenv("OUTPUT_CONTAINER", "outputs")
BUDGETS_BLOB = os.getenv("BUDGETS_BLOB", "inputs/Presupuestos.xlsx")
PENDING_EXPENSES = {}


def _parse_conn_str(conn_str: str) -> dict:
    parts = {}
    for kv in conn_str.split(";"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            parts[k] = v
    return parts


def _download_blob_bytes(bsc: BlobServiceClient, container: str, blob_name: str) -> bytes:
    return bsc.get_blob_client(container, blob_name).download_blob().readall()


def _upload_blob_bytes(bsc: BlobServiceClient, container: str, blob_name: str, data: bytes) -> None:
    bc = bsc.get_blob_client(container, blob_name)
    bc.upload_blob(data, overwrite=True)


def _make_sas_url(conn_str: str, container: str, blob_name: str, hours: int = 2) -> str:
    p = _parse_conn_str(conn_str)
    account_name = p.get("AccountName")
    account_key = p.get("AccountKey")

    if not account_name or not account_key:
        return f"https://{account_name}.blob.core.windows.net/{container}/{blob_name}"

    sas = generate_blob_sas(
        account_name=account_name,
        container_name=container,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=dt.datetime.utcnow() + dt.timedelta(hours=hours),
    )
    return f"https://{account_name}.blob.core.windows.net/{container}/{blob_name}?{sas}"


def _get_container_and_blob(path: str) -> tuple[str, str]:
    path = path.lstrip("/")
    container, blob = path.split("/", 1)
    return container, blob



def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def _norm_text(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = _strip_accents(s)
    s = s.upper()
    s = " ".join(s.split())
    return s


def _norm_code(x) -> str:
    s = _norm_text(x)
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s


def _safe_upper(x):
    if pd.isna(x):
        return ""
    return _norm_text(x)




SI_NO_SYNONYMS = {
    "SI": "SI",
    "S": "SI",
    "YES": "SI",
    "Y": "SI",
    "TRUE": "SI",
    "1": "SI",
    "X": "SI",
    "OK": "SI",
    "NO": "NO",
    "N": "NO",
    "FALSE": "NO",
    "0": "NO",
}

METODO_PAGO_SYNONYMS = {
    "TDC": "TDC",
    "TARJETA": "TDC",
    "TARJETADECREDITO": "TDC",
    "TARJETA DE CREDITO": "TDC",
    "CREDITO": "TDC",
    "TC": "TDC",
    "CREDITCARD": "TDC",
    "CREDIT CARD": "TDC",
    "EFECTIVO": "EFECTIVO",
    "CASH": "EFECTIVO",
    "PERSONAL": "PERSONAL",
    "PROPIO": "PERSONAL",
    "MI DINERO": "PERSONAL",
    "PAGO PERSONAL": "PERSONAL",
}

MONEDA_SYNONYMS = {
    "MXN": "MXN",
    "PESO": "MXN",
    "PESOS": "MXN",
    "MN": "MXN",
    "M.N.": "MXN",
    "M.N": "MXN",
    "USD": "USD",
    "DOLAR": "USD",
    "DOLARES": "USD",
    "US": "USD",
    "EUR": "EUR",
    "EURO": "EUR",
    "EUROS": "EUR",
}

CATEGORIA_SYNONYMS = {
    "UBER": "Taxi",
    "DIDI": "Taxi",
    "CABIFY": "Taxi",
    "RIDESHARE": "Taxi",
    "RIDE SHARE": "Taxi",
    "AVION": "Vuelo",
    "AEROLINEA": "Vuelo",
    "AEROLINEAS": "Vuelo",
    "TRANSPORTE": "Transporte público",
    "TRANSPORTE PUBLICO": "Transporte público",
    "METRO": "Transporte público",
    "BUS": "Transporte público",
    "AUTOBUS": "Transporte público",
    "CASETA": "Peajes",
    "CASETAS": "Peajes",
    "PEAJE": "Peajes",
    "PEAJES": "Peajes",
    "PARKING": "Estacionamiento",
    "TELEFONO": "Telefonía/Internet",
    "INTERNET": "Telefonía/Internet",
}


def _canon_si_no(x) -> str:
    u = _safe_upper(x)
    if not u:
        return ""
    return SI_NO_SYNONYMS.get(u, u)


def _canon_moneda(x, allowed_lookup: dict | None = None) -> str:
    u = _norm_code(x)
    if not u:
        return ""
    u = MONEDA_SYNONYMS.get(u, u)
    if allowed_lookup:
        return allowed_lookup.get(u, u)
    return u


def _canon_metodo(x, allowed_lookup: dict | None = None) -> str:
    u = _safe_upper(x)
    if not u:
        return ""
    u_code = _norm_code(u)
    u = METODO_PAGO_SYNONYMS.get(u, METODO_PAGO_SYNONYMS.get(u_code, u))
    if allowed_lookup:
        return allowed_lookup.get(u, u)
    return u


def _canon_categoria(x, allowed_lookup: dict | None = None) -> str:
    raw = "" if pd.isna(x) else str(x).strip()
    u = _safe_upper(x)
    if not raw and not u:
        return ""
    if u in CATEGORIA_SYNONYMS:
        return CATEGORIA_SYNONYMS[u]
    if allowed_lookup and u in allowed_lookup:
        return allowed_lookup[u]
    return raw




@app.route(route="append-expense-row", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def append_expense_row(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("append-expense-row called")

    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(
            json.dumps({"error": "Body must be JSON"}),
            status_code=400,
            mimetype="application/json",
        )

    target_blob_path = body.get("target_blob_path") or DEFAULT_INPUT_BLOB
    sheet_name = body.get("sheet_name") or "Gastos"
    row = body.get("row")

    required_fields = ["fecha", "categoria", "monto", "moneda", "metodo_pago", "tiene_comprobante"]

    if not isinstance(row, dict) or not row:
        return func.HttpResponse(
            json.dumps(
                {
                    "ok": False,
                    "saved_input_blob": target_blob_path,
                    "sheet_name": sheet_name,
                    "appended_row_index": None,
                    "draft_excel_url": None,
                    "missing_fields": required_fields,
                    "message": "Para guardar el gasto necesito: fecha, categoria, monto, moneda, metodo_pago, tiene_comprobante.",
                },
                ensure_ascii=False,
            ),
            status_code=200,
            mimetype="application/json",
        )

    missing = []
    for f in required_fields:
        v = row.get(f, None)
        if v is None:
            missing.append(f)
        elif isinstance(v, str) and not v.strip():
            missing.append(f)

    try:
        monto_val = _parse_money(row.get("monto"))
    except Exception:
        monto_val = None

    if monto_val is None or monto_val <= 0:
        if "monto" not in missing:
            missing.append("monto")

    if missing:
        missing = [f for f in required_fields if f in missing]
        return func.HttpResponse(
            json.dumps(
                {
                    "ok": False,
                    "saved_input_blob": target_blob_path,
                    "sheet_name": sheet_name,
                    "appended_row_index": None,
                    "draft_excel_url": None,
                    "missing_fields": missing,
                    "message": "Para guardar el gasto necesito: " + ", ".join(missing) + ".",
                },
                ensure_ascii=False,
            ),
            status_code=200,
            mimetype="application/json",
        )

    confirm_token = body.get("confirm_token")

    if not confirm_token:
        token = os.urandom(8).hex()
        PENDING_EXPENSES[token] = {
            "target_blob_path": target_blob_path,
            "sheet_name": sheet_name,
            "row": row,
            "created_utc": dt.datetime.utcnow().isoformat(),
        }
        return func.HttpResponse(
            json.dumps(
                {
                    "ok": False,
                    "saved_input_blob": target_blob_path,
                    "sheet_name": sheet_name,
                    "appended_row_index": None,
                    "draft_excel_url": None,
                    "missing_fields": [],
                    "confirm_token": token,
                    "message": f"Para registrar el gasto, responde exactamente: CONFIRMO {token}",
                },
                ensure_ascii=False,
            ),
            status_code=200,
            mimetype="application/json",
        )

    pending = PENDING_EXPENSES.get(confirm_token)
    if not pending:
        return func.HttpResponse(
            json.dumps(
                {
                    "ok": False,
                    "saved_input_blob": target_blob_path,
                    "sheet_name": sheet_name,
                    "appended_row_index": None,
                    "draft_excel_url": None,
                    "missing_fields": [],
                    "confirm_token": None,
                    "message": "Confirmación inválida o expirada. Vuelve a intentar registrar el gasto.",
                },
                ensure_ascii=False,
            ),
            status_code=200,
            mimetype="application/json",
        )

    target_blob_path = pending["target_blob_path"]
    sheet_name = pending["sheet_name"]
    row = pending["row"]
    PENDING_EXPENSES.pop(confirm_token, None)

    conn_str = os.environ.get("AzureWebJobsStorage")
    if not conn_str:
        return func.HttpResponse(
            json.dumps({"error": "Missing AzureWebJobsStorage setting"}),
            status_code=500,
            mimetype="application/json",
        )

    bsc = BlobServiceClient.from_connection_string(conn_str)

    try:
        container, blob_name = _get_container_and_blob(target_blob_path)
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": f"Invalid target_blob_path: {str(e)}"}),
            status_code=400,
            mimetype="application/json",
        )

    base_cols = ["fecha", "categoria", "monto", "moneda", "metodo_pago", "tiene_comprobante"]
    optional_cols = ["folio_comprobante", "descripcion"]

    try:
        existing_bytes = _download_blob_bytes(bsc, container, blob_name)
        try:
            df = pd.read_excel(io.BytesIO(existing_bytes), sheet_name=sheet_name)
        except Exception:
            df = pd.read_excel(io.BytesIO(existing_bytes), sheet_name=0)
    except ResourceNotFoundError:
        df = pd.DataFrame(columns=base_cols + optional_cols)
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": f"Failed to read existing Excel: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
        )

    for c in base_cols + optional_cols:
        if c not in df.columns:
            df[c] = ""

    if "tiene_comprobante" in row:
        row["tiene_comprobante"] = _canon_si_no(row.get("tiene_comprobante"))
    if "metodo_pago" in row:
        row["metodo_pago"] = _canon_metodo(row.get("metodo_pago"))
    if "moneda" in row:
        row["moneda"] = _canon_moneda(row.get("moneda"))
    if "categoria" in row:
        row["categoria"] = _canon_categoria(row.get("categoria"))

    new_row = {c: "" for c in df.columns}
    for k, v in row.items():
        if k in new_row:
            new_row[k] = v

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    appended_index = int(len(df) - 1)

    bio_out = io.BytesIO()
    with pd.ExcelWriter(bio_out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

    try:
        _upload_blob_bytes(bsc, container, blob_name, bio_out.getvalue())
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": f"Failed to upload updated Excel: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
        )

    draft_url = _make_sas_url(conn_str, container, blob_name, hours=4)

    return func.HttpResponse(
        json.dumps(
            {
                "ok": True,
                "saved_input_blob": target_blob_path,
                "sheet_name": sheet_name,
                "appended_row_index": appended_index,
                "draft_excel_url": draft_url,
            }
        ),
        status_code=200,
        mimetype="application/json",
    )

@app.route(route="validate-expenses-excel", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def validate_expenses_excel(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("validate-expenses-excel called")

    try:
        body = req.get_json()
    except Exception:
        body = {}

    input_blob_path = body.get("input_blob_path") or DEFAULT_INPUT_BLOB
    rules_blob_path = body.get("rules_blob_path") or DEFAULT_RULES_BLOB

    conn_str = os.environ.get("AzureWebJobsStorage")
    if not conn_str:
        return func.HttpResponse(
            json.dumps({"error": "Missing AzureWebJobsStorage setting"}),
            status_code=500,
            mimetype="application/json",
        )

    bsc = BlobServiceClient.from_connection_string(conn_str)

    in_container, in_blob = _get_container_and_blob(input_blob_path)
    rules_container, rules_blob = _get_container_and_blob(rules_blob_path)

    try:
        input_bytes = _download_blob_bytes(bsc, in_container, in_blob)
    except ResourceNotFoundError:
        return func.HttpResponse(
            json.dumps({"error": f"Input blob not found: {input_blob_path}"}),
            status_code=404,
            mimetype="application/json",
        )

    try:
        rules_bytes = _download_blob_bytes(bsc, rules_container, rules_blob)
    except ResourceNotFoundError:
        return func.HttpResponse(
            json.dumps({"error": f"Rules blob not found: {rules_blob_path}"}),
            status_code=404,
            mimetype="application/json",
        )

    try:
        gastos_df = pd.read_excel(io.BytesIO(input_bytes), sheet_name="Gastos")
    except Exception:
        gastos_df = pd.read_excel(io.BytesIO(input_bytes), sheet_name=0)

    catalog_df = pd.read_excel(io.BytesIO(rules_bytes), sheet_name="Catalogos")

    allowed_si_no = [x for x in catalog_df.get("SI_NO", pd.Series()).dropna().astype(str).tolist()]
    allowed_monedas = [x for x in catalog_df.get("MONEDAS", pd.Series()).dropna().astype(str).tolist()]
    allowed_metodos = [x for x in catalog_df.get("METODO_PAGO", pd.Series()).dropna().astype(str).tolist()]
    allowed_categorias = [x for x in catalog_df.get("CATEGORIAS", pd.Series()).dropna().astype(str).tolist()]

    allowed_si_no_u = set([_safe_upper(x) for x in allowed_si_no])
    allowed_monedas_u = set([_safe_upper(x) for x in allowed_monedas])
    allowed_metodos_u = set([_safe_upper(x) for x in allowed_metodos])
    allowed_categorias_u = set([str(x).strip() for x in allowed_categorias])

    allowed_si_no_lookup = { _safe_upper(x): str(x).strip() for x in allowed_si_no }
    allowed_monedas_lookup = { _safe_upper(x): str(x).strip() for x in allowed_monedas }
    allowed_metodos_lookup = { _safe_upper(x): str(x).strip() for x in allowed_metodos }
    allowed_categorias_lookup = { _safe_upper(x): str(x).strip() for x in allowed_categorias }

    rules_map = {}
    try:
        reglas_df = pd.read_excel(io.BytesIO(rules_bytes), sheet_name="Reglas")
        for _, r in reglas_df.iterrows():
            cat = str(r.get("categoria", "")).strip()
            if not cat:
                continue
            rules_map[cat] = {
                "permitido": _safe_upper(r.get("permitido", "SI")) or "SI",
                "requiere_comprobante": _safe_upper(r.get("requiere_comprobante", "SI")) or "SI",
                "tope_mxn": r.get("tope_mxn", None),
            }
    except Exception:
        rules_map = {
            "Propinas": {"permitido": "NO", "requiere_comprobante": "NO", "tope_mxn": 0},
            "Gimnasio/Spa": {"permitido": "NO", "requiere_comprobante": "NO", "tope_mxn": 0},
            "Comida": {"permitido": "SI", "requiere_comprobante": "SI", "tope_mxn": 500},
            "Hotel": {"permitido": "SI", "requiere_comprobante": "SI", "tope_mxn": 2500},
            "Taxi": {"permitido": "SI", "requiere_comprobante": "SI", "tope_mxn": 600},
        }

    required_cols = ["fecha", "categoria", "monto", "moneda", "metodo_pago", "tiene_comprobante"]
    for c in required_cols:
        if c not in gastos_df.columns:
            return func.HttpResponse(
                json.dumps({"error": f"Missing column in input Excel: {c}"}),
                status_code=400,
                mimetype="application/json",
            )

    out = gastos_df.copy()
    out["estatus"] = ""
    out["motivo"] = ""
    out["monto_aprobable"] = out["monto"]
    out["requiere_comprobante_regla"] = ""

    for idx, row in out.iterrows():
        cat = _canon_categoria(row.get("categoria", ""), allowed_lookup=allowed_categorias_lookup)
        moneda_u = _canon_moneda(row.get("moneda"), allowed_lookup=allowed_monedas_lookup)
        metodo_u = _canon_metodo(row.get("metodo_pago"), allowed_lookup=allowed_metodos_lookup)
        comp_u = _canon_si_no(row.get("tiene_comprobante"))
        monto = row.get("monto", 0) or 0

        if "categoria" in out.columns:
            out.at[idx, "categoria"] = cat
        if "moneda" in out.columns:
            out.at[idx, "moneda"] = moneda_u
        if "metodo_pago" in out.columns:
            out.at[idx, "metodo_pago"] = metodo_u
        if "tiene_comprobante" in out.columns:
            out.at[idx, "tiene_comprobante"] = comp_u

        motivos = []
        estatus = "APROBADO"

        if moneda_u and allowed_monedas_u and _safe_upper(moneda_u) not in allowed_monedas_u:
            estatus = "OBSERVADO"
            motivos.append(f"Moneda no permitida: {moneda_u}")

        if metodo_u and allowed_metodos_u and _safe_upper(metodo_u) not in allowed_metodos_u:
            estatus = "OBSERVADO"
            motivos.append(f"Método de pago no permitido: {metodo_u}")

        if comp_u and allowed_si_no_u and _safe_upper(comp_u) not in allowed_si_no_u:
            estatus = "OBSERVADO"
            motivos.append(f"tiene_comprobante inválido: {comp_u}")

        
        if cat and allowed_categorias_lookup and _safe_upper(cat) not in allowed_categorias_lookup:
            estatus = "OBSERVADO"
            motivos.append(f"Categoría fuera de catálogo: {cat}")

        rule = rules_map.get(cat, {"permitido": "SI", "requiere_comprobante": "SI", "tope_mxn": None})
        permitido = rule.get("permitido", "SI")
        req_comp = rule.get("requiere_comprobante", "SI")
        tope = rule.get("tope_mxn", None)

        out.at[idx, "requiere_comprobante_regla"] = req_comp

        if permitido == "NO":
            estatus = "RECHAZADO"
            motivos.append("Categoría no permitida por política")

        if req_comp == "SI":
            if comp_u != "SI":
                if estatus != "RECHAZADO":
                    estatus = "OBSERVADO"
                motivos.append("Se requiere comprobante")

            if "folio_comprobante" in out.columns:
                folio = str(row.get("folio_comprobante", "")).strip()
                if comp_u == "SI" and not folio:
                    if estatus != "RECHAZADO":
                        estatus = "OBSERVADO"
                    motivos.append("Falta folio_comprobante")

        try:
            if tope is not None and not pd.isna(tope):
                tope_val = float(tope)
                if float(monto) > tope_val:
                    if estatus != "RECHAZADO":
                        estatus = "OBSERVADO"
                    out.at[idx, "monto_aprobable"] = tope_val
                    motivos.append(f"Excede tope MXN {tope_val}")
        except Exception:
            pass

        out.at[idx, "estatus"] = estatus
        out.at[idx, "motivo"] = "; ".join(motivos)

        if estatus == "RECHAZADO":
            out.at[idx, "monto_aprobable"] = 0

    summary = {
        "total_filas": int(len(out)),
        "aprobados": int((out["estatus"] == "APROBADO").sum()),
        "observados": int((out["estatus"] == "OBSERVADO").sum()),
        "rechazados": int((out["estatus"] == "RECHAZADO").sum()),
        "monto_total": float(out["monto"].fillna(0).sum()),
        "monto_aprobable_total": float(out["monto_aprobable"].fillna(0).sum()),
    }

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_blob_name = f"Gastos_Validado_{ts}.xlsx"

    bio_out = io.BytesIO()
    with pd.ExcelWriter(bio_out, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="Gastos_Validado")

    try:
        _upload_blob_bytes(bsc, OUTPUT_CONTAINER, out_blob_name, bio_out.getvalue())
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": f"Failed to upload output Excel: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
        )

    url = _make_sas_url(conn_str, OUTPUT_CONTAINER, out_blob_name, hours=4)

    return func.HttpResponse(
        json.dumps({"summary": summary, "output_excel_url": url, "output_blob": f"{OUTPUT_CONTAINER}/{out_blob_name}"}),
        status_code=200,
        mimetype="application/json",
    )


def _norm_text_ci(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = _strip_accents(s)
    s = re.sub(r"\s+", " ", s)
    return s


def _norm_name_key(s: str) -> str:
    if s is None:
        return ""
    s = _norm_text_ci(s)
    s = re.sub(r"[^a-z0-9\\s]", " ", s)
    toks = [t for t in s.split() if t not in {"el","la","los","las","de","del","proyecto"}]
    toks = [t[:-1] if t.endswith("s") and len(t) > 3 else t for t in toks]
    return " ".join(toks)


def _parse_money(x):
    if x is None:
        return None
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def _extract_numeric_key(x):
    if x is None:
        return None
    s = str(x)
    digits = re.findall(r"\d+", s)
    if not digits:
        return None
    try:
        return str(int("".join(digits)))
    except Exception:
        return None


@app.route(route="check_project_budget", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def check_project_budget(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("check_project_budget called")

    project_key = req.params.get("project_key")
    project_name = req.params.get("project_name")
    amount = req.params.get("amount")
    budgets_blob_path = req.params.get("budgets_blob_path")

    if req.method == "POST":
        try:
            body = req.get_json()
        except Exception:
            body = {}
        project_key = body.get("project_key", project_key)
        project_name = body.get("project_name", project_name)
        amount = body.get("amount", amount)
        budgets_blob_path = body.get("budgets_blob_path", budgets_blob_path)

    budgets_blob_path = budgets_blob_path or os.getenv("BUDGETS_BLOB", "inputs/Presupuestos.xlsx")
    amount_val = _parse_money(amount) if amount is not None else None

    if not project_key and not project_name:
        return func.HttpResponse(
            json.dumps(
                {
                    "ok": False,
                    "found": False,
                    "project_key": None,
                    "project_name": None,
                    "budget": None,
                    "amount": amount_val,
                    "is_sufficient": None,
                    "encargado": None,
                    "message": "Falta project_key o project_name para validar el presupuesto.",
                },
                ensure_ascii=False,
            ),
            status_code=200,
            mimetype="application/json",
        )

    conn_str = os.environ.get("AzureWebJobsStorage")
    if not conn_str:
        return func.HttpResponse(
            json.dumps(
                {
                    "found": False,
                    "project_key": None,
                    "project_name": None,
                    "budget": None,
                    "amount": amount_val,
                    "is_sufficient": None,
                    "encargado": None,
                    "message": "Missing AzureWebJobsStorage setting",
                },
                ensure_ascii=False,
            ),
            status_code=500,
            mimetype="application/json",
        )

    bsc = BlobServiceClient.from_connection_string(conn_str)

    try:
        container, blob = _get_container_and_blob(budgets_blob_path)
        xbytes = _download_blob_bytes(bsc, container, blob)
    except Exception:
        logging.exception("No se pudo descargar Presupuestos.xlsx")
        return func.HttpResponse(
            json.dumps(
                {
                    "found": False,
                    "project_key": None,
                    "project_name": None,
                    "budget": None,
                    "amount": amount_val,
                    "is_sufficient": None,
                    "encargado": None,
                    "message": f"No se encontró el archivo de presupuestos: {budgets_blob_path}",
                },
                ensure_ascii=False,
            ),
            status_code=200,
            mimetype="application/json",
        )

    logging.info(f"budget_path={budgets_blob_path} bytes={len(xbytes)} first4={xbytes[:4]}")
    try:
        df = pd.read_excel(io.BytesIO(xbytes), sheet_name=0, engine="openpyxl")
    except Exception:
        logging.exception("No se pudo leer el Excel de presupuestos")
        return func.HttpResponse(
            json.dumps(
                {
                    "found": False,
                    "project_key": None,
                    "project_name": None,
                    "budget": None,
                    "amount": amount_val,
                    "is_sufficient": None,
                    "encargado": None,
                    "message": "El archivo de presupuestos no se pudo leer como Excel.",
                },
                ensure_ascii=False,
            ),
            status_code=200,
            mimetype="application/json",
        )

    required_cols = {"Clave proyecto", "Nombre del proyecto", "Presupuesto", "Encargado"}
    missing = required_cols - set(df.columns)
    if missing:
        return func.HttpResponse(
            json.dumps(
                {
                    "found": False,
                    "project_key": None,
                    "project_name": None,
                    "budget": None,
                    "amount": amount_val,
                    "is_sufficient": None,
                    "encargado": None,
                    "message": f"Faltan columnas en el Excel de presupuestos: {sorted(list(missing))}",
                },
                ensure_ascii=False,
            ),
            status_code=200,
            mimetype="application/json",
        )

    match_row = None

    if project_key:
        target_key_num = _extract_numeric_key(project_key)
        if target_key_num is not None:
            df["_norm_key_num"] = df["Clave proyecto"].apply(_extract_numeric_key)
            df_key = df[df["_norm_key_num"] == target_key_num]
        else:
            target_key = _norm_text_ci(project_key)
            df["_norm_key"] = df["Clave proyecto"].apply(_norm_text_ci)
            df_key = df[df["_norm_key"] == target_key]
        if not df_key.empty:
            match_row = df_key.iloc[0]

    if match_row is None and project_name:
        target = _norm_name_key(project_name)
        df["_norm_name_key"] = df["Nombre del proyecto"].apply(_norm_name_key)

        df_name = df[df["_norm_name_key"] == target]

        if df_name.empty and target:
            df_name = df[df["_norm_name_key"].str.contains(target, na=False)]
        if df_name.empty and target:
            df_name = df[df["_norm_name_key"].apply(lambda n: (n in target) or (target in n))]

        if df_name.empty and target:
            scores = df["_norm_name_key"].apply(lambda n: SequenceMatcher(None, target, n).ratio())
            best_idx = scores.idxmax()
            best_score = float(scores.loc[best_idx])

            if best_score >= 0.82:
                match_row = df.loc[best_idx]
            else:
                top_idx = scores.sort_values(ascending=False).head(3).index
                suggestions = df.loc[top_idx, "Nombre del proyecto"].astype(str).tolist()
                return func.HttpResponse(
                    json.dumps(
                        {
                            "ok": False,
                            "found": False,
                            "project_key": None,
                            "project_name": None,
                            "budget": None,
                            "amount": amount_val,
                            "is_sufficient": None,
                            "encargado": None,
                            "message": "No encontré el proyecto. ¿Quizá quisiste decir: "
                                       + "; ".join(suggestions)
                                       + "?",
                        },
                        ensure_ascii=False,
                    ),
                    status_code=200,
                    mimetype="application/json",
                )
        else:
            if not df_name.empty:
                match_row = df_name.iloc[0]

    if match_row is None:
        return func.HttpResponse(
            json.dumps(
                {
                    "found": False,
                    "project_key": None,
                    "project_name": None,
                    "budget": None,
                    "amount": amount_val,
                    "is_sufficient": None,
                    "encargado": None,
                    "message": "No encontré el proyecto en el archivo de presupuestos. Verifica clave o nombre.",
                },
                ensure_ascii=False,
            ),
            status_code=200,
            mimetype="application/json",
        )

    proj_key = str(match_row["Clave proyecto"]).strip()
    proj_name = str(match_row["Nombre del proyecto"]).strip()
    encargado = str(match_row.get("Encargado", "")).strip()
    budget_val = _parse_money(match_row["Presupuesto"])

    if budget_val is None:
        return func.HttpResponse(
            json.dumps(
                {
                    "found": True,
                    "project_key": proj_key,
                    "project_name": proj_name,
                    "budget": None,
                    "amount": amount_val,
                    "is_sufficient": None,
                    "encargado": encargado,
                    "message": f"Encontré el proyecto {proj_name}, pero el campo Presupuesto no es numérico/legible.",
                },
                ensure_ascii=False,
            ),
            status_code=200,
            mimetype="application/json",
        )

    if amount_val is None:
        ok = budget_val > 0

        if ok:
            msg = (
                f"El proyecto {proj_name} tiene presupuesto suficiente para hacer cargos. "
                f"Presupuesto disponible: ${budget_val:,.0f}."
            )
        else:
            contacto = f"Favor de contactar a {encargado}." if encargado else "Favor de contactar al jefe de proyectos."
            msg = (
                f"El proyecto {proj_name} no cuenta con presupuesto suficiente. "
                f"Presupuesto disponible: ${budget_val:,.0f}. {contacto}"
            )

        resp = {
            "found": True,
            "project_key": proj_key,
            "project_name": proj_name,
            "budget": budget_val,
            "amount": None,
            "is_sufficient": ok,
            "encargado": encargado,
            "message": msg,
        }
    else:
        ok = (budget_val > 0) and (amount_val <= budget_val)
        if ok:
            msg = (
                f"El proyecto {proj_name} tiene presupuesto suficiente para hacer cargos por "
                f"${amount_val:,.0f}. Presupuesto disponible: ${budget_val:,.0f}."
            )
        else:
            contacto = f"Favor de contactar a {encargado}." if encargado else "Favor de contactar al jefe de proyectos."
            msg = (
                f"El proyecto {proj_name} no cuenta con presupuesto suficiente para un cargo por "
                f"${amount_val:,.0f}. Presupuesto disponible: ${budget_val:,.0f}. {contacto}"
            )

        resp = {
            "found": True,
            "project_key": proj_key,
            "project_name": proj_name,
            "budget": budget_val,
            "amount": amount_val,
            "is_sufficient": ok,
            "encargado": encargado,
            "message": msg,
        }

    return func.HttpResponse(
        json.dumps(resp, ensure_ascii=False),
        status_code=200,
        mimetype="application/json",
    )
