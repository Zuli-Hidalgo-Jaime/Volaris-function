import logging
import os
import io
import json
import datetime as dt

import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions

DEFAULT_RULES_BLOB = os.getenv("RULES_BLOB", "rules/Reglas.xlsx")
DEFAULT_INPUT_BLOB = os.getenv("DEFAULT_INPUT_BLOB", "inputs/Gastos_Entrada.xlsx")
OUTPUT_CONTAINER = os.getenv("OUTPUT_CONTAINER", "outputs")

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

def _safe_upper(x):
    if pd.isna(x):
        return ""
    return str(x).strip().upper()

def main(req: func.HttpRequest) -> func.HttpResponse:
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

    # --- Download input + rules ---
    in_container, in_blob = _get_container_and_blob(input_blob_path)
    rules_container, rules_blob = _get_container_and_blob(rules_blob_path)

    input_bytes = _download_blob_bytes(bsc, in_container, in_blob)
    rules_bytes = _download_blob_bytes(bsc, rules_container, rules_blob)

    # --- Read Excels ---
    try:
        gastos_df = pd.read_excel(io.BytesIO(input_bytes), sheet_name="Gastos")
    except Exception:
        gastos_df = pd.read_excel(io.BytesIO(input_bytes), sheet_name=0)

    catalog_df = pd.read_excel(io.BytesIO(rules_bytes), sheet_name="Catalogos")

    # Listas permitidas desde Catalogos (por columna)
    allowed_si_no = [x for x in catalog_df.get("SI_NO", pd.Series()).dropna().astype(str).tolist()]
    allowed_monedas = [x for x in catalog_df.get("MONEDAS", pd.Series()).dropna().astype(str).tolist()]
    allowed_metodos = [x for x in catalog_df.get("METODO_PAGO", pd.Series()).dropna().astype(str).tolist()]
    allowed_categorias = [x for x in catalog_df.get("CATEGORIAS", pd.Series()).dropna().astype(str).tolist()]

    allowed_si_no_u = set([_safe_upper(x) for x in allowed_si_no])
    allowed_monedas_u = set([_safe_upper(x) for x in allowed_monedas])
    allowed_metodos_u = set([_safe_upper(x) for x in allowed_metodos])
    allowed_categorias_u = set([str(x).strip() for x in allowed_categorias])

    # Reglas (si existe hoja "Reglas")
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

    # --- Validación fila por fila ---
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
        cat = str(row.get("categoria", "")).strip()
        moneda_u = _safe_upper(row.get("moneda"))
        metodo_u = _safe_upper(row.get("metodo_pago"))
        comp_u = _safe_upper(row.get("tiene_comprobante"))
        monto = row.get("monto", 0) or 0

        motivos = []
        estatus = "APROBADO"

        # Validaciones de catálogo
        if moneda_u and moneda_u not in allowed_monedas_u:
            estatus = "OBSERVADO"
            motivos.append(f"Moneda no permitida: {moneda_u}")

        if metodo_u and metodo_u not in allowed_metodos_u:
            estatus = "OBSERVADO"
            motivos.append(f"Método de pago no permitido: {metodo_u}")

        if comp_u and allowed_si_no_u and comp_u not in allowed_si_no_u:
            estatus = "OBSERVADO"
            motivos.append(f"tiene_comprobante inválido: {comp_u}")

        if cat and allowed_categorias_u and cat not in allowed_categorias_u:
            estatus = "OBSERVADO"
            motivos.append(f"Categoría fuera de catálogo: {cat}")

        # Reglas por categoría
        rule = rules_map.get(cat, {"permitido": "SI", "requiere_comprobante": "SI", "tope_mxn": None})
        permitido = rule.get("permitido", "SI")
        req_comp = rule.get("requiere_comprobante", "SI")
        tope = rule.get("tope_mxn", None)

        out.at[idx, "requiere_comprobante_regla"] = req_comp

        if permitido == "NO":
            estatus = "RECHAZADO"
            motivos.append("Categoría no permitida por política")

        # Comprobante requerido
        if req_comp == "SI":
            if comp_u != "SI":
                estatus = "OBSERVADO" if estatus != "RECHAZADO" else estatus
                motivos.append("Se requiere comprobante")
            if "folio_comprobante" in out.columns:
                folio = str(row.get("folio_comprobante", "")).strip()
                if comp_u == "SI" and not folio:
                    estatus = "OBSERVADO" if estatus != "RECHAZADO" else estatus
                    motivos.append("Falta folio_comprobante")

        # Tope (solo si es numérico)
        try:
            if tope is not None and not pd.isna(tope):
                tope_val = float(tope)
                if float(monto) > tope_val:
                    estatus = "OBSERVADO" if estatus != "RECHAZADO" else estatus
                    out.at[idx, "monto_aprobable"] = tope_val
                    motivos.append(f"Excede tope MXN {tope_val}")
        except Exception:
            pass

        out.at[idx, "estatus"] = estatus
        out.at[idx, "motivo"] = "; ".join(motivos)

        if estatus == "RECHAZADO":
            out.at[idx, "monto_aprobable"] = 0

    # --- Summary ---
    summary = {
        "total_filas": int(len(out)),
        "aprobados": int((out["estatus"] == "APROBADO").sum()),
        "observados": int((out["estatus"] == "OBSERVADO").sum()),
        "rechazados": int((out["estatus"] == "RECHAZADO").sum()),
        "monto_total": float(out["monto"].fillna(0).sum()),
        "monto_aprobable_total": float(out["monto_aprobable"].fillna(0).sum()),
    }

    # --- Write output Excel ---
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_blob_name = f"Gastos_Validado_{ts}.xlsx"

    bio_out = io.BytesIO()
    with pd.ExcelWriter(bio_out, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="Gastos_Validado")

    _upload_blob_bytes(bsc, OUTPUT_CONTAINER, out_blob_name, bio_out.getvalue())
    url = _make_sas_url(conn_str, OUTPUT_CONTAINER, out_blob_name, hours=4)

    return func.HttpResponse(
        json.dumps({"summary": summary, "output_excel_url": url, "output_blob": f"{OUTPUT_CONTAINER}/{out_blob_name}"}),
        status_code=200,
        mimetype="application/json",
    )
