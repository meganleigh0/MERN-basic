"""
ClauseBot+ Starter EDA
- Loads everything in your contracts data folder
- Profiles Excel outputs (Results, Guidance, FAR/DFARS database)
- Extracts text from PDF contract + Word user guide
- Builds a first-pass “document chunks” table you can later use for diff/search/risk

Run this in a notebook cell. Update DATA_DIR to your folder.
"""

from __future__ import annotations

import re
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import pandas as pd

# ----------------------------
# 0) CONFIG
# ----------------------------
DATA_DIR = Path(r"./CONTRACTS/data")  # <-- CHANGE THIS (e.g., r"C:\...\CONTRACTS\data")
OUTPUT_DIR = DATA_DIR / "_eda_outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 140)

# ----------------------------
# 1) UTILITIES
# ----------------------------
def sniff_files(data_dir: Path) -> Dict[str, List[Path]]:
    exts = {
        "excel": [".xlsx", ".xlsm", ".xls"],
        "pdf": [".pdf"],
        "word": [".docx"],
        "other": []
    }
    buckets = {k: [] for k in exts}
    for p in data_dir.glob("*"):
        if not p.is_file():
            continue
        suffix = p.suffix.lower()
        matched = False
        for k, suf_list in exts.items():
            if suffix in suf_list:
                buckets[k].append(p)
                matched = True
                break
        if not matched:
            buckets["other"].append(p)
    return buckets

def safe_read_excel_all_sheets(xlsx_path: Path) -> Dict[str, pd.DataFrame]:
    """Read all sheets, lightly clean column names, return dict[sheet_name] = df."""
    xls = pd.ExcelFile(xlsx_path)
    out = {}
    for sh in xls.sheet_names:
        df = pd.read_excel(xlsx_path, sheet_name=sh)
        df.columns = [str(c).strip() for c in df.columns]
        out[sh] = df
    return out

def profile_df(df: pd.DataFrame, name: str) -> Dict:
    """Return a compact profile for quick EDA notes."""
    prof = {
        "name": name,
        "shape": df.shape,
        "columns": list(df.columns),
        "dtypes": {c: str(df[c].dtype) for c in df.columns},
        "nulls": df.isna().sum().sort_values(ascending=False).head(15).to_dict(),
        "nunique": df.nunique(dropna=True).sort_values(ascending=False).head(15).to_dict(),
        "sample_rows": df.head(5).to_dict(orient="records"),
    }
    return prof

def save_profile(profile: Dict, out_path: Path) -> None:
    out_path.write_text(json.dumps(profile, indent=2, default=str), encoding="utf-8")

def extract_text_pdf(pdf_path: Path, max_pages: Optional[int] = None) -> str:
    """
    Try to extract PDF text. If it fails, returns empty string (scanned PDFs may need OCR).
    Preferred: pdfplumber. Fallback: pypdf.
    """
    text_parts = []
    # Attempt pdfplumber
    try:
        import pdfplumber  # pip install pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            pages = pdf.pages[:max_pages] if max_pages else pdf.pages
            for i, page in enumerate(pages):
                t = page.extract_text() or ""
                if t.strip():
                    text_parts.append(t)
        return "\n\n".join(text_parts).strip()
    except Exception:
        pass

    # Fallback: pypdf
    try:
        from pypdf import PdfReader  # pip install pypdf
        reader = PdfReader(str(pdf_path))
        pages = reader.pages[:max_pages] if max_pages else reader.pages
        for page in pages:
            t = page.extract_text() or ""
            if t.strip():
                text_parts.append(t)
        return "\n\n".join(text_parts).strip()
    except Exception:
        return ""

def extract_text_docx(docx_path: Path) -> str:
    """Extract text from .docx using python-docx."""
    try:
        from docx import Document  # pip install python-docx
        doc = Document(str(docx_path))
        paras = [p.text for p in doc.paragraphs if p.text and p.text.strip()]
        return "\n".join(paras).strip()
    except Exception:
        return ""

def normalize_whitespace(s: str) -> str:
    s = s.replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def chunk_text(text: str, chunk_chars: int = 2000, overlap: int = 250) -> List[str]:
    """
    Simple chunker for early prototyping.
    Later you can replace with:
      - section/clause-based chunking
      - sentence-based chunking
    """
    text = normalize_whitespace(text)
    if not text:
        return []
    chunks = []
    i = 0
    n = len(text)
    while i < n:
        j = min(i + chunk_chars, n)
        chunks.append(text[i:j])
        i = j - overlap
        if i < 0:
            i = 0
        if i >= n:
            break
    return chunks

def find_clause_ids(text: str) -> List[str]:
    """
    Very loose FAR/DFARS clause ID pattern finder.
    Your ClauseBot results will be more authoritative; this is just exploratory.
    """
    if not text:
        return []
    pat = re.compile(r"\b(?:FAR|DFARS)?\s*\d{2,3}\.\d{1,4}-\d{1,4}\b", re.IGNORECASE)
    hits = pat.findall(text)
    # normalize
    cleaned = []
    for h in hits:
        h2 = re.sub(r"\s+", "", h.upper())
        cleaned.append(h2)
    return sorted(set(cleaned))

# ----------------------------
# 2) DISCOVER FILES
# ----------------------------
buckets = sniff_files(DATA_DIR)
print("Found files:")
for k, files in buckets.items():
    print(f"- {k}: {len(files)}")
    for f in files:
        print(f"    {f.name}")

# ----------------------------
# 3) LOAD & PROFILE EXCEL FILES
# ----------------------------
excel_books: Dict[str, Dict[str, pd.DataFrame]] = {}
profiles: List[Dict] = []

for xlsx in buckets["excel"]:
    print(f"\nLoading Excel: {xlsx.name}")
    sheets = safe_read_excel_all_sheets(xlsx)
    excel_books[xlsx.name] = sheets

    for sh, df in sheets.items():
        nm = f"{xlsx.name} :: {sh}"
        prof = profile_df(df, nm)
        profiles.append(prof)
        # save profile json
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", nm)[:180]
        save_profile(prof, OUTPUT_DIR / f"profile__{safe_name}.json")

print(f"\nSaved {len(profiles)} dataframe profile(s) to: {OUTPUT_DIR}")

# Quick peek: list sheets + shapes
print("\nExcel workbook summary:")
for book, sheets in excel_books.items():
    print(f"\n{book}")
    for sh, df in sheets.items():
        print(f"  - {sh}: shape={df.shape}")

# ----------------------------
# 4) EXTRACT TEXT FROM PDF CONTRACT + WORD USER GUIDE
# ----------------------------
pdf_texts = {}
for pdf in buckets["pdf"]:
    print(f"\nExtracting PDF text: {pdf.name}")
    txt = extract_text_pdf(pdf, max_pages=None)
    pdf_texts[pdf.name] = txt
    out_txt = OUTPUT_DIR / f"{pdf.stem}__extracted.txt"
    out_txt.write_text(txt, encoding="utf-8")
    print(f"  Extracted chars: {len(txt):,} (saved to {out_txt.name})")
    if len(txt) < 1000:
        print("  NOTE: Very little text extracted. This PDF might be scanned (needs OCR).")

docx_texts = {}
for docx in buckets["word"]:
    print(f"\nExtracting DOCX text: {docx.name}")
    txt = extract_text_docx(docx)
    docx_texts[docx.name] = txt
    out_txt = OUTPUT_DIR / f"{docx.stem}__extracted.txt"
    out_txt.write_text(txt, encoding="utf-8")
    print(f"  Extracted chars: {len(txt):,} (saved to {out_txt.name})")

# ----------------------------
# 5) BUILD A FIRST "DOCUMENT CHUNKS" TABLE (FOR SEARCH/DIFF LATER)
# ----------------------------
rows = []

def add_doc_chunks(doc_name: str, raw_text: str, source_type: str):
    chunks = chunk_text(raw_text, chunk_chars=2000, overlap=250)
    clause_ids = find_clause_ids(raw_text)
    for idx, ch in enumerate(chunks):
        rows.append({
            "doc_name": doc_name,
            "source_type": source_type,
            "chunk_index": idx,
            "chunk_text": ch,
            "chunk_len": len(ch),
        })
    return {
        "doc_name": doc_name,
        "source_type": source_type,
        "n_chunks": len(chunks),
        "total_chars": len(raw_text),
        "approx_clause_ids_found": clause_ids[:30],  # show first 30
        "approx_clause_id_count": len(clause_ids),
    }

doc_summaries = []
for name, txt in pdf_texts.items():
    doc_summaries.append(add_doc_chunks(name, txt, "pdf"))
for name, txt in docx_texts.items():
    doc_summaries.append(add_doc_chunks(name, txt, "docx"))

chunks_df = pd.DataFrame(rows)
summ_df = pd.DataFrame(doc_summaries)

print("\nDocument summary:")
display(summ_df)

print("\nChunks preview:")
display(chunks_df.head(10))

# Save chunks to parquet/csv for next steps
chunks_out = OUTPUT_DIR / "document_chunks.parquet"
chunks_df.to_parquet(chunks_out, index=False)
summ_out = OUTPUT_DIR / "document_summaries.csv"
summ_df.to_csv(summ_out, index=False)
print(f"\nSaved:\n- {chunks_out}\n- {summ_out}")

# ----------------------------
# 6) OPTIONAL: IDENTIFY KEY COLUMNS IN CLAUSEBOT OUTPUTS
# ----------------------------
def guess_clausebot_tables(excel_books: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, List[Tuple[str, str]]]:
    """
    Heuristic: find tables likely containing 'clause' and 'dfars/far' references.
    Returns dict[workbook] = list[(sheet_name, reason)]
    """
    hits = {}
    for book, sheets in excel_books.items():
        for sh, df in sheets.items():
            cols = " ".join([c.lower() for c in df.columns])
            reason = []
            if "clause" in cols:
                reason.append("has 'clause' column(s)")
            if "far" in cols or "dfars" in cols:
                reason.append("mentions FAR/DFARS")
            if "guidance" in cols or "action" in cols or "recommend" in cols:
                reason.append("looks like guidance/actions")
            if reason:
                hits.setdefault(book, []).append((sh, "; ".join(reason)))
    return hits

print("\nPossible ClauseBot-relevant sheets (heuristic):")
hit_map = guess_clausebot_tables(excel_books)
for book, items in hit_map.items():
    print(f"\n{book}")
    for sh, why in items:
        print(f"  - {sh}: {why}")

# ----------------------------
# 7) NEXT STEP SUGGESTION (WHAT TO DO AFTER EDA)
# ----------------------------
print(
    "\nNext steps after this EDA:\n"
    "1) Confirm which Excel sheet is the authoritative ClauseBot Results table (clauses detected + locations).\n"
    "2) Build a normalized 'clauses_detected' table: contract_id, clause_id, page/section, detected_text, confidence (if any).\n"
    "3) Link clause_id to the FAR/DFARS database + guidance.\n"
    "4) For contract comparison: align by clause_id, then add semantic similarity on detected_text/section text.\n"
    "5) For risk search: build a risk taxonomy and run keyword + semantic retrieval over document_chunks.\n"
)