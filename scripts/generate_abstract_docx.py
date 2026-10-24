# scripts/generate_abstract_docx.py
"""
Generate VortexStream 2-Page Project Abstract Word Document (.docx)
- Page 1: Topic, Authors, Project Overview/Abstract, and Technologies Used (Stack Table).
- Page 2: System Architecture & Data Flow Diagram.
- Formatted with BOLD, high-contrast, pure black typography for dark, crisp printouts.
"""

from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml import parse_xml
from docx.oxml.ns import nsdecls

def generate_diagram(output_path: str):
    """Generate high-resolution, bold, high-contrast architecture diagram for Page 2."""
    fig, ax = plt.subplots(figsize=(10, 13.5), dpi=300)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 135)
    ax.axis("off")

    def draw_box(x, y, w, h, title, lines, bg_color="#F8FAFC", border_color="#0F172A", title_size=10.5, body_size=8.5):
        box = patches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.8,rounding_size=1.2",
            facecolor=bg_color,
            edgecolor=border_color,
            linewidth=2.0,  # Bold border for printing
            zorder=2
        )
        ax.add_patch(box)
        
        # Bold title
        ax.text(
            x + w / 2, y + h - 2.6, title,
            ha="center", va="center",
            fontsize=title_size, fontweight="bold",
            color="#000000", zorder=3
        )
        
        # Bold lines
        if lines:
            line_text = "\n".join(lines)
            ax.text(
                x + w / 2, y + (h - 2.6) / 2, line_text,
                ha="center", va="center",
                fontsize=body_size, fontweight="bold",
                color="#000000",
                linespacing=1.35, zorder=3
            )

    def draw_arrow(x1, y1, x2, y2, color="#000000"):
        ax.annotate(
            "", xy=(x2, y2), xytext=(x1, y1),
            arrowprops=dict(
                arrowstyle="-|>",
                color=color,
                lw=2.2,  # Bold arrow for printing
                mutation_scale=16,
            ),
            zorder=1
        )

    # 1. Top Source Box
    draw_box(20, 120, 60, 11, "Real-World OTT Video Telemetry", [
        "Stanford Puffer Dataset (High-Resolution Buffer Oscillations & Stalls)"
    ], bg_color="#FEF3C7", border_color="#B45309", title_size=11, body_size=9)

    # Path Headers
    draw_box(6, 106, 42, 7.5, "Real-Time Streaming Path", [], bg_color="#E0F2FE", border_color="#0369A1", title_size=11)
    draw_box(52, 106, 42, 7.5, "Analytical Lakehouse Path", [], bg_color="#EDE9FE", border_color="#6D28D9", title_size=11)

    # Arrows from top to headers
    draw_arrow(40, 120, 27, 113.5)
    draw_arrow(60, 120, 73, 113.5)

    # --- LEFT PATH (STREAMING) ---
    # 1. Kafka producer
    draw_box(6, 91, 42, 11, "1. Kafka Telemetry Producer", [
        "Replays video client buffer, network metrics,",
        "and playback error events as live streams."
    ], bg_color="#F0FDFA", border_color="#0F766E")
    draw_arrow(27, 106, 27, 102)

    # 2. Kafka message broker
    draw_box(6, 75, 42, 11.5, "2. Kafka Message Broker", [
        "(Topics: playback-events, network-metrics, errors, alerts)",
        "Decouples and buffers high-velocity telemetry."
    ], bg_color="#F0FDFA", border_color="#0F766E")
    draw_arrow(27, 91, 27, 86.5)

    # 3. PySpark structured streaming
    draw_box(6, 48, 42, 22, "3. PySpark Structured Streaming", [
        "• Cleans & validates incoming telemetry",
        "• 15-minute watermark deduplication",
        "• Stateful session lifecycle tracking (Q1)",
        "• 5-minute multi-grain QoE aggregation (Q2)",
        "• 2-window hysteresis alert engine (R1-R4)"
    ], bg_color="#F0FDFA", border_color="#0F766E", title_size=10.5, body_size=8.5)
    draw_arrow(27, 75, 27, 70)

    # Sinks (DLQ, Cold Storage, HBase)
    draw_box(6, 28, 12.5, 14, "Dead-Letter\nQueue", ["Corrupted\nevents sent\nto DLQ."], bg_color="#FFE4E6", border_color="#BE123C")
    draw_box(20.5, 28, 13, 14, "Cold Lake\nStorage", ["Clean Parquet\npartitions\non disk."], bg_color="#F3E8FF", border_color="#7E22CE")
    draw_box(35.5, 28, 12.5, 14, "HBase NoSQL\nCluster", ["Sub-ms state,\nmetrics, &\nlive alerts."], bg_color="#DCFCE7", border_color="#15803D")

    # Arrows to sinks
    draw_arrow(18, 48, 12.5, 42)
    draw_arrow(27, 48, 27, 42)
    draw_arrow(36, 48, 41.5, 42)

    # --- RIGHT PATH (LAKEHOUSE & ML) ---
    # 1. PySpark batch ETL
    draw_box(52, 91, 42, 11, "1. PySpark Batch ETL", [
        "Cleans raw telemetry, harmonizes metadata,",
        "and computes ITU-T composite QoE scores."
    ], bg_color="#F5F3FF", border_color="#5B21B6")
    draw_arrow(73, 106, 73, 102)

    # 2. Partitioned Parquet lake
    draw_box(52, 75, 42, 11.5, "2. Partitioned Parquet Lake", [
        "Multi-million row historical dataset saved in",
        "Snappy-compressed columnar files on disk."
    ], bg_color="#F5F3FF", border_color="#5B21B6")
    draw_arrow(73, 91, 73, 86.5)

    # 3. DuckDB warehouse & 4. ML abandonment
    draw_box(52, 50, 20, 19.5, "3. DuckDB\nWarehouse", [
        "Ultra-fast zero-copy\nOLAP queries\nacross full\nParquet lake."
    ], bg_color="#EFF6FF", border_color="#1D4ED8")
    draw_box(74, 50, 20, 19.5, "4. ML Churn\nModel", [
        "Trains XGBoost &\nMLlib models on\nearly-15s stalls to\npredict churn."
    ], bg_color="#FAF5FF", border_color="#6B21A8")

    draw_arrow(67, 75, 62, 69.5)
    draw_arrow(79, 75, 84, 69.5)

    # Reports and ML forecast scores
    draw_box(52, 28, 20, 14, "8 SQL Reports", ["QoE by City/ISP,\nCDN latency,\nbitrate ladder."], bg_color="#EFF6FF", border_color="#1D4ED8")
    draw_box(74, 28, 20, 14, "Churn Scores", ["Abandonment\nprobabilities\nsaved to gold."], bg_color="#FAF5FF", border_color="#6B21A8")

    draw_arrow(62, 50, 62, 42)
    draw_arrow(84, 50, 84, 42)

    # Bottom Unifying Dashboard
    draw_box(22, 6, 56, 14, "Operations Control Dashboard (Streamlit)", [
        "Interactive Mission Control Web App (Port 8501)",
        "Unifies real-time HBase alerts, regional trends, & ML churn triage."
    ], bg_color="#FEF3C7", border_color="#B45309", title_size=11, body_size=9)

    # Arrows to Dashboard
    draw_arrow(41.5, 28, 45, 20)
    draw_arrow(62, 28, 55, 20)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Diagram successfully saved to {output_path}")


def set_cell_background(cell, fill_hex):
    """Set background color of a table cell."""
    tcPr = cell._tc.get_or_add_tcPr()
    shd = parse_xml(f'<w:shd {nsdecls("w")} w:fill="{fill_hex}"/>')
    tcPr.append(shd)

def set_cell_margins(cell, top=80, bottom=80, left=100, right=100):
    """Set cell internal margins in twips."""
    tcPr = cell._tc.get_or_add_tcPr()
    tcMar = parse_xml(
        f'<w:tcMar {nsdecls("w")}>'
        f'<w:top w:w="{top}" w:type="dxa"/>'
        f'<w:bottom w:w="{bottom}" w:type="dxa"/>'
        f'<w:left w:w="{left}" w:type="dxa"/>'
        f'<w:right w:w="{right}" w:type="dxa"/>'
        f'</w:tcMar>'
    )
    tcPr.append(tcMar)

def create_2page_abstract_docx(diagram_image_path: str, docx_path: str):
    doc = Document()

    # Tight 0.5-inch margins for guaranteed 2-page fit with generous printable area
    for section in doc.sections:
        section.top_margin = Inches(0.5)
        section.bottom_margin = Inches(0.5)
        section.left_margin = Inches(0.55)
        section.right_margin = Inches(0.55)

    pure_black = RGBColor(0x00, 0x00, 0x00)

    # ================= PAGE 1 =================
    # Topic
    p_topic = doc.add_paragraph()
    p_topic.paragraph_format.space_before = Pt(0)
    p_topic.paragraph_format.space_after = Pt(2)
    run_topic_lbl = p_topic.add_run("Topic : ")
    run_topic_lbl.font.name = "Calibri"
    run_topic_lbl.font.size = Pt(13)
    run_topic_lbl.font.bold = True
    run_topic_lbl.font.color.rgb = pure_black

    run_topic_val = p_topic.add_run("VortexStream - Real-Time OTT Video QoE & Operations Engine")
    run_topic_val.font.name = "Calibri"
    run_topic_val.font.size = Pt(13)
    run_topic_val.font.bold = True
    run_topic_val.font.color.rgb = pure_black

    # Team Members
    p_tm = doc.add_paragraph()
    p_tm.paragraph_format.space_before = Pt(0)
    p_tm.paragraph_format.space_after = Pt(4)
    run_tm_lbl = p_tm.add_run("Team Members: ")
    run_tm_lbl.font.name = "Calibri"
    run_tm_lbl.font.size = Pt(10.5)
    run_tm_lbl.font.bold = True
    run_tm_lbl.font.color.rgb = pure_black

    run_tm = p_tm.add_run("J R Abhishek (24PW17)   |   Pranav PK (24PW25)")
    run_tm.font.name = "Calibri"
    run_tm.font.size = Pt(10.5)
    run_tm.font.bold = True
    run_tm.font.color.rgb = pure_black

    # Solid Dark Divider Line
    p_line = doc.add_paragraph()
    p_line.paragraph_format.space_before = Pt(0)
    p_line.paragraph_format.space_after = Pt(6)
    pBdr = parse_xml(f'<w:pBdr {nsdecls("w")}><w:bottom w:val="single" w:sz="18" w:space="1" w:color="000000"/></w:pBdr>')
    p_line._p.get_or_add_pPr().append(pBdr)

    # Project Abstract Heading
    p_abs_hd = doc.add_paragraph()
    p_abs_hd.paragraph_format.space_before = Pt(0)
    p_abs_hd.paragraph_format.space_after = Pt(3)
    run_abs_hd = p_abs_hd.add_run("Project Overview & Abstract")
    run_abs_hd.font.name = "Calibri"
    run_abs_hd.font.size = Pt(11.5)
    run_abs_hd.font.bold = True
    run_abs_hd.font.color.rgb = pure_black

    # Concise & Dense Overview (Bold text for dark physical printout)
    p_abs = doc.add_paragraph()
    p_abs.paragraph_format.space_before = Pt(0)
    p_abs.paragraph_format.space_after = Pt(8)
    p_abs.paragraph_format.line_spacing = 1.15
    p_abs.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY

    r_abs = p_abs.add_run(
        "VortexStream is an enterprise-scale big data engineering, real-time streaming, and predictive analytics platform "
        "designed to monitor, analyze, and proactively triage Quality of Experience (QoE) degradations across large-scale "
        "Over-The-Top (OTT) video streaming ecosystems. Utilizing high-resolution playback session telemetry from the "
        "Stanford Puffer dataset—capturing multi-client buffer oscillations, chunk delivery stalls, bitrate adaptations, "
        "and network throughput anomalies across thousands of concurrent streams—the platform operates as an intelligent "
        "video streaming control tower. VortexStream implements a dual-path architecture: an asynchronous, low-latency "
        "streaming path ingesting events via Apache Kafka and processing them with PySpark Structured Streaming (watermarked "
        "deduplication, RocksDB stateful session tracking in Query Q1, 5-minute multi-grain sliding window aggregations in "
        "Query Q2, composite ITU-T P.1203/P.1204 QoE scoring, and a 2-window hysteresis alert engine) writing sub-millisecond "
        "updates to Apache HBase via salted reverse-timestamp keys; and a high-throughput analytical lakehouse maintaining "
        "Snappy-compressed Parquet storage, queried via DuckDB for zero-copy OLAP diagnostics and PySpark batch ETL. "
        "Complementing telemetry triage, an XGBoost ML model evaluates early 15-second playback features to predict viewer "
        "churn risk in real time, unified within an interactive Streamlit mission control dashboard."
    )
    r_abs.font.name = "Calibri"
    r_abs.font.size = Pt(9.5)
    r_abs.font.bold = True  # Fully BOLD for crisp, dark printouts
    r_abs.font.color.rgb = pure_black

    # Technologies Table Heading
    p_tech_hd = doc.add_paragraph()
    p_tech_hd.paragraph_format.space_before = Pt(0)
    p_tech_hd.paragraph_format.space_after = Pt(4)
    run_tech_hd = p_tech_hd.add_run("Technologies Used & Architecture Stack")
    run_tech_hd.font.name = "Calibri"
    run_tech_hd.font.size = Pt(11.5)
    run_tech_hd.font.bold = True
    run_tech_hd.font.color.rgb = pure_black

    # 9 Stack Items
    tech_data = [
        ("Apache Kafka", "Distributed message broker ingesting and buffering real-time video playback telemetry, network metrics, and client error logs."),
        ("PySpark Streaming", "Micro-batch stream engine executing 15m watermark deduplication, RocksDB stateful session tracking (Q1), and 5m multi-grain QoE aggregations (Q2)."),
        ("Apache Spark Core & SQL", "Distributed engine executing parallel batch ETL, data harmonization, pipeline reconciliation, and data quality screening audit gates (V1–V8)."),
        ("Apache HBase (NoSQL)", "Distributed wide-column database engineered for sub-millisecond writes, active session state persistence, and O(1) reverse-timestamp alert scans."),
        ("Apache Parquet", "Columnar format offering high-ratio Snappy compression, partition pruning, and zero-redundancy historical lakehouse storage on disk."),
        ("DuckDB", "In-process analytical OLAP SQL engine delivering ultra-fast, zero-copy analytical queries and feature aggregations directly over Parquet files."),
        ("XGBoost / Spark MLlib", "Distributed machine learning classification models evaluating early-session playback stalls (first 15s) to predict viewer abandonment and churn."),
        ("Streamlit", "Python-based web framework providing an interactive, real-time mission control operations dashboard for QoE monitoring and incident triage."),
        ("Docker & Python", "Container orchestration system deploying isolated Kafka and HBase services; Python driving end-to-end streaming pipelines and algorithms.")
    ]

    table = doc.add_table(rows=1, cols=2)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False

    col_widths = [Inches(1.85), Inches(5.55)]

    # Format Header Row
    hdr_cells = table.rows[0].cells
    hdr_cells[0].text = "Technology"
    hdr_cells[1].text = "Role & Architecture Description"

    for i, title in enumerate(["Technology", "Role & Architecture Description"]):
        cell = hdr_cells[i]
        cell.width = col_widths[i]
        set_cell_background(cell, "000000")  # Pure black background for maximum print contrast
        set_cell_margins(cell, top=70, bottom=70, left=100, right=100)
        p = cell.paragraphs[0]
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after = Pt(0)
        run = p.runs[0]
        run.font.name = "Calibri"
        run.font.size = Pt(9.5)
        run.font.bold = True
        run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)

    # Format Body Rows (All Bold, pure black text)
    for idx, (tech_name, tech_desc) in enumerate(tech_data):
        row_cells = table.add_row().cells
        bg_color = "F1F5F9" if idx % 2 == 1 else "FFFFFF"

        # Col 0: Technology
        cell_0 = row_cells[0]
        cell_0.width = col_widths[0]
        cell_0.text = tech_name
        set_cell_background(cell_0, bg_color)
        set_cell_margins(cell_0, top=45, bottom=45, left=90, right=90)
        p0 = cell_0.paragraphs[0]
        p0.paragraph_format.space_before = Pt(0)
        p0.paragraph_format.space_after = Pt(0)
        r0 = p0.runs[0]
        r0.font.name = "Calibri"
        r0.font.size = Pt(8.8)
        r0.font.bold = True
        r0.font.color.rgb = pure_black

        # Col 1: Description
        cell_1 = row_cells[1]
        cell_1.width = col_widths[1]
        cell_1.text = tech_desc
        set_cell_background(cell_1, bg_color)
        set_cell_margins(cell_1, top=45, bottom=45, left=90, right=90)
        p1_cell = cell_1.paragraphs[0]
        p1_cell.paragraph_format.space_before = Pt(0)
        p1_cell.paragraph_format.space_after = Pt(0)
        p1_cell.paragraph_format.line_spacing = 1.05
        r1_cell = p1_cell.runs[0]
        r1_cell.font.name = "Calibri"
        r1_cell.font.size = Pt(8.5)
        r1_cell.font.bold = True  # Fully BOLD for crisp printout
        r1_cell.font.color.rgb = pure_black

    # Solid borders
    tblPr = table._tbl.tblPr
    tblBorders = parse_xml(
        f'<w:tblBorders {nsdecls("w")}>'
        f'<w:top w:val="single" w:sz="8" w:space="0" w:color="000000"/>'
        f'<w:left w:val="none"/>'
        f'<w:bottom w:val="single" w:sz="12" w:space="0" w:color="000000"/>'
        f'<w:right w:val="none"/>'
        f'<w:insideH w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>'
        f'<w:insideV w:val="none"/>'
        f'</w:tblBorders>'
    )
    tblPr.append(tblBorders)

    # ================= PAGE 2 =================
    doc.add_page_break()

    p_arch_hd = doc.add_paragraph()
    p_arch_hd.paragraph_format.space_before = Pt(0)
    p_arch_hd.paragraph_format.space_after = Pt(6)
    run_arch_hd = p_arch_hd.add_run("System Architecture & End-to-End Data Flow")
    run_arch_hd.font.name = "Calibri"
    run_arch_hd.font.size = Pt(13)
    run_arch_hd.font.bold = True
    run_arch_hd.font.color.rgb = pure_black

    # Embed High-Contrast Diagram
    p_img = doc.add_paragraph()
    p_img.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_img.paragraph_format.space_before = Pt(0)
    p_img.paragraph_format.space_after = Pt(0)
    run_img = p_img.add_run()
    run_img.add_picture(diagram_image_path, width=Inches(7.3))

    doc.save(docx_path)
    print(f"2-Page Document successfully created at {docx_path}")

if __name__ == "__main__":
    diagram_path = "VortexStream_Architecture_Flow.png"
    generate_diagram(diagram_path)
    # Save as 2-page document
    for docx_name in ["VortexStream_Abstract_2Page.docx", "VortexStream_Project_Abstract.docx"]:
        try:
            create_2page_abstract_docx(diagram_path, docx_name)
        except PermissionError:
            print(f"Notice: {docx_name} is currently open in Microsoft Word. Saved other versions successfully.")

