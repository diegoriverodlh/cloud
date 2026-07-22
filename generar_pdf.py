import markdown
from weasyprint import HTML

# 1. Leer el archivo Markdown
with open("DOCUMENTACION.md", "r", encoding="utf-8") as f:
    markdown_text = f.read()

# 2. Convertir Markdown a HTML habilitando tablas y bloques de código
html_body = markdown.markdown(
    markdown_text, 
    extensions=['tables', 'fenced_code', 'codehilite']
)

# 3. Aplicar estilo CSS profesional para la impresión en PDF
html_document = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{
            font-family: 'Helvetica', 'Arial', sans-serif;
            margin: 40px;
            color: #202124;
            line-height: 1.6;
            font-size: 11pt;
        }}
        h1 {{
            color: #1a73e8;
            border-bottom: 2px solid #1a73e8;
            padding-bottom: 8px;
            font-size: 20pt;
        }}
        h2 {{
            color: #202124;
            border-bottom: 1px solid #dadce0;
            padding-bottom: 4px;
            margin-top: 25px;
            font-size: 15pt;
        }}
        h3 {{
            color: #3c4043;
            font-size: 12pt;
        }}
        code {{
            background-color: #f1f3f4;
            padding: 2px 6px;
            border-radius: 4px;
            font-family: 'Courier New', Courier, monospace;
            font-size: 9.5pt;
        }}
        pre {{
            background-color: #282c34;
            color: #abb2bf;
            padding: 15px;
            border-radius: 6px;
            overflow-x: auto;
            font-family: 'Courier New', Courier, monospace;
            font-size: 9pt;
            line-height: 1.4;
        }}
        pre code {{
            background-color: transparent;
            color: inherit;
            padding: 0;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
        }}
        th, td {{
            border: 1px solid #dadce0;
            padding: 8px 12px;
            text-align: left;
        }}
        th {{
            background-color: #f8f9fa;
            font-weight: bold;
        }}
        blockquote {{
            border-left: 4px solid #1a73e8;
            margin: 0;
            padding-left: 15px;
            color: #5f6368;
        }}
    </style>
</head>
<body>
    {html_body}
</body>
</html>
"""

# 4. Generar el PDF final
HTML(string=html_document).write_pdf("DOCUMENTACION.pdf")
print("[ÉXITO] Documentación exportada a DOCUMENTACION.pdf")