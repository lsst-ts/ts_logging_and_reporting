junk = """
<!DOCTYPE html>
<html>
<head>
    <style>
    th,td {
        text-align: left;
        vertical-align: text-top;
    }
    </style>
</head>
<body>

...



</body>
</html>
"""

templates = dict(
    decanted_html="""
    <p>
    table_columns={{ df.columns }}.
    sparse_dict={{ sparse_dict.keys() }}
    </p>
    <p>DF as html</p>
    {{ df.to_html() }}

    <ul>
    {% for key, val in sparse_dict.items() %}
      <li><b>{{ key }}:</b> <pre>{{ val }}</pre></li>
    {% endfor %}
    </ul>
""",
)
