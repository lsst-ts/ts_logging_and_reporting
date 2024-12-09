templates = dict(
    decanted_html="""
    {{ df.to_html() }}

    <ul>
    {% for key, val in sparse_dict.items() %}
      <li>
         <b>{{ key }}:</b>
         <pre {font-size: 0.5em; color: slateblue;}>
            {{ val }}
         </pre>
      </li>
    {% endfor %}
    </ul>
""",
)

#  ###########################################################################
head = """
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
"""

foot = """
</body>
</html>
"""
