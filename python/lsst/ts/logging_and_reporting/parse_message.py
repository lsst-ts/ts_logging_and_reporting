import html
import re

import lsst.ts.logging_and_reporting.reports as rep

# Start after whitespace before error. End at string end.
# BLACK work-around
flags = re.DOTALL | re.ASCII | re.MULTILINE | re.IGNORECASE
re_err = re.compile(r"(?P<err>\S*error:.*)", flags=flags)
re_tb = re.compile(r"\b(Traceback \(most recent call last\):.*)", flags=flags)
re_pnl = re.compile(r"&lt;PARSED_NL&gt;", flags=re.ASCII | re.MULTILINE)


def highlight_code(matchobj):
    text = html.escape(matchobj.group(1))
    # light red
    return rep.htmlcode(text, bgcolor="#FFDDDD", size="0.875em", left=20)


def markup_errors(records, src_field="message_text"):
    """SIDE-EFFECTS: add DEST_FIELD to all records that are marked up.
    The DEST_FIELD will contain modified text from SRC_FIELD.
    """
    dest_field = "error_message"

    for r in records:
        orig = r.get(src_field)

        # If the RE is found, markup the whole string.   # TODO remove
        # #! if re_err.search(orig):
        # #!     r[dest_field] = rep.htmlcode(orig, bgcolor='tomato')

        # #!re.search('|'.join([re_err, re_tb])
        # #!new = re_err.sub(highlight_code, orig)

        new = re_pnl.sub("<br>", orig)
        new = re_tb.sub(highlight_code, new)
        if orig != new:
            r[dest_field] = new
