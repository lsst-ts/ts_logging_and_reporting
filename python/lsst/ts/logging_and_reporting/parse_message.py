import re

import lsst.ts.logging_and_reporting.reports as rep


def ignore_this():  # TODO remove
    allsrc = all.AllSources(
        server_url="https://summit-lsp.lsst.codes",
        min_dayobs="2024-08-02",
        max_dayobs="2024-10-21",
    )

    eidx = [
        idx
        for idx, r in enumerate(allsrc.nar_src.records[:125])
        if "Error:" in r["message_text"]
    ]

    allsrc.nar_src.records[eidx[6]]["message_text"]

    #! re_err = re.compile(r'^.*(Error:.*)', re.DOTALL | re.DEBUG | re.MULTILINE)

    # re_err.match(allsrc.nar_src.records[eidx[6]]['message_text']).group(1)
    # => "Error: msg='Command failed', ackcmd=(ackcmd private_seqNum=1579091411, ack=&lt;SalRetCode.CMD_FAILED: -302&gt;, error=1, result='Failed: wavefrontError write(private_revCode: cdb25a59, private_sndStamp: 1725582543.3115678, private_rcvStamp: 0.0, private_seqNum: 1, private_identity: MTAOS, private_origin: 3159695, sensorId: 0, annularZernikeCoeff: 0.08561753960450617) failed: probably at least one array field is too short')\r\n"


re_err1 = re.compile(
    r"^(?P<err>\S*Error:.*)",
    flags=(re.DOTALL | re.ASCII | re.MULTILINE | re.IGNORECASE),
)

re_err2 = re.compile(
    r"(?P<err>.*error:.*)", flags=(re.DOTALL | re.ASCII | re.MULTILINE | re.IGNORECASE)
)

# Start after whitespace before error. End at string end.
re_err = re.compile(
    r"(?P<err>\S*error:.*)", flags=(re.DOTALL | re.ASCII | re.MULTILINE | re.IGNORECASE)
)


def highlight_code(matchobj):
    return rep.htmlcode(matchobj.group("err"), bgcolor="tomato", size="0.875em")


def markup_error(records, src_field="message_text"):
    """SIDE-EFFECTS: add DEST_FIELD to all records.
    The DEST_FIELD will contain modified text from SRC_FIELD (or None).
    """
    dest_field = "error_message"

    for r in records:
        orig = r.get(src_field)

        # If the RE is found, markup the whole string.
        #! if re_err.search(orig):
        #!     r[dest_field] = rep.htmlcode(orig, bgcolor='tomato')

        new = re_err.sub(highlight_code, orig)
        if orig != new:
            r[dest_field] = new
