import numpy as np
from jinja2 import DictLoader, Environment

import lsst.ts.logging_and_reporting.utils as ut
from lsst.ts.logging_and_reporting.templates import templates

# white-space: pre-wrap;
# vertical-align: text-top;
#    background-color: coral;


def decant_by_column_name(df, nontable_columns):
    """Partition a DF frame into a dense part and a sparse part.
    The dense part is stored in a DF.
    The sparse part is stored in a dict[col] => {elem1, ...}
    """
    table_columns = sorted(df.columns)
    for c in nontable_columns:
        table_columns.remove(c)
    dense_df = df[table_columns]
    sparse_dict = {k: set(v.values()) for k, v in df[nontable_columns].to_dict().items()}
    return dense_df, sparse_dict


def decant_by_density(df, thresh):
    """Partition a DF frame a dense part and a sparse part.
    See: decant_by_column_name

    The columns that that are removed from DF are determined
    by the density THRESH (1- NaN/num_rows).
    """

    def density(df, column):
        pass  # TODO

    # Remove columns >= 95% NaN
    val_count = int(0.05 * len(df))
    df.dropna(thresh=val_count, axis="columns", inplace=True)  # DATA LOSS
    nontable_columns = []
    return decant_by_column_name(df, nontable_columns)


def decant_by_maxwidth(df, thresh):
    df_object = df.select_dtypes(include=[object])
    measurer = np.vectorize(len)
    width_map = dict(zip(df_object, measurer(df_object.values.astype(str)).max(axis=0)))
    wide = {col for col, width in width_map.items() if width > thresh}
    table_columns = list(set(df.columns.to_list()) - wide)
    dense_df = df[table_columns]
    sparse_dict = {k: "\n\n".join([s for s in v.values() if s]) for k, v in df[list(wide)].to_dict().items()}
    return ut.wrap_dataframe_columns(dense_df), sparse_dict


# It would be better to have each template in its own HTML file
# and use jinja2.Environment().get_template().render().
# But this requires having the HTML file in the filesystem and we
# expect to be run from a Notebook under Times Square. Times Square
# edicts that the notebook does not use the filesytem.
def render_using(template, save=False, **context):
    env = Environment(loader=DictLoader(templates))
    compiled_template = env.get_template(template)
    rendered_html = compiled_template.render(**context)
    if save:
        with open("output.html", "w") as f:
            f.write(rendered_html)
    return rendered_html


# for reduced DF
def render_reduced_df(df, thresh=0.95, verbose=False):
    # dense_df, sparse_dict = decant_by_density(df, thresh)
    dense_df, sparse_dict = decant_by_maxwidth(df, 10)

    if verbose:
        print(
            f"""Debug view.render_reduced_df:
        columns={list(dense_df.columns)}
        keys={list(sparse_dict.keys())}
        """
        )

    # https://jinja.palletsprojects.com/en/stable/templates/
    context = dict(
        df=dense_df,
        sparse_dict=sparse_dict,
    )
    return render_using("decanted_html", **context)
