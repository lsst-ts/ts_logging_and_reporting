from collections import Counter, defaultdict
from urllib.parse import urlencode
from warnings import warn

import lsst.ts.logging_and_reporting.consdb as cdb
import lsst.ts.logging_and_reporting.source_adapters as sad
import lsst.ts.logging_and_reporting.utils as ut
import requests


class Dashboard:  # TODO Move to its own file (utils.py).
    """Verify that we can get to all the API endpoints and databases
    we need for any of our sources.
    """

    timeout = (3.05, 2)  # connect, read (seconds)

    envs = dict(  # key, server
        summit="https://summit-lsp.lsst.codes",
        usdf_dev="https://usdf-rsp-dev.slac.stanford.edu",
        usdf="https://usdf-rsp.slac.stanford.edu",
        tucson="https://tucson-teststand.lsst.codes",
        # Environments not currently used:
        #    rubin_usdf_dev = '',
        #    data_lsst_cloud = '',
        #    base_data_facility = '',
        #    rubin_idf_int = '',
    )
    adapters = [
        sad.NightReportAdapter,
        sad.NarrativelogAdapter,
        sad.ExposurelogAdapter,
        cdb.ConsdbAdapter,
    ]

    def keep_fields(self, recs, outfields):
        """Keep only keys in OUTFIELDS list of RECS (list of dicts)
        SIDE EFFECT: Removes extraneous keys from all dicts in RECS.
        """
        if (recs is None) or (len(recs) < 1):
            return None
        if not outfields:
            return None

        nukefields = set(recs[0].keys()) - set(outfields)
        for rec in recs:
            nukefields = set(rec.keys()) - set(outfields)
            for f in nukefields:
                del rec[f]

    def get_big_sample(self, endpoint, samples, fields):
        timeout = (5.05, 120)  # connection, read timeouts (secs)
        qparams = dict(
            limit=samples,
        )
        url = f"{endpoint}?{urlencode(qparams)}"
        response = requests.get(url, timeout=timeout, headers=ut.get_auth_header())

        response.raise_for_status()
        records = response.json()
        self.keep_fields(records, fields)
        return records

    def nr_values(self, samples):
        cset = set()
        sset = set()
        hset = set()
        histo = Counter()

        for idx, r in enumerate(samples):
            if r.get("components"):
                cset.update(r.get("components", []))
            if r.get("primary_software_components"):
                sset.update(r.get("primary_software_components", []))
            if r.get("primary_hardware_components"):
                hset.update(r.get("primary_hardware_components", []))

            histo.update(r.get("components", []))
            histo.update(r.get("primary_software_components", []))
            histo.update(r.get("primary_hardware_components", []))

        return (cset | sset | hset), histo

    def get_sample_data(self, server, count=1):
        samples = defaultdict(dict)  # samples[endpoint_url] -> one_record_dict
        for adapter in self.adapters:
            sa = adapter(server_url=server, limit=count)
            for ep in sa.endpoints:
                qstr = "?instrument=LSSTComCamSim" if ep == "exposures" else ""
                url = f"{server}/{sa.service}/{ep}{qstr}"
                try:
                    res = requests.get(
                        url, timeout=self.timeout, headers=ut.get_auth_header()
                    )
                    recs = res.json()
                    if isinstance(recs, dict):
                        samples[url] = recs
                    else:
                        samples[url] = recs[0:count]
                except Exception as err:
                    # Made following more complicated to get around
                    # comboniation of BLACK re-write and FLAKE8
                    msg = f"Could not get data from {url}: "
                    msg += f" {err=} {res.content[:300]=}..."
                    warn(msg)
                    samples[url] = None
        return dict(samples)

    def report(self, timeout=None, verbose=True):
        """Check our ability to connect to every Source on every Environment.
        Report a summary.

        RETURN: percentage of good connectons.
        """
        url_status = dict()  # url_status[endpoint_url] = http _status_code
        working = set()  # Servers that work for  all our required endpoints.

        if not timeout:
            timeout = self.timeout
        for env, server in self.envs.items():
            server_all_good = True
            for adapter in self.adapters:
                service = adapter(server_url=server)
                service.verbose = verbose
                stats, aag = service.check_endpoints()
                url_status.update(stats)
            server_all_good &= aag  # adapter all good
            if server_all_good:
                working.add(server)

        total_cnt = good_cnt = 0
        good = list()
        bad = list()
        for url, stat in url_status.items():
            total_cnt += 1
            if stat == 200:
                good_cnt += 1
                good.append(url)
            else:
                bad.append((url, stat))

        print(
            f"\nConnected to {good_cnt} out of {total_cnt} endpoints."
            f"({good_cnt/total_cnt:.0%})"
        )
        print(f"Successful connects ({good_cnt}): ")
        for gurl in good:
            print(f"\t{gurl}")

        print(f"Failed connects ({total_cnt - good_cnt}): ")
        for burl, stat in bad:
            print(f"\t{stat}: {burl}")

        return good_cnt / total_cnt, working


# END: class Dashboard
