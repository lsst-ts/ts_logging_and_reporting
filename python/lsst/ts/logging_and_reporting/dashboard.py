import lsst.ts.logging_and_reporting.source_adapters as sad

class Dashboard:  # TODO Move to its own file (utils.py).
    """Verify that we can get to all the API endpoints and databases we need for
    any of our sources.
    """

    envs = dict(
        summit = 'https://summit-lsp.lsst.codes',
        usdf_dev = 'https://usdf-rsp-dev.slac.stanford.edu',
        tucson = 'https://tucson-teststand.lsst.codes',
        # Environments not currently used:
        #    rubin_usdf_dev = '',
        #    data_lsst_cloud = '',
        #    usdf = '',
        #    base_data_facility = '',
        #    rubin_idf_int = '',
    )
    adapters = [sad.ExposurelogAdapter,
                sad.NarrativelogAdapter,
                sad.NightReportAdapter,
                ]

    def report(self, timeout=None):
        """Check our ability to connect to every Source on every Environment.
        Report a summary.

        RETURN: percentage of good connectons.
        """
        url_status = dict() # url_status[endpoint_url] = http _status_code
        working = set() # Set of servers that work for  all our required endpoints.

        for env,server in self.envs.items():
            server_all_good = True
            for adapter in self.adapters:
                service = adapter(server_url=server)
                stats, adapter_all_good = service.check_endpoints(timeout=timeout)
                url_status.update(stats)
            server_all_good &= adapter_all_good
            if server_all_good:
                working.add(server)

        total_cnt = good_cnt = 0
        good = list()
        bad = list()
        for url,stat in url_status.items():
            total_cnt += 1
            if stat == 200:
                good_cnt += 1
                good.append(url)
            else:
                bad.append((url,stat))

        print(f'\nConnected to {good_cnt} out of {total_cnt} endpoints.'
              f'({good_cnt/total_cnt:.0%})'
              )
        goodstr = "\n\t".join(good)
        print(f'Successful connects ({good_cnt}): ')
        for gurl in good:
            print(f'\t{gurl}')

        print(f'Failed connects ({total_cnt - good_cnt}): ')
        for burl,stat in bad:
            print(f'\t{stat}: {burl}')

        status = dict(num_good=good_cnt,
                      num_total=total_cnt,
                      good_urls=good,
                      bad_ursl=bad,
                      )
        return good_cnt/total_cnt, working
# END: class Dashboard