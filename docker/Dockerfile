ARG dev_cycle=develop
FROM lsstts/develop-env:${dev_cycle}

WORKDIR /home/saluser/develop
COPY . .

USER root
RUN chown -R saluser:saluser /home/saluser/develop
USER saluser

RUN source /home/saluser/.setup_dev.sh && \
	pip uninstall -y rubin-sim && \
	# pip uninstall -y lsst-resources lsst-utils && \
	# pip install --upgrade schedview==0.23.0 && \
	# pip install lsst-resources lsst-utils && \
	pip install git+https://github.com/lsst/rubin_sim.git@v2.6.2a2 && \
	# install this package
	pip install -e .

CMD ["docker/startup.sh"]
