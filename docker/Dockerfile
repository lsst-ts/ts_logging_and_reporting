ARG dev_cycle=develop
FROM lsstts/develop-env:${dev_cycle}

WORKDIR /home/saluser/develop
COPY . .

USER root
RUN chown -R saluser:saluser /home/saluser/develop
USER saluser

RUN source /home/saluser/.setup_dev.sh && \
	pip install git+https://github.com/lsst-ts/ts_planning_tool.git@develop && \
	# install this package
	pip install -e .

CMD ["docker/startup.sh"]
