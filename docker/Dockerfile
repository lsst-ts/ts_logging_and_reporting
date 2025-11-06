ARG dev_cycle=develop
FROM lsstts/develop-env:${dev_cycle}

WORKDIR /home/saluser/develop
COPY . .

USER root
RUN chown -R saluser:saluser /home/saluser/develop
USER saluser

RUN source /home/saluser/.setup_dev.sh && \
	# workaround meanwhile waiting for dependencies to be built into the develop-env image
	conda config --set solver classic && \
	conda install -c conda-forge -y rubin-sim=2.4 && \
	# install this package
	pip install -e .

CMD ["docker/startup.sh"]
