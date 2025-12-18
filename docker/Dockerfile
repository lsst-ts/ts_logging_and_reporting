ARG dev_cycle=develop
FROM lsstts/develop-env:${dev_cycle}

WORKDIR /home/saluser/develop
COPY . .

USER root
RUN chown -R saluser:saluser /home/saluser/develop
USER saluser

RUN source /home/saluser/.setup_dev.sh && \
	# workaround meanwhile develop image gets updated
	conda install -c lsstts -y ts-develop==0.5.9 && \
	# install this package
	pip install -e .

CMD ["docker/startup.sh"]
