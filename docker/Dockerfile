ARG dev_cycle=develop
FROM rubincr.lsst.org/develop-env-sqre:${dev_cycle}

WORKDIR /home/saluser/develop
COPY --chown=saluser:saluser . .

RUN source ~/.bashrc && pip install -e .

CMD ["docker/startup.sh"]
