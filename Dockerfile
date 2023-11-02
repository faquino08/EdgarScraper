FROM sofraserv/financedb_base_chromium:test

ENV EDGAR_PWD "autopass"
ENV EDGAR_DEBUG True

RUN    useradd -ms /bin/bash edgardocker
RUN    echo edgardocker:${EDGAR_PWD} | chpasswd
WORKDIR /var/www/EdgarFlaskDocker
RUN    mkdir /var/www/EdgarFlaskDocker/logs

EXPOSE 8080/tcp

COPY   requirements.txt requirements.txt
RUN    pip3 install -r requirements.txt

COPY   . /var/www/EdgarFlaskDocker
RUN    chown -R edgardocker:edgardocker /var/www/
RUN    mkdir /var/www/EdgarFlaskDocker/DataBroker/Sources/Edgar/index

ADD start.sh /var/www/EdgarFlaskDocker/start.sh
RUN chmod +x /var/www/EdgarFlaskDocker/start.sh
CMD ["/var/www/EdgarFlaskDocker/start.sh"]