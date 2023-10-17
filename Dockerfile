FROM python:3.9.13-buster

ENV EDGAR_PWD "autopass"
ENV EDGAR_DEBUG True

RUN    useradd -ms /bin/bash edgardocker
RUN    echo edgardocker:${EDGAR_PWD} | chpasswd
WORKDIR /var/www/EdgarFlaskDocker

RUN    apt-get update
RUN    apt-get install -y wget


RUN    echo y | apt-get install unixodbc unixodbc-dev
RUN    echo y | apt-get install locales
RUN    echo y | apt-get install ufw
RUN    echo y | apt-get install chromium
RUN    echo y | apt-get install libpam-pwdfile
RUN    wget https://chromedriver.storage.googleapis.com/90.0.4430.24/chromedriver_linux64.zip
RUN    unzip chromedriver_linux64.zip
RUN    mv chromedriver /usr/bin
RUN    sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen
RUN    locale-gen en_US.UTF-8  
ENV    LANG en_US.UTF-8  
ENV    LANGUAGE en_US:en  
ENV    LC_ALL en_US.UTF-8 

EXPOSE 8080/tcp
RUN    ufw allow in 21/tcp
RUN    ufw allow in 22/tcp

COPY   requirements.txt requirements.txt
RUN    pip3 install -r requirements.txt

COPY   . /var/www/EdgarFlaskDocker
RUN    chown -R edgardocker:edgardocker /var/www/
RUN    mkdir /var/www/EdgarFlaskDocker/DataBroker/Sources/Edgar/index

ADD start.sh /var/www/EdgarFlaskDocker/start.sh
RUN chmod +x /var/www/EdgarFlaskDocker/start.sh
CMD ["/var/www/EdgarFlaskDocker/start.sh"]