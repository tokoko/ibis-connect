FROM gitpod/openvscode-server:latest
USER root

RUN apt-get update && apt-get install -y python3 python3-pip iputils-ping python-is-python3 python3-packaging
RUN python3 -m pip install --upgrade pip

RUN wget https://open-vsx.org/api/ms-python/python/2023.16.0/file/ms-python.python-2023.16.0.vsix --no-check-certificate
RUN /home/.openvscode-server/bin/openvscode-server --install-extension ms-python.python-2023.16.0.vsix
RUN rm -rf ms-python.python-2023.16.0.vsix

RUN pip install ibis-framework[duckdb]
ENV PYTHONPATH=/home/ibis-connect


