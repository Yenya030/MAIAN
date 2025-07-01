FROM python:3.8-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends software-properties-common ca-certificates wget && \
    add-apt-repository -y ppa:ethereum/ethereum && \
    apt-get update && \
    apt-get install -y --no-install-recommends geth solc && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /maian

COPY . /maian

RUN pip install --no-cache-dir web3 z3-solver

CMD ["python", "tool/maian.py", "-h"]
