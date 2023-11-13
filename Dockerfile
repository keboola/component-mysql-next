FROM pypy:3.10-7.3.13-slim-bullseye
ENV PYTHONIOENCODING utf-8
# ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1

COPY . /code/

# install gcc to be able to build packages - e.g. required by regex, dateparser, also required for pandas
# Update default packages
RUN apt-get clean
RUN apt-get update

# Get Ubuntu packages
RUN apt-get install -y \
    unzip \
    build-essential \
    wget \
    curl \
    unixodbc \
    odbcinst \
    unixodbc-dev \
    openssl

WORKDIR /tmp

# Install DuckDB odbc
RUN wget https://github.com/duckdb/duckdb/releases/download/v0.9.1/duckdb_odbc-linux-amd64.zip
RUN mkdir duckdb_odbc
RUN unzip duckdb_odbc-linux-amd64.zip -d duckdb_odbc
WORKDIR /tmp/duckdb_odbc

RUN bash unixodbc_setup.sh -s
COPY ./.docker/duckdb_odbc.ini /etc/odbc.ini


# Get Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install flake8

# requirements
RUN pypy -m ensurepip
RUN pypy -m pip install -U pip
RUN pypy -m pip install -U pip wheel
RUN pypy -m pip install --upgrade pip
RUN pypy -m pip install -r /code/requirements.txt

WORKDIR /code/

CMD ["pypy", "-u", "/code/src/component.py"]