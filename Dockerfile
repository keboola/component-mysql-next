FROM pypy:3.10-7.3.13-slim-bullseye


ENV PYTHONIOENCODING utf-8
# ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1

COPY . /code/

# install gcc to be able to build packages - e.g. required by regex, dateparser, also required for pandas
# Update default packages
RUN apt-get update

# Get Ubuntu packages
RUN apt-get install -y \
    build-essential \
    wget \
    curl \
    unixodbc \
    odbcinst \
    unixodbc-dev \
    openssl

# install Snowflake odbc
WORKDIR /tmp
RUN wget https://sfc-repo.snowflakecomputing.com/odbc/linux/3.1.1/snowflake-odbc-3.1.1.x86_64.deb
RUN dpkg -i snowflake-odbc-3.1.1.x86_64.deb; apt-get install -y -f


# Get Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install flake8


# requirements
RUN pypy -m ensurepip
RUN pypy -m pip install -U pip wheel
RUN pypy -m pip install --upgrade pip
RUN pypy -m pip install -r /code/requirements.txt

WORKDIR /code/

CMD ["pypy", "-u", "/code/src/component.py"]