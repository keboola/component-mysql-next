FROM pypy:3.9-slim
ENV PYTHONIOENCODING utf-8
# ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1

COPY . /code/

# install gcc to be able to build packages - e.g. required by regex, dateparser, also required for pandas
# Update default packages
RUN apt-get update

# Get Ubuntu packages
RUN apt-get install -y \
    build-essential \
    curl \
    libssl-dev \
    openssl \
    libopenblas-dev


# Get Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install flake8

# requirements
RUN pypy -m ensurepip
RUN pypy -m pip install -U pip
RUN pypy -m pip install -U pip wheel
RUN pypy -m pip install --upgrade pip
RUN pypy -m pip install duckdb==0.7.0
RUN pypy -m pip install -r /code/requirements.txt

WORKDIR /code/

CMD ["pypy", "-u", "/code/src/component.py"]