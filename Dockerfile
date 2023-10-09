FROM python:3.11-slim
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
    zlib1g-dev wget



# https://community.snowflake.com/s/question/0D5Do000010NAVzKAO/hi-snowflake-teami-am-facing-issues-while-connecting-to-snowflake-using-snowflake-connector-with-oscryptoerrorslibrarynotfounderror-error-detecting-the-version-of-libcrypto
RUN wget https://www.openssl.org/source/openssl-3.0.9.tar.gz &&\
    tar -xzvf openssl-3.0.9.tar.gz &&\
    cd openssl-3.0.9 &&\
    ./config --prefix=/usr/local/ssl --openssldir=/usr/local/ssl shared zlib &&\
    make &&\
    make install


RUN echo '/usr/local/ssl/lib' > /etc/ld.so.conf.d/openssl-3.0.9.conf &&\
    ldconfig -v &&\
    mv /usr/bin/openssl /usr/bin/openssl.bak &&\
    mv /usr/bin/c_rehash /usr/bin/c_rehash.bak &&\
    update-alternatives --install /usr/bin/openssl openssl /usr/local/ssl/bin/openssl 1 &&\
    update-alternatives --install /usr/bin/c_rehash c_rehash /usr/local/ssl/bin/c_rehash 1

RUN ln -s libcrypto.so.3 /usr/lib/libcrypto.so
# Get Rust
#RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
#
#ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install flake8


# requirements
#RUN -m ensurepip
RUN pip install -U pip wheel
RUN pip install --upgrade pip
RUN pip install -r /code/requirements.txt

WORKDIR /code/

CMD ["python", "-u", "/code/src/component.py"]