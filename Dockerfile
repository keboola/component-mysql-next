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
#
#
#
## https://community.snowflake.com/s/question/0D5Do000010NAVzKAO/hi-snowflake-teami-am-facing-issues-while-connecting-to-snowflake-using-snowflake-connector-with-oscryptoerrorslibrarynotfounderror-error-detecting-the-version-of-libcrypto
RUN wget https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-3.3.6.tar.gz &&\
    tar -xzvf libressl-3.3.6.tar.gz


RUN cd libressl-3.3.6 && ./configure &&\
    make &&\
    make install

RUN ln -s /usr/lib/aarch64-linux-gnu/libssl.so.48.0.2 libssl.so.48 && \
    ln -s /usr/lib/aarch64-linux-gnu/libcrypto.so.46.0.2 libcrypto.so46 && \
    ldconfig




#RUN echo '/usr/local/ssl/lib' > /etc/ld.so.conf.d/libressl-3.3.6.conf &&\
#    ldconfig -v &&\
#    mv /usr/bin/openssl /usr/bin/openssl.bak &&\
#    mv /usr/bin/c_rehash /usr/bin/c_rehash.bak &&\
#    update-alternatives --install /usr/bin/openssl openssl /usr/local/ssl/bin/openssl 1 &&\
#    update-alternatives --install /usr/bin/c_rehash c_rehash /usr/local/ssl/bin/c_rehash 1

#RUN ln -s libcrypto.so.3 /usr/lib/libcrypto.so
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