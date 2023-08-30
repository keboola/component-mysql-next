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
    openssl

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