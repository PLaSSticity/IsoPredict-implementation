FROM ubuntu:20.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update &&                   \
    apt-get install -y                  \
        build-essential                 \
        python3                         \
        python3-pip                     \
        python3.8-venv                  \
        cargo                           \
        ant                             \
        mariadb-client                  \
        libssl-dev                      \
        clang                           \
        bsdmainutils                    \
        curl                            \
        pkg-config
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup install stable
WORKDIR /isopredict
COPY . /isopredict
RUN pip3 install -r requirements.txt
RUN python3 -m build
RUN pip3 install .
WORKDIR /isopredict/tests/monkeydb
RUN bash build.sh
WORKDIR /isopredict/tests/oltp
RUN bash build.sh
CMD ["/bin/bash"]
