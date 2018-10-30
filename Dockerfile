FROM rust:1.30

ADD . /source
WORKDIR /source

RUN rustc -V

RUN rustup default nightly

EXPOSE 80

RUN cargo build --release
CMD ROCKET_ENV=production cargo run --release