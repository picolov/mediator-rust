# mediator-rust
mediator of inter worker communication, parts of workforce.
workforce system consist of :
- worker
- mediator

## how to generate certificate
xxx.key is the private key
xxx.crt is the public certificate
```
# Generate a self-signed certificate for localhost.
# This is only valid for 10 days so we can use serverCertificateHashes to avoid a CA (bugged).

> openssl ecparam -genkey -name prime256v1 -out localhost.key
> openssl req -x509 -sha256 -nodes -days 10 -key localhost.key -out localhost.crt -config localhost.conf -extensions 'v3_req'

# Generate a hex-encoded (easy to parse) SHA-256 hash of the certificate.
> openssl x509 -in localhost.crt -outform der | openssl dgst -sha256 -binary | xxd -p -c 256 > localhost.hex
```

## how to run server
```
# clone forked version of rumqtt because latest version on crate is not having async auth feature

> git clone https://github.com/picolov/rumqtt.git libs/rumqtt
> cargo run -- --tls-cert ./cert.crt --tls-key ./cert.key
```
## run binary
```
> ./target/debug/mediator --tls-cert ./cert.crt --tls-key ./cert.key
```