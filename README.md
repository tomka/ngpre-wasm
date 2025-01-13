# NG-Pre [![Build Status](https://travis-ci.org/tomka/ngpre-wasm.svg?branch=master)](https://travis-ci.org/tomka/ngpre-wasm)

Browser-compatible WASM bindings to the [Rust implementation](https://github.com/tomka/rust-ngpre) of the [Neuroglancer Precomputed n-dimensional tensor file storage format](https://github.com/google/neuroglancer/tree/master/src/neuroglancer/datasource/precomputed). This library is based on the [n5-wasm](https://github.com/aschampion/n5-wasm) and [rust-n5](https://github.com/aschampion/rust-n5) libraries and reused a lot of its infrastructure. Sharded datasets are supported.

NGPre datasets must be available via CORS-compatible HTTP.

Currently only zip and jpeg chunk compression is supported.

## Build Instructions

This assumes you have [rustup](https://rustup.rs/) installed.

```sh
git clone https://github.com/tomka/ngpre-wasm
cd ngpre-wasm
rustup override set nightly
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
wasm-pack build --target no-modules --release
echo "self.ngpre_wasm = wasm_bindgen;" >> pkg/ngpre_wasm.js
```

The built npm package will be in `pkg/`.

Buld and update CATMAID:

wasm-pack build --target no-modules --debug && echo "self.ngpre_wasm = wasm_bindgen;" >> pkg/ngpre_wasm.js && cp pkg/ngpre_wasm_bg.wasm <catmaid-dir>/django/applications/catmaid/static/libs/ngpre-wasm && cp pkg/ngpre_wasm.js <catmaid-dir>/django/applications/catmaid/static/libs/ngpre-wasm

chromium --disable-web-security --user-data-dir=/tmp/google-chrome-user
