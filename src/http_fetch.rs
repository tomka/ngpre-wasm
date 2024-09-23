use std::{fmt::Write};
use std::str::FromStr;
use std::cmp;
use futures::future::{self, FutureExt, Either};
use futures::TryFutureExt;

use std::collections::HashMap;
use std::path::PathBuf;

use js_sys::ArrayBuffer;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    Request,
    RequestInit,
    RequestMode,
    Response,
    console,
};

use super::*;

use ngpre::{
    DataLoader,
    DataLoaderResult,
};


const ATTRIBUTES_FILE: &str = "info";

enum GlobalProxy {
    Window(web_sys::Window),
    WorkerGlobalScope(web_sys::WorkerGlobalScope),
}

impl GlobalProxy {
    fn fetch_with_request(&self, request: &Request) -> Promise {
        match self {
            GlobalProxy::Window(window) => window.fetch_with_request(request),
            GlobalProxy::WorkerGlobalScope(scope) => scope.fetch_with_request(request),
        }
    }
}

macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

fn self_() -> Result<GlobalProxy, JsValue> {
    let global = js_sys::global();
    if js_sys::eval("typeof WorkerGlobalScope !== 'undefined'")?.as_bool().unwrap() {
        Ok(global.dyn_into::<web_sys::WorkerGlobalScope>().map(GlobalProxy::WorkerGlobalScope)?)
    }
    else {
        Ok(global.dyn_into::<web_sys::Window>().map(GlobalProxy::Window)?)
    }
}


#[wasm_bindgen]
#[derive(Clone)]
pub struct NgPreHTTPFetch {
    base_path: String,
}

impl NgPreHTTPFetch {
    fn fetch(&self, path_name: &str) -> JsFuture {
        let request_options = RequestInit::new();
        request_options.set_method("GET");
        request_options.set_mode(RequestMode::Cors);

        let req = Request::new_with_str_and_init(
            &format!("{}/{}", &self.base_path, path_name),
            &request_options).unwrap();

        let req_promise = self_().unwrap().fetch_with_request(&req);

        JsFuture::from(req_promise)
    }

    async fn fetch_json(&self, path_name: &str) -> JsValue {
        self.fetch(path_name).await.and_then(|resp_value| {
            assert!(resp_value.is_instance_of::<Response>());
            let resp: Response = resp_value.dyn_into()?;

            resp.json()
        }).unwrap().into()
    }

    async fn get_attributes(&self, path_name: &str) -> serde_json::Value {
        utils::set_panic_hook();
        let path = self.get_dataset_attributes_path(path_name);
        let to_return = self.fetch_json(&path)
            .then(|json| {
                serde_wasm_bindgen::from_value(json)
            });

        map_future_error_rust(to_return).await.unwrap()
    }

    fn relative_block_path(&self, path_name: &str, grid_position: &[i64], block_size: &[u32], voxel_offset: &[i64], dimensions: &[u64]) -> String {
        let mut block_path = path_name.to_owned();
        let mut n = 0;
        write!(block_path, "/").unwrap();
        for coord in grid_position {
            if n > 0 {
                write!(block_path, "_").unwrap();
            }
            // This assumes 0 <= coord <= grid_size[n].
            // write!(block_path, "x{}x{}x{}x", voxel_offset[n], coord, block_size[n]).unwrap();
            let begin_offset = voxel_offset[n] + coord * block_size[n] as i64;
            let end_offset = voxel_offset[n] + cmp::min(
                     (coord + 1) * block_size[n] as i64, dimensions[n] as i64);

            write!(block_path, "{}-{}", begin_offset, end_offset).unwrap();
            n = n + 1;
        }

        block_path
    }

    fn get_dataset_attributes_path(&self, path_name: &str) -> String {
        if path_name.is_empty() {
            ATTRIBUTES_FILE.to_owned()
        } else {
            // There is only one top-level attribute file
            format!("{}", ATTRIBUTES_FILE)
        }
    }
}

#[wasm_bindgen]
impl NgPreHTTPFetch {
    pub fn open(base_path: &str) -> Promise {
        let reader = NgPreHTTPFetch {
            base_path: base_path.into(),
        };

        let to_return = NgPreAsyncReader::get_version(&reader).and_then(|version| {

            if !ngpre::is_version_compatible(&ngpre::VERSION, &version) {
                return future::err(Error::new(ErrorKind::Other, "TODO: Incompatible version"))
            }

            future::ok(JsValue::from(reader))
        });

        future_to_promise(map_future_error_wasm(to_return))
    }
}

/// Delegations to expose NgPrePromiseReader trait to WASM.
#[wasm_bindgen]
impl NgPreHTTPFetch {
    pub fn get_version(&self) -> Promise {
        NgPrePromiseReader::get_version(self)
    }

    pub fn get_dataset_attributes(&self, path_name: &str) -> Promise {
        NgPrePromiseReader::get_dataset_attributes(self, path_name)
    }

    pub fn exists(&self, path_name: &str) -> Promise {
        NgPrePromiseReader::exists(self, path_name)
    }

    pub fn dataset_exists(&self, path_name: &str) -> Promise {
        NgPrePromiseReader::dataset_exists(self, path_name)
    }

    pub fn read_block(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> Promise {
        NgPrePromiseReader::read_block(self, path_name, data_attrs, grid_position)
    }

    pub fn list_attributes(&self, path_name: &str) -> Promise {
        NgPrePromiseReader::list_attributes(self, path_name)
    }

    pub fn block_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> Promise {
        NgPrePromiseEtagReader::block_etag(
            self, path_name, data_attrs, grid_position)
    }

    pub fn read_block_with_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> Promise {
        NgPrePromiseEtagReader::read_block_with_etag(
            self, path_name, data_attrs, grid_position)
    }
}

struct HTTPDataLoader {

}

fn path_join(paths: Vec<&str>) -> Option<String> {
    let mut path = PathBuf::new();
    path.extend(paths);
    Some(path.to_str().unwrap().to_owned())
}

impl DataLoader for HTTPDataLoader {
    fn get(&self, path: String, progress: Option<bool>, tuples: Vec<(String, u64, u64)>, num: usize)
        -> HashMap<String, Result<DataLoaderResult, Error>> {

        let result = HashMap::new();
        console::log_1(&format!("HTTPDataLoader {:?} num: {:?}", &path, num).into());

        // FIXME: Optional parallelization
        for request in tuples.iter() {
            let full_path = path_join(vec![&path, &request.0]).unwrap();
            console::log_1(&format!("Requesting {:?}", full_path).into());

            let mut request_options = RequestInit::new();
            request_options.set_method("GET");
            request_options.set_mode(RequestMode::Cors);

            let req = Request::new_with_str_and_init(
                &full_path,
                &request_options).unwrap();

            req.headers().set("Range",
                &format!("bytes={}-{}", request.1, request.2));

            console::log_1(&format!("Request: {:?}", req).into());

            let req_promise = self_().unwrap().fetch_with_request(&req);

            let f = JsFuture::from(req_promise);

            //let completed_req = futures::executor::block_on(f);
            //assert!(completed_req.is_instance_of::<Response>());
            //let resp: Response = completed_req.dyn_into().unwrap();



            //let json = JsFuture::from(resp.json()?).await?;
            /*
                .map(|resp_value| {
                    assert!(resp_value.is_instance_of::<Response>());
                    let resp: Response = resp_value.dyn_into().unwrap();

                    if resp.ok() {
                        resp.headers().get("ETag").unwrap_or(None)
                    } else {
                        None
                    }
                })
            */

            //console::log_1(&format!("Received: {:?}", resp).into());
            //Box::new(map_future_error_rust(f))
        }

        result
    }
}

impl NgPreAsyncReader for NgPreHTTPFetch {
    async fn get_version(&self) -> ngpre::Version {
        let to_return = self.get_attributes("").map(|_attr| {
                ngpre::Version::from_str(&"2.3.0").unwrap()
            }).await;

        to_return
    }

    async fn get_dataset_attributes(&self, path_name: &str) ->
            ngpre::DatasetAttributes {
        utils::set_panic_hook();

        console::log_1(&"get_dataset_attributes".into());
        let path = self.get_dataset_attributes_path(path_name);
        let to_return = self
            .fetch_json(&path)
            .map(|json| {
                let val: ngpre::DatasetAttributes = serde_wasm_bindgen::from_value(json).unwrap();
                log!("val: {:?}", val);
                val
            });

        to_return.await
    }

    async fn exists(&self, path_name: &str) -> bool {
        let to_return = self.fetch(path_name).map_ok(|resp_value| {
            assert!(resp_value.is_instance_of::<Response>());
            let resp: Response = resp_value.dyn_into().unwrap();

            resp.ok()
        });

        map_future_error_rust(to_return).await.unwrap()
    }

    // Override the default NgPreAsyncReader impl to not require the GET on the
    // dataset directory path to be 200.
    async fn dataset_exists(&self, path_name: &str) -> bool {
        let path = self.get_dataset_attributes_path(path_name);
        NgPreAsyncReader::exists(self, &path).await
    }

    async fn read_block<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Option<VecDataBlock<T>>
        where VecDataBlock<T>: DataBlock<T> + ngpre::ReadableDataBlock,
            T: ReflectedType,
    {

        NgPreAsyncEtagReader::read_block_with_etag(
                self, path_name, data_attrs, grid_position)
            .map(|maybe_block| maybe_block.map(|(block, _etag)| block)).await
    }

    async fn list(&self, _path_name: &str) -> Vec<String> {
        // TODO: Not implemented because remote paths are not listable.
        unimplemented!()
    }

    async fn list_attributes(
        &self,
        path_name: &str,
    ) -> serde_json::Value {

        self.get_attributes(path_name).await
    }
}

impl NgPreAsyncEtagReader for NgPreHTTPFetch {
    async fn block_etag(
        &self,
        path_name: &str,
        _data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Option<String> {
        let mut request_options = RequestInit::new();
        request_options.set_method("HEAD");
        request_options.set_mode(RequestMode::Cors);

        // TODO: Could be nicer
        let zoom_level = _data_attrs.get_scales().iter().position(|s| s.key == path_name).unwrap();

        let block_path = self.relative_block_path(path_name, &grid_position,
                 _data_attrs.get_block_size(zoom_level), _data_attrs.get_voxel_offset(zoom_level),
                 _data_attrs.get_dimensions(zoom_level));

        let req = Request::new_with_str_and_init(
            &format!("{}/{}", &self.base_path, block_path),
            &request_options).unwrap();

        console::log_1(&"block_etag".into());
        console::log_1(&block_path.into());
        let req_promise = self_().unwrap().fetch_with_request(&req);

        let f = JsFuture::from(req_promise)
            .map_ok(|resp_value| {
                assert!(resp_value.is_instance_of::<Response>());
                let resp: Response = resp_value.dyn_into().unwrap();

                if resp.ok() {
                    resp.headers().get("ETag").unwrap_or(None)
                } else {
                    None
                }
            });

        map_future_error_rust(f).await.unwrap()
    }

    async fn read_block_with_etag<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Option<(VecDataBlock<T>, Option<String>)>
            where VecDataBlock<T>: DataBlock<T> + ngpre::ReadableDataBlock,
                T: ReflectedType,
    {

        console::log_1(&"read etag block".into());
        let da2 = data_attrs.clone();

        // TODO: Could be nicer
        let zoom_level = data_attrs.get_scales().iter().position(|s| s.key == path_name).unwrap();
        let voxel_offset = data_attrs.get_voxel_offset(zoom_level);
        let chunk_size = data_attrs.get_block_size(zoom_level);
        let dimensions = data_attrs.get_dimensions(zoom_level);

        // Handling sharding is handled here, because
        if data_attrs.is_sharded(zoom_level) {
            console::log_1(&"--------------------".into());
            console::log_1(&format!("sharding format detected at zoom level, zoom: {:?} path: {:?}", &zoom_level, &self.base_path).into());

            let meta = ngpre::PrecomputedMetadata {
                cloudpath: self.base_path.to_string(),
            };
            let mut offset_grid_position = GridCoord::new();
            let mut n = 0;
            for coord in grid_position {
                if coord < 0 || coord * chunk_size[n] as i64 > dimensions[n] as i64  {
                    return None;
                }
                offset_grid_position.push(coord as u64);
                n = n + 1;
            }
            console::log_2(&"offset_grid_position".into(), &format!("{:?}", &offset_grid_position).into());

            //full_bbox = requested_bbox.expand_to_chunk_size(
            //  meta.chunk_size(mip), offset=meta.voxel_offset(mip)
            //)
            //full_bbox = Bbox.clamp(full_bbox, meta.bounds(mip))

            // bounds(mip) is dataset size in voxels
            // grid_size = np.ceil(meta.bounds(mip).size3() / chunk_size).astype(np.uint32)
            let bounds = data_attrs.bounds(zoom_level);
            let grid_size: Vec<u64> = bounds[0].iter().zip(bounds[1].iter()).enumerate()
                .map(|(i, (bmin, bmax))| ((bmax - bmin) as u64).div_ceil(chunk_size[i].into()))
                .collect();
            console::log_2(&"voxel_offset".into(), &format!("{:?}", &voxel_offset).into());
            console::log_2(&"chunk size".into(), &format!("{:?}", &chunk_size).into());
            console::log_2(&"dimensions".into(), &format!("{:?}", &dimensions).into());
            console::log_2(&"bounds".into(), &format!("{:?}", &bounds).into());
            console::log_2(&"grid_size".into(), &format!("{:?}", &grid_size).into());
            //bounds = meta.bounds(mip)
            //gpts = list(gridpoints(full_bbox, bounds, chunk_size))
            let gpts = vec![offset_grid_position];
            let morton_codes = ngpre::compressed_morton_code(&gpts, &grid_size).unwrap();

            console::log_2(&"gpts".into(), &format!("{:?}", &gpts).into());
            console::log_2(&"morton_codes".into(), &format!("{:?}", &morton_codes).into());

            // Get raw data and send back
            let progress = false;
            let decompress = false;
            let parallel = 0;

            // The cache service ultimately retrieves the data. We need to provide it with a
            // function to download the data from the respective path/URL.
            /*
            fn download_as (shard_path) {
                let f = self.fetch(&shard_path).and_then(|resp_value| {
                    assert!(resp_value.is_instance_of::<Response>());
                    let resp: Response = resp_value.dyn_into().unwrap();

                    if resp.ok() {
                        let etag: Option<String> = resp.headers().get("ETag").unwrap_or(None);
                        let to_return = JsFuture::from(resp.array_buffer().unwrap())
                            .map(move |arrbuff_value| {
                                assert!(arrbuff_value.is_instance_of::<ArrayBuffer>());
                                let typebuff: js_sys::Uint8Array = js_sys::Uint8Array::new(&arrbuff_value);
                                let buff = typebuff.to_vec();

                                Some((<ngpre::DefaultBlock as ngpre::DefaultBlockReader<T, &[u8]>>::read_block(
                                    &buff,
                                    &Da2,
                                    offset_grid_position).unwrap(),
                                    etag))
                            });
                        future::Either::A(to_return)
                    } else  {
                        future::Either::B(future::ok(None))
                    }
                };
            }
            */

            let loader = HTTPDataLoader {};
            let cache = ngpre::CacheService {
                enabled: false,
                data_loader: &loader,
                meta: &meta,
            };
            let spec = data_attrs.get_sharding_spec(zoom_level).unwrap();
            let mut reader = ngpre::ShardReader::new(&meta, &cache, &spec, &loader, None, None);
            console::log_1(&format!("{:?}", reader.to_string()).into());
            let io_chunkdata = reader.get_data(&morton_codes, Some(data_attrs.get_key(zoom_level)),
                    Some(progress), Some(parallel), Some(!decompress));
            // io_chunkdata = reader.get_data(
            //      morton_codes, meta.key(mip),
            //      progress=progress,
            //      raw=(not decompress),
            //  )


            // Rust-based decoding
            //code_map = {}
            //for gridpoint, morton_code in zip(gpts, morton_codes):
            //  cutout_bbox = Bbox(
            //  bounds.minpt + gridpoint * chunk_size,
            //  min2(bounds.minpt + (gridpoint + 1) * chunk_size, bounds.maxpt)
            //decode_fn = partial(decode_single_voxel, requested_bbox.minpt - full_bbox.minpt)

            let f = future::ok(None);
            map_future_error_rust(f).await.unwrap()
        } else {
            let block_path = self.relative_block_path(path_name, &grid_position,
                    chunk_size, voxel_offset, dimensions);

            // Make sure we are in bounds with requested blocks. This method accepts signed inputed, to
            // allow catching overflows when data from JavaScript is passed in.
            let mut offset_grid_position = GridCoord::new();
            let mut n = 0;
            for coord in grid_position {
                if coord < 0 || coord * chunk_size[n] as i64 > dimensions[n] as i64  {
                    return None;
                }
                offset_grid_position.push(coord as u64);
                n = n + 1;
            }

            console::log_1(&"read_block_with etag".into());
            console::log_1(&block_path.clone().into());

            let f = self.fetch(&block_path).and_then(|resp_value| {
                assert!(resp_value.is_instance_of::<Response>());
                let resp: Response = resp_value.dyn_into().unwrap();

                if resp.ok() {
                    let etag: Option<String> = resp.headers().get("ETag").unwrap_or(None);
                    let to_return = JsFuture::from(resp.array_buffer().unwrap())
                        .map_ok(move |arrbuff_value| {
                            assert!(arrbuff_value.is_instance_of::<ArrayBuffer>());
                            let typebuff: js_sys::Uint8Array = js_sys::Uint8Array::new(&arrbuff_value);
                            let buff = typebuff.to_vec();

                            Some((<ngpre::DefaultBlock as ngpre::DefaultBlockReader<T, &[u8]>>::read_block(
                                &buff,
                                &da2,
                                offset_grid_position).unwrap(),
                                etag))
                        });
                    Either::Left(to_return)
                } else {
                    Either::Right(future::ok(None))
                }
            });

            map_future_error_rust(f).await.unwrap()
        }
    }
}
